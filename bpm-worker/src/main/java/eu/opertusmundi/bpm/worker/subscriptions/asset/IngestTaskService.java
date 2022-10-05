package eu.opertusmundi.bpm.worker.subscriptions.asset;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.model.EnumPublishRequestType;
import eu.opertusmundi.bpm.worker.model.ErrorCodes;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.model.ServiceException;
import eu.opertusmundi.common.model.asset.AssetDraftDto;
import eu.opertusmundi.common.model.asset.EnumResourceType;
import eu.opertusmundi.common.model.asset.FileResourceDto;
import eu.opertusmundi.common.model.asset.ResourceDto;
import eu.opertusmundi.common.model.asset.service.UserServiceDto;
import eu.opertusmundi.common.model.file.FileSystemException;
import eu.opertusmundi.common.model.ingest.IngestServiceMessageCode;
import eu.opertusmundi.common.model.ingest.ServerIngestDeferredResponseDto;
import eu.opertusmundi.common.model.ingest.ServerIngestPublishResponseDto;
import eu.opertusmundi.common.model.ingest.ServerIngestResultResponseDto;
import eu.opertusmundi.common.model.ingest.ServerIngestStatusResponseDto;
import eu.opertusmundi.common.model.ingest.ServerIngestTicketResponseDto;
import eu.opertusmundi.common.model.profiler.DataProfilerServiceMessageCode;
import eu.opertusmundi.common.service.DraftFileManager;
import eu.opertusmundi.common.service.IngestService;
import eu.opertusmundi.common.service.ProviderAssetService;
import eu.opertusmundi.common.service.UserServiceFileManager;
import eu.opertusmundi.common.service.UserServiceService;
import eu.opertusmundi.common.service.ogc.UserGeodataConfigurationResolver;

@Service
public class IngestTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(IngestTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.ingest.lock-duration:120000}")
    private Long lockDurationMillis;

    @Override
    protected long getLockDuration() {
        return this.lockDurationMillis;
    }

    @Autowired
    private UserGeodataConfigurationResolver userGeodataConfigurationResolver;

    @Autowired
    private DraftFileManager draftFileManager;

    @Autowired
    private UserServiceFileManager userServiceFileManager;

    @Autowired
    private ProviderAssetService providerAssetService;

    @Autowired
    private UserServiceService userServiceService;

    @Autowired
    private IngestService ingestService;

    @Override
    public String getTopicName() {
        return "ingest";
    }

    @Override
    public final void execute(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        final String taskId = externalTask.getId();

        logger.info("Received task. [taskId={}]", taskId);

        final String                 requestType = this.getVariableAsString(externalTask, externalTaskService, "requestType");
        final EnumPublishRequestType type        = EnumPublishRequestType.valueOf(requestType);

        logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

        try {
            switch (type) {
                case CATALOGUE_ASSET :
                    this.ingestCatalogueAsset(externalTask, externalTaskService);
                    break;
                case USER_SERVICE :
                    this.ingestUserService(externalTask, externalTaskService);
                    break;
            }

            logger.info("Completed task. [taskId={}]", taskId);
        } catch (final ServiceException ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);
            if (ExceptionUtils.indexOfType(ex, feign.RetryableException.class) != -1) {
                // For feign client retryable exceptions, create a new incident
                // instead of canceling the process instance. Errors such as
                // network connectivity, unavailable services etc may be
                // automatically resolved after retrying the failed task
                //
                // See:
                // https://javadoc.io/doc/io.github.openfeign/feign-core/latest/feign/RetryableException.html
                //
                // "This exception is raised when the Response is deemed to be
                // retryable, typically via an ErrorDecoder when the status is
                // 503."
                this.handleFailure(externalTaskService, externalTask, ex);
            } else {
                this.handleBpmnError(externalTaskService, externalTask, this.getErrorCode(type), ex);
            }
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleFailure(externalTaskService, externalTask, ex);
        }
    }

    private final void ingestCatalogueAsset(
        ExternalTask externalTask, ExternalTaskService externalTaskService
    ) throws InterruptedException {
        final UUID    draftKey     = this.getVariableAsUUID(externalTask, externalTaskService, "assetKey");
        final UUID    publisherKey = this.getVariableAsUUID(externalTask, externalTaskService, "publisherKey");
        final boolean published    = this.getVariableAsBooleanString(externalTask, externalTaskService, "published");

        final AssetDraftDto draft             = providerAssetService.findOneDraft(publisherKey, draftKey, false);
        var                 userGeodataConfig = userGeodataConfigurationResolver.resolveFromUserKey(publisherKey);

        final List<ResourceDto> resources = draft.getCommand().getResources();

        // Process all resources
        for (ResourceDto resource : resources) {
            if (resource.getType() != EnumResourceType.FILE) {
                continue;
            }
            final FileResourceDto fileResource = (FileResourceDto) resource;
            final String          tableName    = fileResource.getId();
            final String          fileName     = fileResource.getFileName();
            final String          encoding     = fileResource.getEncoding();
            final String          crs          = fileResource.getCrs();
            final String          shard        = userGeodataConfig.getShard();
            final String          workspace    = userGeodataConfig.getWorkspace();
            final String          path         = this.getResource(externalTask, externalTaskService, publisherKey, draftKey, fileName);

            final ServerIngestResultResponseDto ingestResult = this.ingest(
                externalTask, externalTaskService, path, shard, workspace, tableName, encoding, crs
            );

            // Update metadata for the specific file
            providerAssetService.updateResourceIngestionData(publisherKey, draftKey, resource.getId(), ingestResult);

            if(published) {
                final ServerIngestPublishResponseDto publishResult = this.publish(
                    externalTask, externalTaskService, shard, workspace, ingestResult.getTable()
                );

                providerAssetService.updateResourceIngestionData(publisherKey, draftKey, resource.getId(), publishResult);

                // TODO: Update services
                logger.warn(publishResult.toString());
            }
        }

        // Complete task
        externalTaskService.complete(externalTask);
    }

    private final void ingestUserService(
        ExternalTask externalTask, ExternalTaskService externalTaskService
    ) throws InterruptedException {
        final UUID ownerKey   = this.getVariableAsUUID(externalTask, externalTaskService, "ownerKey");
        final UUID parentKey  = this.getVariableAsUUID(externalTask, externalTaskService, "parentKey");
        final UUID serviceKey = this.getVariableAsUUID(externalTask, externalTaskService, "serviceKey");

        final UserServiceDto service  = userServiceService.findOne(ownerKey, parentKey, serviceKey);
        final String         encoding = service.getEncoding();
        final String         crs      = service.getCrs();

        var userGeodataConfig = userGeodataConfigurationResolver.resolveFromUserKey(ownerKey);

        final String shard     = userGeodataConfig.getShard();
        final String workspace = userGeodataConfig.getWorkspace();
        final String tableName = service.getKey().toString();
        final String fileName  = service.getFileName();
        final String path      = this.getUserServiceResource(externalTask, externalTaskService, ownerKey, serviceKey, fileName);

        // Ingest
        final ServerIngestResultResponseDto ingestResult = this.ingest(
            externalTask, externalTaskService, path, shard, workspace, tableName, encoding, crs
        );
        userServiceService.updateResourceIngestionData(ownerKey, serviceKey, ingestResult);

        // Publish to GeoServer
        final ServerIngestPublishResponseDto publishResult = this.publish(
            externalTask, externalTaskService, shard, workspace, ingestResult.getTable()
        );
        userServiceService.updateResourceIngestionData(ownerKey, serviceKey, publishResult);

        // Publish
        userServiceService.publish(ownerKey, parentKey, serviceKey);

        // Complete task
        externalTaskService.complete(externalTask);
    }

    private ServerIngestResultResponseDto ingest(
        ExternalTask externalTask, ExternalTaskService externalTaskService,
        String path, String shard, String workspace, String table, String encoding, String crs
    ) throws InterruptedException {
        final String                  idempotentKey = table;
        String                        ticket;
        ServerIngestStatusResponseDto result        = null;
        int                           counter       = 0;

        final ServerIngestTicketResponseDto ticketResponse = this.ingestService.getTicket(idempotentKey);

        if (ticketResponse == null) {
            final ServerIngestDeferredResponseDto ingestResponse = this.ingestService.ingestAsync(
                idempotentKey, path.toString(), shard, workspace, table, encoding, crs
            );
            ticket = ingestResponse.getTicket();
        } else {
            ticket = ticketResponse.getTicket();

            result  = this.ingestService.getStatus(ticket);
        }

        if (result == null || !result.isCompleted()) {
            // TODO: Add a parameter for preventing infinite loops
            while (counter++ < 100) {
                // NOTE: Polling interval must be less than lock duration
                Thread.sleep(15000);

                // Extend lock duration
                externalTaskService.extendLock(externalTask, this.getLockDuration());

                try {
                    result = this.ingestService.getStatus(ticket);

                    if (result.isCompleted()) {
                        break;
                    }
                } catch (Exception ex) {
                    // Ignore exception since the remote server may have not
                    // initialized the job
                    logger.info(String.format("Ingest service get status operation has failed [ticket=%s]", ticket), ex);
                }
            }
        }

        if (result != null && result.isCompleted() && result.isSuccess()) {
            // Get table name
            final ServerIngestResultResponseDto resultResponse = this.ingestService.getResult(ticket);

            return resultResponse;
        } else {
            if (counter == 100) {
                logger.warn(String.format("Ingest service ingest operation has timed out [ticket=%s]", ticket));
            }

            throw BpmnWorkerException.builder()
                .code(IngestServiceMessageCode.SERVICE_ERROR)
                .message("[INGEST Service] Operation has failed")
                .errorDetails(String.format("Ticket: [%s]. Comment: [%s]", ticket, result.getComment()))
                .build();
        }
    }

    private ServerIngestPublishResponseDto publish(
        ExternalTask externalTask, ExternalTaskService externalTaskService, String shard, String workspace, String tableName
    ) throws InterruptedException {
        final String idempotentKey = UUID.randomUUID().toString();

        return this.ingestService.publish(idempotentKey, shard, workspace, tableName);
    }

    private String getResource(
        ExternalTask externalTask, ExternalTaskService externalTaskService, UUID publisherKey, UUID draftKey, String resourceFileName
    ) throws BpmnWorkerException {
        try {
            final Path path = this.draftFileManager.resolveResourcePath(publisherKey, draftKey, resourceFileName);

            return path.toString();
        } catch(final FileSystemException ex) {
            throw BpmnWorkerException.builder()
                .code(DataProfilerServiceMessageCode.SOURCE_NOT_FOUND)
                .message(String.format("Failed to resolve resource file [%s]", resourceFileName))
                .errorDetails(ex.getRootCause().getMessage())
                .build();
        }
    }

    private String getUserServiceResource(
        ExternalTask externalTask, ExternalTaskService externalTaskService, UUID ownerKey, UUID serviceKey, String fileName
    ) throws BpmnWorkerException {
        try {
            final Path path = this.userServiceFileManager.resolveResourcePath(ownerKey, serviceKey, fileName);

            return path.toString();
        } catch(final FileSystemException ex) {
            throw BpmnWorkerException.builder()
                .code(DataProfilerServiceMessageCode.SOURCE_NOT_FOUND)
                .message(String.format("Failed to resolve resource file [%s]", fileName))
                .errorDetails(ex.getRootCause().getMessage())
                .build();
        }
    }

    private String getErrorCode(EnumPublishRequestType type) {
        switch (type) {
            case CATALOGUE_ASSET :
                return ErrorCodes.PublishAsset;
            case USER_SERVICE :
                return ErrorCodes.PublishUserService;
        }

        return ErrorCodes.None;
    }
}