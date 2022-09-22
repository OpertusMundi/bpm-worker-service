package eu.opertusmundi.bpm.worker.subscriptions.asset;

import java.math.BigDecimal;
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

import com.fasterxml.jackson.databind.JsonNode;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.model.EnumPublishRequestType;
import eu.opertusmundi.bpm.worker.model.ErrorCodes;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.model.ServiceException;
import eu.opertusmundi.common.model.asset.AssetDraftDto;
import eu.opertusmundi.common.model.asset.AssetDraftSetStatusCommandDto;
import eu.opertusmundi.common.model.asset.EnumProviderAssetDraftStatus;
import eu.opertusmundi.common.model.asset.EnumResourceType;
import eu.opertusmundi.common.model.asset.FileResourceDto;
import eu.opertusmundi.common.model.asset.ResourceDto;
import eu.opertusmundi.common.model.asset.service.UserServiceDto;
import eu.opertusmundi.common.model.catalogue.client.EnumAssetType;
import eu.opertusmundi.common.model.file.FileSystemException;
import eu.opertusmundi.common.model.profiler.DataProfilerDeferredResponseDto;
import eu.opertusmundi.common.model.profiler.DataProfilerOptions;
import eu.opertusmundi.common.model.profiler.DataProfilerServiceException;
import eu.opertusmundi.common.model.profiler.DataProfilerServiceMessageCode;
import eu.opertusmundi.common.model.profiler.DataProfilerStatusResponseDto;
import eu.opertusmundi.common.service.DataProfilerService;
import eu.opertusmundi.common.service.DraftFileManager;
import eu.opertusmundi.common.service.ProviderAssetService;
import eu.opertusmundi.common.service.UserServiceFileManager;
import eu.opertusmundi.common.service.UserServiceService;
import eu.opertusmundi.common.util.BpmInstanceVariablesBuilder;

@Service
public class ProfileTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(ProfileTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.data-profiler.lock-duration:120000}")
    private Long lockDurationMillis;

    @Value("${opertusmundi.data-profiler.parameters.aspect-ratio:}")
    private BigDecimal aspectRatio;

    @Value("${opertusmundi.data-profiler.parameters.height:}")
    private Integer height;

    @Value("${opertusmundi.data-profiler.parameters.width:1920}")
    private Integer width;

    @Autowired
    private DraftFileManager draftFileManager;

    @Autowired
    private UserServiceFileManager userServiceFileManager;

    @Autowired
    private DataProfilerService profilerService;

    @Autowired
    private ProviderAssetService providerAssetService;

    @Autowired
    private UserServiceService userServiceService;

    @Override
    public String getTopicName() {
        return "computeAutomatedMetadata";
    }

    @Override
    protected long getLockDuration() {
        return this.lockDurationMillis;
    }

    @Override
    public void execute(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        final String taskId = externalTask.getId();

        logger.info("Received task. [taskId={}]", taskId);

        final String                 requestType = this.getVariableAsString(externalTask, externalTaskService, "requestType");
        final EnumPublishRequestType type        = EnumPublishRequestType.valueOf(requestType);

        logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);
        try {
            switch (type) {
                case CATALOGUE_ASSET :
                    this.profileCatalogueAsset(externalTask, externalTaskService);
                    break;
                case USER_SERVICE :
                    this.profileUserService(externalTask, externalTaskService);
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

    private void profileCatalogueAsset(ExternalTask externalTask, ExternalTaskService externalTaskService) throws InterruptedException {
        final UUID    draftKey             = this.getVariableAsUUID(externalTask, externalTaskService, "draftKey");
        final UUID    publisherKey         = this.getVariableAsUUID(externalTask, externalTaskService, "publisherKey");
        final boolean dataProfilingEnabled = this.getVariableAsBoolean(externalTask, externalTaskService, "dataProfilingEnabled", true);

        final AssetDraftDto draft = providerAssetService.findOneDraft(publisherKey, draftKey, false);

        if (dataProfilingEnabled) {
            final List<ResourceDto> resources = draft.getCommand().getResources();

            // Process all resources
            for (ResourceDto resource : resources) {
                if (resource.getType() != EnumResourceType.FILE) {
                    continue;
                }
                final FileResourceDto fileResource  = (FileResourceDto) resource;
                final String          idempotentKey = fileResource.getId();
                final EnumAssetType   assetType     = fileResource.getCategory();
                final String          fileName      = fileResource.getFileName();
                final String          crs           = fileResource.getCrs();
                final String          encoding      = fileResource.getEncoding();
                final String          path          = this.getResource(externalTask, externalTaskService, publisherKey, draftKey, fileName);

                final JsonNode metadata = this.profile(externalTask, externalTaskService, idempotentKey, path, assetType, crs, encoding);

                // Update metadata for the specific file
                providerAssetService.updateMetadata(publisherKey, draftKey, resource.getId(), metadata);
            }
        }

        // Update draft status if this is not a SERVICE asset
        final BpmInstanceVariablesBuilder variables = BpmInstanceVariablesBuilder.builder();

        if (draft.getCommand().getType() != EnumAssetType.SERVICE) {
            final AssetDraftSetStatusCommandDto command   = new AssetDraftSetStatusCommandDto();
            final EnumProviderAssetDraftStatus  newStatus = EnumProviderAssetDraftStatus.PENDING_HELPDESK_REVIEW;

            command.setAssetKey(draftKey);
            command.setPublisherKey(publisherKey);
            command.setStatus(newStatus);

            variables.variableAsString("status", newStatus.toString());

            this.providerAssetService.updateStatus(command);
        }

        // Complete task
        externalTaskService.complete(externalTask, variables.buildValues());
    }

    private void profileUserService(ExternalTask externalTask, ExternalTaskService externalTaskService) throws InterruptedException {
        final UUID ownerKey   = this.getVariableAsUUID(externalTask, externalTaskService, "ownerKey");
        final UUID serviceKey = this.getVariableAsUUID(externalTask, externalTaskService, "serviceKey");

        final UserServiceDto service = userServiceService.findOne(serviceKey);

        final String        idempotentKey = service.getKey().toString();
        final EnumAssetType assetType     = EnumAssetType.VECTOR;
        final String        crs           = service.getCrs();
        final String        encoding      = service.getEncoding();
        final String        fileName      = service.getFileName();
        final String        path          = this.getUserServiceResource(externalTask, externalTaskService, ownerKey, serviceKey, fileName);

        final JsonNode metadata = this.profile(externalTask, externalTaskService, idempotentKey, path, assetType, crs, encoding);

        // Update metadata for the specific file
        userServiceService.updateMetadata(ownerKey, serviceKey, metadata);

        // Complete task
        externalTaskService.complete(externalTask);
    }

    private JsonNode profile(
        ExternalTask externalTask, ExternalTaskService externalTaskService,
        String idempotentKey, String path,
        EnumAssetType assetType, String crs, String encoding
    ) throws InterruptedException {

        final DataProfilerOptions options = DataProfilerOptions.builder()
                .aspectRatio(this.aspectRatio)
                .crs(crs)
                .encoding(encoding)
                .height(this.height)
                .width(this.width)
                .build();

        if (assetType == EnumAssetType.VECTOR) {
            options.setGeometry("WKT");
        }

        final DataProfilerDeferredResponseDto profilerResponse = this.profilerService.profile(idempotentKey, assetType, path.toString(), options);
        final String                          ticket           = profilerResponse.getTicket();
        DataProfilerStatusResponseDto         result           = null;
        int                                   counter          = 0;

        // TODO: Add a parameter for preventing infinite loops
        while (counter++ < 100) {
            // NOTE: Polling interval must be less than lock duration
            Thread.sleep(15000);

            // Extend lock duration
            externalTaskService.extendLock(externalTask, this.getLockDuration());

            try {
                result = this.profilerService.getStatus(ticket);

                if (result.isCompleted()) {
                    break;
                }
            } catch (Exception ex) {
                // Ignore exception since the remote server may have not
                // initialized the job
                logger.info(String.format("Data profiler get status operation has failed [ticket=%s]", ticket), ex);
            }
        }

        if (result != null && result.isCompleted() && result.isSuccess()) {
            final JsonNode metadata = this.profilerService.getMetadata(ticket);

            return metadata;
        } else {
            if (counter == 100) {
                logger.warn(String.format("Data profiler get metadata operation has timed out [ticket=%s]", ticket));
            }

            throw new DataProfilerServiceException(DataProfilerServiceMessageCode.SERVICE_ERROR, String.format(
                "Data profiler operation has failed [ticket=%s, comment=%s]",
                ticket, result == null ? "" : result.getComment()
            ));
        }
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