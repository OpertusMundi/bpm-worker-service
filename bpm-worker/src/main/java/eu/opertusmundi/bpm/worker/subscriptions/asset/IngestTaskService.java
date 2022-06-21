package eu.opertusmundi.bpm.worker.subscriptions.asset;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.model.ErrorCodes;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.model.ServiceException;
import eu.opertusmundi.common.model.asset.AssetDraftDto;
import eu.opertusmundi.common.model.asset.EnumResourceType;
import eu.opertusmundi.common.model.asset.FileResourceDto;
import eu.opertusmundi.common.model.asset.ResourceDto;
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
    private DraftFileManager draftFileManager;

    @Autowired
    private ProviderAssetService providerAssetService;

    @Autowired
    private IngestService ingestService;

    @Override
    public String getTopicName() {
        return "ingest";
    }

    @Override
    public final void execute(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        try {
            final String taskId = externalTask.getId();

            logger.info("Received task. [taskId={}]", taskId);

            this.preExecution(externalTask, externalTaskService);

            final UUID    draftKey     = this.getAssetKey(externalTask, externalTaskService);
            final UUID    publisherKey = this.getPublisherKey(externalTask, externalTaskService);
            final boolean published    = this.isPublished(externalTask, externalTaskService);

            final AssetDraftDto draft = providerAssetService.findOneDraft(publisherKey, draftKey, false);

            final List<ResourceDto> resources = draft.getCommand().getResources();

            logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

            // Process all resources
            for (ResourceDto resource : resources) {
                if (resource.getType() != EnumResourceType.FILE) {
                    continue;
                }
                final FileResourceDto               fileResource = (FileResourceDto) resource;
                final ServerIngestResultResponseDto ingestResult = this.ingest(
                    externalTask, externalTaskService, publisherKey, draftKey, fileResource
                );

                // Update metadata for the specific file
                providerAssetService.updateResourceIngestionData(publisherKey, draftKey, resource.getId(), ingestResult);

                if(published) {
                    final ServerIngestPublishResponseDto publishResult = this.publish(
                        externalTask, externalTaskService, publisherKey, draftKey, fileResource, ingestResult.getTable()
                    );

                    providerAssetService.updateResourceIngestionData(publisherKey, draftKey, resource.getId(), publishResult);

                    // TODO: Update services
                    logger.warn(publishResult.toString());
                }
            }

            // Complete task
            this.postExecution(externalTask, externalTaskService);

            externalTaskService.complete(externalTask);

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
                this.handleBpmnError(externalTaskService, externalTask, ErrorCodes.PublishAsset, ex);
            }
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleFailure(externalTaskService, externalTask, ex);
        }
    }

    private ServerIngestResultResponseDto ingest(
        ExternalTask externalTask, ExternalTaskService externalTaskService,
        UUID publisherKey, UUID draftKey, FileResourceDto resource
    ) throws InterruptedException {
        // Resolve path
        final String path = this.getResource(externalTask, externalTaskService, publisherKey, draftKey, resource.getFileName());

        final String                  tableName     = resource.getId();
        final String                  idempotentKey = tableName;
        String                        ticket;
        ServerIngestStatusResponseDto result        = null;
        int                           counter       = 0;

        final ServerIngestTicketResponseDto ticketResponse = this.ingestService.getTicket(idempotentKey);

        if (ticketResponse == null) {
            final ServerIngestDeferredResponseDto ingestResponse = this.ingestService.ingestAsync(idempotentKey, path.toString(), tableName);
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
        ExternalTask externalTask, ExternalTaskService externalTaskService,
        UUID publisherKey, UUID draftKey, FileResourceDto resource, String tableName
    ) throws InterruptedException {
        final String idempotentKey = UUID.randomUUID().toString();

        return this.ingestService.publish(idempotentKey, tableName);
    }

    private UUID getAssetKey(ExternalTask externalTask, ExternalTaskService externalTaskService) throws BpmnWorkerException {
        final String name     = "assetKey";
        final String draftKey = (String) externalTask.getVariable(name);

        if (StringUtils.isBlank(draftKey)) {
            logger.error("Expected draft key to be non empty. [name={}]", name);

            throw this.buildVariableNotFoundException(name);
        }

        return UUID.fromString(draftKey);
    }

    private UUID getPublisherKey(ExternalTask externalTask, ExternalTaskService externalTaskService) throws BpmnWorkerException {
        final String name         = "publisherKey";
        final String publisherKey = (String) externalTask.getVariable(name);

        if (StringUtils.isBlank(publisherKey)) {
            logger.error("Expected publisher key to be non empty. [name={}]", name);

            throw this.buildVariableNotFoundException(name);
        }

        return UUID.fromString(publisherKey);
    }

    private boolean isPublished(ExternalTask externalTask, ExternalTaskService externalTaskService) throws BpmnWorkerException {
        final String name      = "published";
        final String published = (String) externalTask.getVariable(name);

        if (StringUtils.isBlank(published)) {
            logger.error("Expected variable to be non empty. [name={}]", name);

            throw this.buildVariableNotFoundException(name);
        }

        return Boolean.parseBoolean(published);
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

    protected void preExecution(ExternalTask externalTask, ExternalTaskService externalTaskService) {

    }

    protected void postExecution(ExternalTask externalTask, ExternalTaskService externalTaskService) {

    }

}