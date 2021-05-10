package eu.opertusmundi.bpm.worker.subscriptions.asset;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.model.asset.AssetDraftDto;
import eu.opertusmundi.common.model.asset.EnumResourceType;
import eu.opertusmundi.common.model.asset.FileResourceDto;
import eu.opertusmundi.common.model.asset.ResourceDto;
import eu.opertusmundi.common.model.ingest.IngestServiceMessageCode;
import eu.opertusmundi.common.model.ingest.ServerIngestDeferredResponseDto;
import eu.opertusmundi.common.model.ingest.ServerIngestPublishResponseDto;
import eu.opertusmundi.common.model.ingest.ServerIngestResultResponseDto;
import eu.opertusmundi.common.model.ingest.ServerIngestStatusResponseDto;
import eu.opertusmundi.common.model.ingest.ServerIngestTicketResponseDto;
import eu.opertusmundi.common.service.DraftFileManager;
import eu.opertusmundi.common.service.IngestService;
import eu.opertusmundi.common.service.ProviderAssetService;

@Service
public class IngestTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(IngestTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.ingest.lock-duration:60000}")
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

            logger.info("Received task {}", taskId);

            this.preExecution(externalTask, externalTaskService);
           
            final UUID    draftKey     = this.getAssetKey(externalTask, externalTaskService);
            final UUID    publisherKey = this.getPublisherKey(externalTask, externalTaskService);
            final boolean published    = this.isPublished(externalTask, externalTaskService);

            final AssetDraftDto draft = providerAssetService.findOneDraft(publisherKey, draftKey);

            final List<ResourceDto> resources = draft.getCommand().getResources();

            logger.debug("Processing task {}: {}", taskId, externalTask);
            
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
            logger.debug("Completed task {}: {}", taskId, externalTask);

            this.postExecution(externalTask, externalTaskService);

            externalTaskService.complete(externalTask);

            logger.info("Completed task {}", taskId);
        } catch (final BpmnWorkerException ex) {
            logger.error(String.format("[Ingest Service] Operation has failed. Error details: %s", ex.getErrorDetails()), ex);

            externalTaskService.handleFailure(
                externalTask, ex.getMessage(), ex.getErrorDetails(), ex.getRetries(), ex.getRetryTimeout()
            );
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleError(externalTaskService, externalTask, ex);
        }
    }

    private ServerIngestResultResponseDto ingest(
        ExternalTask externalTask, ExternalTaskService externalTaskService, 
        UUID publisherKey, UUID draftKey, FileResourceDto resource
    ) throws InterruptedException {
        // Resolve path
        final Path path = this.draftFileManager.resolveResourcePath(publisherKey, draftKey, resource.getFileName());

        final UUID                    idempotentKey = resource.getId();
        final String                  tableName     = resource.getId().toString();
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
                // NOTE: Polling time must be less than lock duration
                Thread.sleep(30000);

                // Extend lock duration
                externalTaskService.extendLock(externalTask, this.getLockDuration());

                result = this.ingestService.getStatus(ticket);
                if (result.isCompleted()) {
                    break;
                }
            }
        }

        if (result != null && result.isCompleted() && result.isSuccess()) {
            // Get table name
            final ServerIngestResultResponseDto resultResponse = this.ingestService.getResult(ticket);
           
            return resultResponse;            
        } else {
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
        final UUID idempotentKey = UUID.randomUUID();

        return this.ingestService.publish(idempotentKey, tableName);
    }
  
    private UUID getAssetKey(ExternalTask externalTask, ExternalTaskService externalTaskService) throws BpmnWorkerException {
        final String name     = "assetKey";
        final String draftKey = (String) externalTask.getVariable(name);
        
        if (StringUtils.isBlank(draftKey)) {
            logger.error(String.format("Expected draft key [%s] to be non empty!", name));

            throw this.buildVariableNotFoundException(name);
        }

        return UUID.fromString(draftKey);
    }

    private UUID getPublisherKey(ExternalTask externalTask, ExternalTaskService externalTaskService) throws BpmnWorkerException {
        final String name         = "publisherKey";
        final String publisherKey = (String) externalTask.getVariable(name);

        if (StringUtils.isBlank(publisherKey)) {
            logger.error(String.format("Expected publisher key [%s] to be non empty!", name));

            throw this.buildVariableNotFoundException(name);
        }

        return UUID.fromString(publisherKey);
    }

    private boolean isPublished(ExternalTask externalTask, ExternalTaskService externalTaskService) throws BpmnWorkerException {
        final String name      = "published";
        final String published = (String) externalTask.getVariable(name);

        if (StringUtils.isBlank(published)) {
            logger.error(String.format("Expected variable [%s] to be non empty!", name));

            throw this.buildVariableNotFoundException(name);
        }

        return Boolean.parseBoolean(published);
    }
    
    protected void preExecution(ExternalTask externalTask, ExternalTaskService externalTaskService) {

    }

    protected void postExecution(ExternalTask externalTask, ExternalTaskService externalTaskService) {

    }

}