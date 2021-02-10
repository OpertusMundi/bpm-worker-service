package eu.opertusmundi.bpm.worker.support;

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

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.model.asset.AssetDraftDto;
import eu.opertusmundi.common.model.asset.AssetResourceDto;
import eu.opertusmundi.common.model.ingest.IngestServiceMessageCode;
import eu.opertusmundi.common.model.ingest.ServerIngestDeferredResponseDto;
import eu.opertusmundi.common.model.ingest.ServerIngestEndpointsResponseDto;
import eu.opertusmundi.common.model.ingest.ServerIngestStatusResponseDto;
import eu.opertusmundi.common.service.AssetFileManager;
import eu.opertusmundi.common.service.IngestService;
import eu.opertusmundi.common.service.ProviderAssetService;

public abstract class BaseIngestTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(BaseIngestTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.ingest.lock-duration:60000}")
    private Long lockDurationMillis;

    @Override
    protected long getLockDuration() {
        return this.lockDurationMillis;
    }

    @Autowired
    private AssetFileManager assetFileManager;

    @Autowired
    private ProviderAssetService providerAssetService;
    
    @Autowired
    private IngestService ingestService;

    @Override
    public final void execute(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        try {
            final String taskId = externalTask.getId();

            logger.info("Received task {}", taskId);

            this.preExecution(externalTask, externalTaskService);
           
            final UUID draftKey     = this.getDraftKey(externalTask, externalTaskService);
            final UUID publisherKey = this.getPublisherKey(externalTask, externalTaskService);

            final AssetDraftDto draft = providerAssetService.findOneDraft(publisherKey, draftKey);

            final List<AssetResourceDto> resources = draft.getCommand().getResources();

            logger.debug("Processing task {}: {}", taskId, externalTask);
            
            // Process all resources
            for (AssetResourceDto resource : resources) {
                final ServerIngestEndpointsResponseDto endpoints = this.ingest(externalTask, externalTaskService, publisherKey, draftKey, resource);
                
                // TODO: Update endpoints
                logger.warn(endpoints.toString());
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

    private ServerIngestEndpointsResponseDto ingest(
        ExternalTask externalTask, ExternalTaskService externalTaskService, 
        UUID publisherKey, UUID draftKey, AssetResourceDto resource
    ) throws InterruptedException {
        
        // Resolve path
        final Path                            source         = this.assetFileManager.resolveResourcePath(publisherKey, draftKey, resource.getFileName());
        final ServerIngestDeferredResponseDto ingestResponse = this.ingestService.ingestAsync(source.toString());
        final String                          ticket         = ingestResponse.getTicket();
        
        ServerIngestStatusResponseDto         result         = null;
        int                                   counter        = 0;

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

        if (result != null && result.isCompleted() && result.isSuccess()) {
            // GET endpoints
            final ServerIngestEndpointsResponseDto endpointsResponse = this.ingestService.getEndpoints(ticket);

            return endpointsResponse;            
        } else {
            throw BpmnWorkerException.builder()
                .code(IngestServiceMessageCode.SERVICE_ERROR)
                .message("[INGEST Service] Operation has failed")
                .errorDetails(String.format("Ticket: [%s]. Comment: [%s]", ticket, result.getComment()))
                .build();
        }
    }
    
    private UUID getDraftKey(ExternalTask externalTask, ExternalTaskService externalTaskService) throws BpmnWorkerException {
        final String name     = this.getAssetKeyVariableName(externalTask, externalTaskService);
        final String draftKey = (String) externalTask.getVariable(name);
        
        if (StringUtils.isBlank(draftKey)) {
            logger.error(String.format("Expected draft key [%s] to be non empty!", name));

            throw this.buildVariableNotFoundException(name);
        }

        return UUID.fromString(draftKey);
    }

    private UUID getPublisherKey(ExternalTask externalTask, ExternalTaskService externalTaskService) throws BpmnWorkerException {
        final String name         = this.getPublisherKeyVariableName(externalTask, externalTaskService);
        final String publisherKey = (String) externalTask.getVariable(name);

        if (StringUtils.isBlank(publisherKey)) {
            logger.error(String.format("Expected publisher key [%s] to be non empty!", name));

            throw this.buildVariableNotFoundException(name);
        }

        return UUID.fromString(publisherKey);
    }

    abstract protected String getPublisherKeyVariableName(ExternalTask externalTask, ExternalTaskService externalTaskService);
    
    abstract protected String getAssetKeyVariableName(ExternalTask externalTask, ExternalTaskService externalTaskService);

    protected void preExecution(ExternalTask externalTask, ExternalTaskService externalTaskService) {

    }

    protected void postExecution(ExternalTask externalTask, ExternalTaskService externalTaskService) {

    }

}