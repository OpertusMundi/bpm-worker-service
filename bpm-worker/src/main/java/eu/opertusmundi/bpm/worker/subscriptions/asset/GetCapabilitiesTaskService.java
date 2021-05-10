package eu.opertusmundi.bpm.worker.subscriptions.asset;

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
import eu.opertusmundi.common.model.asset.AssetDraftSetStatusCommandDto;
import eu.opertusmundi.common.model.asset.EnumProviderAssetDraftStatus;
import eu.opertusmundi.common.model.asset.ServiceResourceDto;
import eu.opertusmundi.common.model.catalogue.client.EnumSpatialDataServiceType;
import eu.opertusmundi.common.model.catalogue.client.EnumType;
import eu.opertusmundi.common.model.ingest.ResourceIngestionDataDto;
import eu.opertusmundi.common.service.ProviderAssetService;
import eu.opertusmundi.common.util.GeoServerUtils;
import eu.opertusmundi.common.util.GetCapabilitiesServiceMessageCode;

@Service
public class GetCapabilitiesTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(IngestTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.get-capabilities.lock-duration:60000}")
    private Long lockDurationMillis;

    @Override
    protected long getLockDuration() {
        return this.lockDurationMillis;
    }

    @Autowired
    private ProviderAssetService providerAssetService;
    
    @Autowired
    private GeoServerUtils geoServerUtils;

    @Override
    public String getTopicName() {
        return "getCapabilities";
    }
    
    @Override
    public final void execute(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        try {
            final String taskId = externalTask.getId();

            logger.info("Received task {}", taskId);

            this.preExecution(externalTask, externalTaskService);
           
            final UUID     draftKey     = this.getAssetKey(externalTask, externalTaskService);
            final UUID     publisherKey = this.getPublisherKey(externalTask, externalTaskService);
            final EnumType type         = this.getType(externalTask, externalTaskService);

            if (type != EnumType.SERVICE) {
                throw BpmnWorkerException.builder()
                    .code(GetCapabilitiesServiceMessageCode.TYPE_NOT_SUPPORTED)
                    .message(String.format("Asset type [%s] is not supported", type))
                    .build();
            }
            
            final AssetDraftDto draft = providerAssetService.findOneDraft(publisherKey, draftKey);

            final List<ResourceIngestionDataDto> services    = draft.getCommand().getIngestionInfo();
            final EnumSpatialDataServiceType     serviceType = draft.getCommand().getSpatialDataServiceType();

            logger.debug("Processing task {}: {}", taskId, externalTask);
            
            // Process all services
            for (ResourceIngestionDataDto service : services) {
                // Find endpoint
                final ResourceIngestionDataDto.ServiceEndpoint endpoint = service.getEndpointByServiceType(serviceType);

                if (endpoint == null) {
                    // Service type not supported
                    continue;
                }

                final ServiceResourceDto resource = this.geoServerUtils.getCapabilities(
                    endpoint.getType(), endpoint.getUri(), service.getTableName().toString()
                );
                
                if(resource == null) {
                    throw BpmnWorkerException.builder()
                        .code(GetCapabilitiesServiceMessageCode.RESOURCE_NOT_CREATED)
                        .message(String.format("Failed to load metadata for resource (layer) [%s]", service.getTableName()))
                        .build();
                }
                
                // Link new resource with parent file resource
                resource.setId(UUID.randomUUID());
                resource.setParentId(service.getKey());

                this.providerAssetService.addServiceResource(publisherKey, draftKey, resource);
                
                // Update draft status
                final AssetDraftSetStatusCommandDto command = new AssetDraftSetStatusCommandDto();

                command.setAssetKey(draftKey);
                command.setPublisherKey(publisherKey);
                command.setStatus(EnumProviderAssetDraftStatus.PENDING_HELPDESK_REVIEW);

                this.providerAssetService.updateStatus(command);
            }    

            // Complete task
            logger.debug("Completed task {}: {}", taskId, externalTask);

            this.postExecution(externalTask, externalTaskService);

            externalTaskService.complete(externalTask);

            logger.info("Completed task {}", taskId);
        } catch (final BpmnWorkerException ex) {
            logger.error(String.format("[GetCapabilities] Operation has failed. Error details: %s", ex.getErrorDetails()), ex);

            externalTaskService.handleFailure(
                externalTask, ex.getMessage(), ex.getErrorDetails(), ex.getRetries(), ex.getRetryTimeout()
            );
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleError(externalTaskService, externalTask, ex);
        }
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

    private EnumType getType(ExternalTask externalTask, ExternalTaskService externalTaskService) throws BpmnWorkerException {
        final String name      = "type";
        final String type = (String) externalTask.getVariable(name);

        if (StringUtils.isBlank(type)) {
            logger.error(String.format("Expected variable [%s] to be non empty!", name));

            throw this.buildVariableNotFoundException(name);
        }

        return EnumType.fromString(type);
    }
    
    protected void preExecution(ExternalTask externalTask, ExternalTaskService externalTaskService) {

    }

    protected void postExecution(ExternalTask externalTask, ExternalTaskService externalTaskService) {

    }

}