package eu.opertusmundi.bpm.worker.subscriptions.asset;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.model.ErrorCodes;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.model.ServiceException;
import eu.opertusmundi.common.model.asset.AssetDraftDto;
import eu.opertusmundi.common.model.asset.AssetDraftSetStatusCommandDto;
import eu.opertusmundi.common.model.asset.EnumProviderAssetDraftStatus;
import eu.opertusmundi.common.model.asset.EnumResourceType;
import eu.opertusmundi.common.model.asset.ServiceResourceDto;
import eu.opertusmundi.common.model.catalogue.client.EnumAssetType;
import eu.opertusmundi.common.model.catalogue.client.EnumSpatialDataServiceType;
import eu.opertusmundi.common.model.geodata.EnumGeodataWorkspace;
import eu.opertusmundi.common.model.geodata.UserGeodataConfiguration;
import eu.opertusmundi.common.model.ingest.ResourceIngestionDataDto;
import eu.opertusmundi.common.service.ProviderAssetService;
import eu.opertusmundi.common.service.ogc.GeoServerUtils;
import eu.opertusmundi.common.service.ogc.OgcServiceMessageCode;
import eu.opertusmundi.common.service.ogc.UserGeodataConfigurationResolver;
import eu.opertusmundi.common.util.BpmInstanceVariablesBuilder;

@Service
public class GetCapabilitiesTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(GetCapabilitiesTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.get-capabilities.lock-duration:120000}")
    private Long lockDurationMillis;

    @Override
    protected long getLockDuration() {
        return this.lockDurationMillis;
    }

    @Autowired
    private ProviderAssetService providerAssetService;

    @Autowired
    private UserGeodataConfigurationResolver userGeodataConfigurationResolver;

    @Autowired
    private GeoServerUtils geoServerUtils;

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public String getTopicName() {
        return "getCapabilities";
    }

    @Override
    public final void execute(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        try {
            final String taskId = externalTask.getId();

            logger.info("Received task. [taskId={}]", taskId);

            final UUID          draftKey     = this.getDraftKey(externalTask, externalTaskService);
            final UUID          publisherKey = this.getPublisherKey(externalTask, externalTaskService);
            final EnumAssetType type         = this.getType(externalTask, externalTaskService);

            logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);
            if (type == EnumAssetType.SERVICE) {
                this.getCapabilities(publisherKey, draftKey);
            }

            // Update draft status
            final AssetDraftSetStatusCommandDto command   = new AssetDraftSetStatusCommandDto();
            final EnumProviderAssetDraftStatus  newStatus = EnumProviderAssetDraftStatus.PENDING_HELPDESK_REVIEW;

            command.setAssetKey(draftKey);
            command.setPublisherKey(publisherKey);
            command.setStatus(newStatus);

            this.providerAssetService.updateStatus(command);

            // Complete task
            final Map<String, Object> variables = BpmInstanceVariablesBuilder.builder()
                .variableAsString("status", newStatus.toString())
                .buildValues();

            logger.info("Completed task. [taskId={}]", taskId);
            externalTaskService.complete(externalTask, variables);
        } catch (final ServiceException ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleBpmnError(externalTaskService, externalTask, ErrorCodes.PublishAsset, ex);
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleFailure(externalTaskService, externalTask, ex);
        }
    }
    
    private void getCapabilities(UUID publisherKey, UUID draftKey) throws JsonProcessingException {
        final UserGeodataConfiguration geodataConfig = userGeodataConfigurationResolver.resolveFromUserKey(publisherKey, EnumGeodataWorkspace.PUBLIC);
        final AssetDraftDto            draft         = providerAssetService.findOneDraft(publisherKey, draftKey, false);

        final List<ResourceIngestionDataDto> services    = draft.getCommand().getIngestionInfo();
        final EnumSpatialDataServiceType     serviceType = draft.getCommand().getSpatialDataServiceType();

        // Process all services
        for (ResourceIngestionDataDto service : services) {
            // Find endpoint
            final ResourceIngestionDataDto.ServiceEndpoint endpoint = service.getEndpointByServiceType(serviceType);

            if (endpoint == null) {
                // Service type not supported
                throw BpmnWorkerException.builder()
                    .code(OgcServiceMessageCode.TYPE_NOT_SUPPORTED)
                    .message(String.format(
                        "Failed to load metadata for resource (layer). Endpoint not found [tableName=%s, type=%s]",
                        service.getTableName(), serviceType
                    ))
                    .build();
            }

            logger.info("Processing endpoint {}", endpoint.getUri());

            final ServiceResourceDto resource = this.geoServerUtils.getCapabilities(
                endpoint.getType(), geodataConfig.getUrl(), endpoint.getUri(), geodataConfig.getEffectiveWorkspace(), service.getTableName().toString()
            );

            if(resource == null) {
                throw BpmnWorkerException.builder()
                    .code(OgcServiceMessageCode.RESOURCE_NOT_CREATED)
                    .message(String.format(
                        "Failed to load metadata for resource (layer) [tableName=%s, type=%s, endpoint=%s]",
                        service.getTableName(), endpoint.getType(), endpoint.getUri()
                    ))
                    .build();
            }

            logger.info("Service capabilities {}", objectMapper.writeValueAsString(resource));

            // Set service resource properties
            resource.setEndpoint(endpoint.getUri());
            resource.setServiceType(endpoint.getType());
            resource.setType(EnumResourceType.SERVICE);

            // Link new resource with parent file resource
            resource.setId(UUID.randomUUID().toString());
            resource.setParentId(service.getKey());

            this.providerAssetService.addResource(publisherKey, draftKey, resource);
        }
    }

    private UUID getDraftKey(ExternalTask externalTask, ExternalTaskService externalTaskService) throws BpmnWorkerException {
        final String name     = "draftKey";
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

    private EnumAssetType getType(ExternalTask externalTask, ExternalTaskService externalTaskService) throws BpmnWorkerException {
        final String name      = "type";
        final String type = (String) externalTask.getVariable(name);

        if (StringUtils.isBlank(type)) {
            logger.error("Expected variable to be non empty. [name={}]", name);

            throw this.buildVariableNotFoundException(name);
        }

        return EnumAssetType.fromString(type);
    }

}