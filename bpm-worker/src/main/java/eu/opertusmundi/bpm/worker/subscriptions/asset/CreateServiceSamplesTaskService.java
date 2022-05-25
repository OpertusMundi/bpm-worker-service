package eu.opertusmundi.bpm.worker.subscriptions.asset;

import java.util.List;
import java.util.UUID;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.model.ServiceException;
import eu.opertusmundi.common.model.asset.AssetDraftDto;
import eu.opertusmundi.common.model.catalogue.client.CatalogueItemMetadataCommandDto;
import eu.opertusmundi.common.model.catalogue.client.EnumAssetType;
import eu.opertusmundi.common.model.catalogue.client.EnumSpatialDataServiceType;
import eu.opertusmundi.common.model.catalogue.client.ServiceResourceSampleAreaDto;
import eu.opertusmundi.common.model.catalogue.client.WfsLayerSample;
import eu.opertusmundi.common.model.catalogue.client.WmsLayerSample;
import eu.opertusmundi.common.model.ingest.ResourceIngestionDataDto;
import eu.opertusmundi.common.service.ProviderAssetService;
import eu.opertusmundi.common.service.ogc.GeoServerUtils;
import eu.opertusmundi.common.service.ogc.OgcServiceMessageCode;
import eu.opertusmundi.common.util.StreamUtils;

@Service
public class CreateServiceSamplesTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(CreateServiceSamplesTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.create-service-samples.lock-duration:120000}")
    private Long lockDurationMillis;

    @Override
    protected long getLockDuration() {
        return this.lockDurationMillis;
    }

    @Autowired
    private ProviderAssetService providerAssetService;

    @Autowired
    private GeoServerUtils geoServerUtils;

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public String getTopicName() {
        return "createSamples";
    }

    @Override
    public final void execute(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        try {
            final String taskId = externalTask.getId();

            logger.info("Received task. [taskId={}]", taskId);

            this.preExecution(externalTask, externalTaskService);

            final UUID          draftKey     = this.getDraftKey(externalTask, externalTaskService);
            final UUID          publisherKey = this.getPublisherKey(externalTask, externalTaskService);
            final EnumAssetType type         = this.getType(externalTask, externalTaskService);

            if (type == EnumAssetType.SERVICE) {
                final AssetDraftDto draft = providerAssetService.findOneDraft(publisherKey, draftKey, false);

                final List<ResourceIngestionDataDto> services    = draft.getCommand().getIngestionInfo();
                final EnumSpatialDataServiceType     serviceType = draft.getCommand().getSpatialDataServiceType();

                logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

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

                    // Find endpoint
                    final ServiceResourceSampleAreaDto sampleAreas = StreamUtils.from(draft.getCommand().getSampleAreas())
                        .filter(r-> r.getId().equals(service.getKey()))
                        .findFirst()
                        .orElse(null);

                    logger.info("Processing endpoint {}", endpoint.getUri());

                    if (sampleAreas != null && !CollectionUtils.isEmpty(sampleAreas.getAreas())) {
                        final CatalogueItemMetadataCommandDto command = new CatalogueItemMetadataCommandDto();

                        switch (serviceType) {
                            case WMS :
                                final List<WmsLayerSample> images = this.geoServerUtils.getWmsSamples(service, sampleAreas.getAreas());
                                command.setSamples(this.objectMapper.valueToTree(images));
                                break;
                            case WFS :
                                final List<WfsLayerSample> features = this.geoServerUtils.getWfsSamples(service, sampleAreas.getAreas());
                                command.setSamples(this.objectMapper.valueToTree(features));
                                break;
                            default :
                                // Ignore
                        }

                        command.setDraftKey(draftKey);
                        command.setOwnerKey(publisherKey);
                        command.setPublisherKey(publisherKey);
                        command.setResourceKey(UUID.fromString(service.getKey()));
                        command.setVisibility(null);
                        command.setSampleAreas(null);

                        this.providerAssetService.updateDraftMetadata(command);
                    }
                }
            }

            // Complete task
            this.postExecution(externalTask, externalTaskService);

            externalTaskService.complete(externalTask);

            logger.info("Completed task. [taskId={}]", taskId);
        } catch (final ServiceException ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleBpmnError(externalTaskService, externalTask, ex);
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleFailure(externalTaskService, externalTask, ex);
        }
    }

    private UUID getDraftKey(ExternalTask externalTask, ExternalTaskService externalTaskService) throws BpmnWorkerException {
        final String draftKey = (String) externalTask.getVariable("draftKey");
        if (StringUtils.isBlank(draftKey)) {
            logger.error("Expected draft key to be non empty");

            throw this.buildVariableNotFoundException("draftKey");
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

    protected void preExecution(ExternalTask externalTask, ExternalTaskService externalTaskService) {

    }

    protected void postExecution(ExternalTask externalTask, ExternalTaskService externalTaskService) {

    }

}
