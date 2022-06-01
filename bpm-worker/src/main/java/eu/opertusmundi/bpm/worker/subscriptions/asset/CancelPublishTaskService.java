package eu.opertusmundi.bpm.worker.subscriptions.asset;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.subscriptions.user.AbstractCustomerTaskService;
import eu.opertusmundi.common.model.Message;
import eu.opertusmundi.common.model.ServiceException;
import eu.opertusmundi.common.model.asset.AssetDraftDto;
import eu.opertusmundi.common.model.asset.EnumResourceType;
import eu.opertusmundi.common.model.asset.ServiceResourceDto;
import eu.opertusmundi.common.model.ingest.IngestServiceException;
import eu.opertusmundi.common.service.IngestService;
import eu.opertusmundi.common.service.ProviderAssetService;

@Service
public class CancelPublishTaskService extends AbstractCustomerTaskService {

    private static final Logger logger = LoggerFactory.getLogger(CancelPublishTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.cancel-publish.lock-duration:120000}")
    private Long lockDurationMillis;

    @Autowired
    private IngestService ingestService;

    @Autowired
    private ProviderAssetService providerAssetService;

    @Override
    public String getTopicName() {
        return "cancelPublish";
    }

    @Override
    protected long getLockDuration() {
        return this.lockDurationMillis;
    }

    @Override
    public void execute(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        try {
            final String taskId = externalTask.getId();

            logger.info("Received task. [taskId={}]", taskId);

            final UUID   draftKey      = this.getDraftKey(externalTask, externalTaskService);
            final UUID   publisherKey  = this.getPublisherKey(externalTask, externalTaskService);
            final String errorDetails = this.getErrorDetails(externalTask, externalTaskService);
            final String errorMessages = this.getErrorMessages(externalTask, externalTaskService);

            logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

            this.cancelPublish(publisherKey, draftKey, errorDetails, errorMessages);

            externalTaskService.complete(externalTask);

            logger.info("Completed task. [taskId={}]", taskId);
        } catch (final BpmnWorkerException ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            externalTaskService.handleFailure(
                externalTask, ex.getMessage(), ex.getErrorDetails(), ex.getRetries(), ex.getRetryTimeout()
            );
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleFailure(externalTaskService, externalTask, ex);
        }
    }

    @Transactional
    private void cancelPublish(
        UUID publisherKey, UUID draftKey, String errorDetails, String errorMessages
    ) throws ServiceException, JsonMappingException, JsonProcessingException {
        List<Message> messages = objectMapper.readValue(errorMessages, new TypeReference<List<Message>>() { });

        // Remove all ingested resources
        final AssetDraftDto            draft            = providerAssetService.findOneDraft(draftKey);
        final List<ServiceResourceDto> serviceResources = draft.getCommand().getResources().stream()
            .filter(r -> r.getType() == EnumResourceType.SERVICE)
            .map(r -> (ServiceResourceDto) r)
            .collect(Collectors.toList());

        for (final ServiceResourceDto r : serviceResources) {
            this.deleteIngestedResource(r);
        } ;

        // Reset draft
        providerAssetService.cancelPublishDraft(publisherKey, draftKey, errorDetails, messages);
    }

    private void deleteIngestedResource(ServiceResourceDto resource) {
        final String tableName = resource.getId();
        try {
            this.ingestService.removeLayerAndData(tableName, null, null);
        } catch (IngestServiceException ex) {
            logger.error(String.format("Failed to remove data and layer for resource [tableName=%s]", tableName), ex);
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
        final String publisherKey = (String) externalTask.getVariable("publisherKey");
        if (StringUtils.isBlank(publisherKey)) {
            logger.error("Expected publisher key to be non empty");

            throw this.buildVariableNotFoundException("publisherKey");
        }

        return UUID.fromString(publisherKey);
    }

}
