package eu.opertusmundi.bpm.worker.subscriptions.asset;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

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
import eu.opertusmundi.bpm.worker.model.EnumPublishRequestType;
import eu.opertusmundi.bpm.worker.subscriptions.user.AbstractCustomerTaskService;
import eu.opertusmundi.common.model.Message;
import eu.opertusmundi.common.model.asset.AssetDraftDto;
import eu.opertusmundi.common.model.asset.EnumResourceType;
import eu.opertusmundi.common.model.asset.ServiceResourceDto;
import eu.opertusmundi.common.model.asset.service.UserServiceDto;
import eu.opertusmundi.common.service.IngestService;
import eu.opertusmundi.common.service.ProviderAssetService;
import eu.opertusmundi.common.service.UserServiceService;

@Service
public class CancelPublishTaskService extends AbstractCustomerTaskService {

    private static final Logger logger = LoggerFactory.getLogger(CancelPublishTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.cancel-publish.lock-duration:120000}")
    private Long lockDurationMillis;

    @Autowired
    private IngestService ingestService;

    @Autowired
    private ProviderAssetService providerAssetService;

    @Autowired
    private UserServiceService userServiceService;

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
        final String taskId = externalTask.getId();

        logger.info("Received task. [taskId={}]", taskId);

        final String                 requestType = this.getVariableAsString(externalTask, externalTaskService, "requestType");
        final EnumPublishRequestType type        = EnumPublishRequestType.valueOf(requestType);

        logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

        try {
            switch (type) {
                case CATALOGUE_ASSET :
                    this.cancelPublishAsset(externalTask, externalTaskService);
                    break;
                case USER_SERVICE :
                    this.cancelPublishUserService(externalTask, externalTaskService);
                    break;
            }
            
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
    public void cancelPublishAsset(
        ExternalTask externalTask, ExternalTaskService externalTaskService
    ) throws JsonMappingException, JsonProcessingException {
        final UUID   draftKey      = this.getVariableAsUUID(externalTaskService, externalTask, "draftKey");
        final UUID   publisherKey  = this.getVariableAsUUID(externalTaskService, externalTask, "publisherKey");
        final String errorDetails  = this.getErrorDetails(externalTask, externalTaskService);
        final String errorMessages = this.getErrorMessages(externalTask, externalTaskService);

        List<Message> messages = objectMapper.readValue(errorMessages, new TypeReference<List<Message>>() { });

        // Remove all ingested resources
        final AssetDraftDto            draft            = providerAssetService.findOneDraft(draftKey);
        final List<ServiceResourceDto> serviceResources = draft.getCommand().getResources().stream()
            .filter(r -> r.getType() == EnumResourceType.SERVICE)
            .map(r -> (ServiceResourceDto) r)
            .collect(Collectors.toList());

        for (final ServiceResourceDto r : serviceResources) {
            this.deleteIngestedResource(r.getId());
        } ;

        // Reset draft
        providerAssetService.cancelPublishDraft(publisherKey, draftKey, errorDetails, messages);
    }

    @Transactional
    public void cancelPublishUserService(
        ExternalTask externalTask, ExternalTaskService externalTaskService
    ) throws JsonMappingException, JsonProcessingException {
        final UUID   ownerKey      = this.getVariableAsUUID(externalTaskService, externalTask, "ownerKey");
        final UUID   serviceKey    = this.getVariableAsUUID(externalTaskService, externalTask, "serviceKey");
        final String errorDetails  = this.getErrorDetails(externalTask, externalTaskService);
        final String errorMessages = this.getErrorMessages(externalTask, externalTaskService);

        List<Message> messages = objectMapper.readValue(errorMessages, new TypeReference<List<Message>>() { });

        // Remove all ingested resources
        final UserServiceDto service = userServiceService.findOne(serviceKey);
        this.deleteIngestedResource(service.getKey().toString());

        // Reset draft
        userServiceService.cancelPublishOperation(ownerKey, serviceKey, errorDetails, messages);
    }
    
    private void deleteIngestedResource(String tableName) {
        this.ingestService.removeLayerAndData(tableName, null, null);
    }
}
