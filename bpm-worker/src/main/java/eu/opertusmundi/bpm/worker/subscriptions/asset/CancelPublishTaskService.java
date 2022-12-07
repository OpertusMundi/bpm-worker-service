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
import eu.opertusmundi.common.model.geodata.EnumGeodataWorkspace;
import eu.opertusmundi.common.model.message.client.ClientMessageCommandDto;
import eu.opertusmundi.common.service.IngestService;
import eu.opertusmundi.common.service.ProviderAssetService;
import eu.opertusmundi.common.service.UserServiceService;
import eu.opertusmundi.common.service.messaging.MessageService;
import eu.opertusmundi.common.service.ogc.UserGeodataConfigurationResolver;

@Service
public class CancelPublishTaskService extends AbstractCustomerTaskService {

    private static final Logger logger = LoggerFactory.getLogger(CancelPublishTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.cancel-publish.lock-duration:120000}")
    private Long lockDurationMillis;

    private final IngestService                    ingestService;
    private final MessageService                   messageService;
    private final ProviderAssetService             providerAssetService;
    private final UserGeodataConfigurationResolver userGeodataConfigurationResolver;
    private final UserServiceService               userServiceService;
    
    @Autowired
    public CancelPublishTaskService(
        IngestService                    ingestService,
        MessageService                   messageService,
        ProviderAssetService             providerAssetService,
        UserGeodataConfigurationResolver userGeodataConfigurationResolver,
        UserServiceService               userServiceService
    ) {
        this.ingestService                    = ingestService;
        this.messageService                   = messageService;
        this.providerAssetService             = providerAssetService;
        this.userGeodataConfigurationResolver = userGeodataConfigurationResolver;
        this.userServiceService               = userServiceService;
    }
    
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
        final UUID   draftKey      = this.getVariableAsUUID(externalTask, externalTaskService, "draftKey");
        final UUID   publisherKey  = this.getVariableAsUUID(externalTask, externalTaskService, "publisherKey");
        final String errorDetails  = this.getErrorDetails(externalTask, externalTaskService);
        final String errorMessages = this.getErrorMessages(externalTask, externalTaskService);

        var           userGeodataConfig = userGeodataConfigurationResolver.resolveFromUserKey(publisherKey, EnumGeodataWorkspace.PUBLIC);
        List<Message> messages          = objectMapper.readValue(errorMessages, new TypeReference<List<Message>>() { });

        // Remove all ingested resources
        final AssetDraftDto            draft            = providerAssetService.findOneDraft(draftKey);
        final List<ServiceResourceDto> serviceResources = draft.getCommand().getResources().stream()
            .filter(r -> r.getType() == EnumResourceType.SERVICE)
            .map(r -> (ServiceResourceDto) r)
            .collect(Collectors.toList());

        for (final ServiceResourceDto r : serviceResources) {
            this.deleteIngestedResource(userGeodataConfig.getShard(), userGeodataConfig.getEffectiveWorkspace(), r.getId());
        } ;

        // Reset draft
        providerAssetService.cancelPublishDraft(publisherKey, draftKey, errorDetails, messages);
        // Send message to provider
        var subject        = String.format("Asset Publish Failure: %s %s", draft.getTitle(), draft.getVersion());
        var text           = draft.getHelpdeskErrorMessage();
        var messageCommand = ClientMessageCommandDto.of(subject, text);
        messageService.sendMessage(draft.getHelpdeskSetErrorAccount().getKey(), draft.getPublisher().getKey(), messageCommand);
        
    }

    @Transactional
    public void cancelPublishUserService(
        ExternalTask externalTask, ExternalTaskService externalTaskService
    ) throws JsonMappingException, JsonProcessingException {
        final UUID   ownerKey      = this.getVariableAsUUID(externalTask, externalTaskService, "ownerKey");
        final UUID   serviceKey    = this.getVariableAsUUID(externalTask, externalTaskService, "serviceKey");
        final String errorDetails  = this.getErrorDetails(externalTask, externalTaskService);
        final String errorMessages = this.getErrorMessages(externalTask, externalTaskService);

        var userGeodataConfig = userGeodataConfigurationResolver.resolveFromUserKey(ownerKey, EnumGeodataWorkspace.PRIVATE);

        List<Message> messages = objectMapper.readValue(errorMessages, new TypeReference<List<Message>>() {});

        // Remove all ingested resources
        final UserServiceDto service = userServiceService.findOne(serviceKey);
        this.deleteIngestedResource(userGeodataConfig.getShard(), userGeodataConfig.getEffectiveWorkspace(), service.getKey().toString());

        // Reset draft
        userServiceService.cancelPublishOperation(ownerKey, serviceKey, errorDetails, messages);
        // Send message to provider
        var subject        = String.format("User Service Publish Failure: %s %s", service.getTitle(), service.getVersion());
        var text           = service.getHelpdeskErrorMessage();
        var messageCommand = ClientMessageCommandDto.of(subject, text);
        messageService.sendMessage(service.getHelpdeskSetErrorAccount().getKey(), service.getOwner().getKey(), messageCommand);
    }

    private void deleteIngestedResource(String shard, String workspace, String tableName) {
        this.ingestService.removeDataAndLayer(shard, workspace, tableName);
    }
}
