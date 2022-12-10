package eu.opertusmundi.bpm.worker.service;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;

import eu.opertusmundi.common.model.Message;
import eu.opertusmundi.common.model.asset.AssetDraftDto;
import eu.opertusmundi.common.model.asset.EnumProviderAssetDraftStatus;
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
@Transactional
public class DefaultAssetDraftWorkerService extends BaseWorkerService implements AssetDraftWorkerService {

    private final IngestService                    ingestService;
    private final MessageService                   messageService;
    private final ProviderAssetService             providerAssetService;
    private final UserGeodataConfigurationResolver userGeodataConfigurationResolver;
    private final UserServiceService               userServiceService;

    @Autowired
    public DefaultAssetDraftWorkerService(
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
    public void cancelPublishAsset(
        ExternalTask externalTask, ExternalTaskService externalTaskService
    ) throws JsonMappingException, JsonProcessingException {
        final UUID          draftKey     = this.getVariableAsUUID(externalTask, externalTaskService, "draftKey");
        final UUID          publisherKey = this.getVariableAsUUID(externalTask, externalTaskService, "publisherKey");
        final AssetDraftDto draft        = providerAssetService.findOneDraft(draftKey);
        if (draft.getStatus() != EnumProviderAssetDraftStatus.CANCELLED) {
            final String errorDetails  = this.getErrorDetails(externalTask, externalTaskService);
            final String errorMessages = this.getErrorMessages(externalTask, externalTaskService);

            var           userGeodataConfig = userGeodataConfigurationResolver.resolveFromUserKey(publisherKey, EnumGeodataWorkspace.PUBLIC);
            List<Message> messages          = objectMapper.readValue(errorMessages, new TypeReference<List<Message>>() { });
    
            // Remove all ingested resources
    
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
    }

    @Override
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
