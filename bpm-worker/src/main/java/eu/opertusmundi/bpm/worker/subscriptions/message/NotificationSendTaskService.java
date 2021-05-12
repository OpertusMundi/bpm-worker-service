package eu.opertusmundi.bpm.worker.subscriptions.message;

import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.feign.client.MessageServiceFeignClient;
import eu.opertusmundi.common.model.message.client.EnumNotificationType;
import eu.opertusmundi.common.model.message.server.ServerNotificationCommandDto;

@Service
public class NotificationSendTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(NotificationSendTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.notification-send.lock-duration:10000}")
    private Long lockDurationMillis;

    @Autowired
    private ObjectMapper objectMapper;
    
    @Autowired
    private ObjectProvider<MessageServiceFeignClient> messageClient;

    @Override
    public String getTopicName() {
        return "sendNotification";
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

            // Get parameters
            final String notificationType          = this.getVariableAsString(externalTask, externalTaskService, "notificationType");
            final UUID   notificationRecipient     = this.getVariableAsUUID(externalTaskService, externalTask, "notificationRecipient");
            
            logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

            // Build notification message
            final ServerNotificationCommandDto notification = ServerNotificationCommandDto.builder()
                .data(this.collectNotificationData(externalTask, externalTaskService, notificationType))
                .eventType(notificationType)
                .recipient(notificationRecipient)
                .text(this.composeNotificationText(externalTask, externalTaskService, notificationType))
                .build();
            
            messageClient.getObject().sendNotification(notification);

            // Complete task
            externalTaskService.complete(externalTask);

            logger.info("Completed task. [taskId={}]", taskId);
        } catch (final BpmnWorkerException ex) {
            logger.error(String.format("Operation has failed. [details=%s]", ex.getErrorDetails()), ex);
            
            externalTaskService.handleFailure(
                externalTask, ex.getMessage(), ex.getErrorDetails(), ex.getRetries(), ex.getRetryTimeout()
            );
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleError(externalTaskService, externalTask, ex);
        }
    }
    
    private String composeNotificationText(ExternalTask externalTask, ExternalTaskService externalTaskService, String eventTypeValue) {
        // TODO: Move messages to properties file or database store
        
        if (StringUtils.isBlank(eventTypeValue)) {
            return null;
        }

        EnumNotificationType eventType = EnumNotificationType.valueOf(eventTypeValue);
               
        switch (eventType) {
            case CATALOGUE_HARVEST_COMPLETED :
                return String.format(
                    "Harvest operation for catalogue %s has been completed",
                    this.getVariableAsString(externalTask, externalTaskService, "catalogueUrl")
                );
        }
        
        return "";
    }
    
    private JsonNode collectNotificationData(ExternalTask externalTask, ExternalTaskService externalTaskService, String eventTypeValue) {
        if (StringUtils.isBlank(eventTypeValue)) {
            return null;
        }

        EnumNotificationType eventType = EnumNotificationType.valueOf(eventTypeValue);

        switch (eventType) {
            case CATALOGUE_HARVEST_COMPLETED :
                final ObjectNode data = objectMapper.createObjectNode();
                data.put("catalogueUrl", this.getVariableAsString(externalTask, externalTaskService, "catalogueUrl"));
                data.put("catalogueType", this.getVariableAsString(externalTask, externalTaskService, "catalogueType"));
                return data;
        }

        return null;
    }

}