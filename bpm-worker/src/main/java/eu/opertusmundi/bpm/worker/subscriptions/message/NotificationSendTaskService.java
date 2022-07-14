package eu.opertusmundi.bpm.worker.subscriptions.message;

import java.util.Map;
import java.util.UUID;

import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.feign.client.MessageServiceFeignClient;
import eu.opertusmundi.common.model.message.EnumNotificationType;
import eu.opertusmundi.common.model.message.server.ServerNotificationCommandDto;
import eu.opertusmundi.common.service.messaging.NotificationMessageHelper;

@Service
public class NotificationSendTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(NotificationSendTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.notification-send.lock-duration:120000}")
    private Long lockDurationMillis;

    @Autowired
    private ObjectProvider<MessageServiceFeignClient> messageClient;

    @Autowired
    private NotificationMessageHelper notificationMessageBuilder;

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
            final UUID                 businessKey           = UUID.fromString(externalTask.getBusinessKey());
            final String               notificationType      = this.getVariableAsString(externalTask, externalTaskService, "notificationType");
            final UUID                 notificationRecipient = this.getVariableAsUUID(externalTask, externalTaskService, "notificationRecipient");
            final String               idempotentKey         = businessKey.toString() + "::" + notificationType;
            final EnumNotificationType type                  = EnumNotificationType.valueOf(notificationType);
            final Map<String, Object>  variables             = externalTask.getAllVariables();

            logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

            // Build notification message
            final JsonNode data = this.notificationMessageBuilder.collectNotificationData(type, variables);

            final ServerNotificationCommandDto notification = ServerNotificationCommandDto.builder()
                .data(data)
                .eventType(notificationType)
                .idempotentKey(idempotentKey)
                .recipient(notificationRecipient)
                .text(this.notificationMessageBuilder.composeNotificationText(type, data))
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

            this.handleFailure(externalTaskService, externalTask, ex);
        }
    }

}