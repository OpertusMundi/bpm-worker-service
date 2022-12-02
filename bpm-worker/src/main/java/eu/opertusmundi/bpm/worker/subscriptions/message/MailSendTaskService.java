package eu.opertusmundi.bpm.worker.subscriptions.message;

import java.util.Map;
import java.util.UUID;

import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.service.MailService;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.model.email.EnumMailType;

@Service
public class MailSendTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(MailSendTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.mail-send.lock-duration:120000}")
    private Long lockDurationMillis;

    @Autowired
    private MailService mailService;

    @Override
    public String getTopicName() {
        return "sendMail";
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
            final String              mailType      = this.getVariableAsString(externalTask, externalTaskService, "mailType");
            final UUID                mailRecipient = this.getVariableAsUUID(externalTask, externalTaskService, "mailRecipient");
            final EnumMailType        type          = EnumMailType.valueOf(mailType);
            final Map<String, Object> variables     = externalTask.getAllVariables();

            logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

            this.mailService.sendMail(type, mailRecipient, variables);

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