package eu.opertusmundi.bpm.worker.subscriptions.user;

import java.util.UUID;

import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.service.AccountActivationService;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.model.account.AccountDto;
import eu.opertusmundi.common.util.BpmInstanceVariablesBuilder;

@Service
public class ActivateAccountTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(ActivateAccountTaskService.class);

    private static final String VARIABLE_USER_KEY          = "userKey";
    private static final String VARIABLE_REGISTER_CONSUMER = "registerConsumer";

    @Value("${opertusmundi.bpm.worker.tasks.activate-account.lock-duration:120000}")
    private Long lockDurationMillis;

    
    @Autowired
    private AccountActivationService accountActivationService;

    @Override
    public String getTopicName() {
        return "activateAccount";
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

            final UUID    userKey          = this.getVariableAsUUID(externalTask, externalTaskService, VARIABLE_USER_KEY);
            final boolean registerConsumer = this.getVariableAsBoolean(externalTask, externalTaskService, VARIABLE_REGISTER_CONSUMER, false);

            logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

            // Complete account registration
            final AccountDto account = this.accountActivationService.completeAccountRegistration(userKey, registerConsumer);

            // Complete task
            final BpmInstanceVariablesBuilder builder = BpmInstanceVariablesBuilder.builder();
            if (registerConsumer == true) {
                final UUID registrationKey = account.getProfile().getConsumer().getDraft().getKey();
                builder.variableAsString("registrationKey", registrationKey.toString());
            }

            externalTaskService.complete(externalTask, builder.buildValues());
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
}
