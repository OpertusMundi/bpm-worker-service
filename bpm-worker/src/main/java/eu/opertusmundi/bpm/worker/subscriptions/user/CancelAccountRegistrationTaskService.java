package eu.opertusmundi.bpm.worker.subscriptions.user;

import java.util.UUID;

import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.domain.AccountEntity;
import eu.opertusmundi.common.model.account.AccountMessageCode;
import eu.opertusmundi.common.model.account.EnumActivationStatus;
import eu.opertusmundi.common.repository.AccountRepository;

@Service
public class CancelAccountRegistrationTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(CancelAccountRegistrationTaskService.class);

    private static final String VARIABLE_USER_KEY = "userKey";
    
    @Value("${opertusmundi.bpm.worker.tasks.cancel-account-registration.lock-duration:10000}")
    private Long lockDurationMillis;

    @Autowired
    private AccountRepository accountRepository;

    @Override
    public String getTopicName() {
        return "cancelAccountRegistration";
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

            final UUID userKey = this.getVariableAsUUID(externalTaskService, externalTask, VARIABLE_USER_KEY);

            logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

            this.cancelAccountRegistration(userKey);
            
            externalTaskService.complete(externalTask);

            logger.info("Completed task. [taskId={}]", taskId);
        } catch (final BpmnWorkerException ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            externalTaskService.handleFailure(
                externalTask, ex.getMessage(), ex.getErrorDetails(), ex.getRetries(), ex.getRetryTimeout()
            );
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleError(externalTaskService, externalTask, ex);
        }
    }

    @Transactional
    private void cancelAccountRegistration(UUID userKey) throws BpmnWorkerException {
        final AccountEntity account = this.accountRepository.findOneByKey(userKey).orElse(null);

        if (account == null) {
            final String message = String.format("Account not found [userKey=%s]", userKey);

            throw this.buildException(AccountMessageCode.ACCOUNT_NOT_FOUND, message, message);
        }

        if (account.getActivationStatus() != EnumActivationStatus.PENDING) {
            final String message = String.format(
                "Invalid account status. Expected status [PENDING]. Found [%s]. [userKey=%s]",
                account.getActivationStatus(), userKey
            );

            throw this.buildException(AccountMessageCode.INVALID_ACCOUNT_STATUS, message, message);
        }
        
        this.accountRepository.cancelAccountRegistration(userKey);
    }

}
