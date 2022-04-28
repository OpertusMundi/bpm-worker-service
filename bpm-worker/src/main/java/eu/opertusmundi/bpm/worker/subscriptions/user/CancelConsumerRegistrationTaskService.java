package eu.opertusmundi.bpm.worker.subscriptions.user;

import java.util.List;
import java.util.UUID;

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
import eu.opertusmundi.common.domain.AccountEntity;
import eu.opertusmundi.common.model.Message;
import eu.opertusmundi.common.model.account.AccountMessageCode;
import eu.opertusmundi.common.repository.AccountRepository;

@Service
public class CancelConsumerRegistrationTaskService extends AbstractCustomerTaskService {

    private static final Logger logger = LoggerFactory.getLogger(CancelAccountRegistrationTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.cancel-consumer-registration.lock-duration:120000}")
    private Long lockDurationMillis;

    @Autowired
    private AccountRepository accountRepository;

    @Override
    public String getTopicName() {
        return "cancelConsumerRegistration";
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

            final UUID   userKey       = this.getUserKey(externalTask, externalTaskService);
            final String errorMessages = this.getErrorMessages(externalTask, externalTaskService);

            logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

            this.cancelRegistration(userKey, errorMessages);

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
    private void cancelRegistration(UUID userKey, String errorMessages) throws BpmnWorkerException, JsonMappingException, JsonProcessingException {
        final AccountEntity account = this.accountRepository.findOneByKey(userKey).orElse(null);

        if (account == null) {
            final String message = String.format("Account not found [userKey=%s]", userKey);

            throw this.buildException(AccountMessageCode.ACCOUNT_NOT_FOUND, message, message);
        }

        List<Message> messages = objectMapper.readValue(errorMessages, new TypeReference<List<Message>>() { });
        
        this.accountRepository.failConsumerRegistration(userKey, messages);
    }

}
