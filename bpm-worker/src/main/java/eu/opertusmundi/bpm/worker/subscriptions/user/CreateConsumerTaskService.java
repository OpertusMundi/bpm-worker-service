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
import eu.opertusmundi.common.model.payment.UserRegistrationCommandDto;
import eu.opertusmundi.common.service.ConsumerRegistrationService;
import eu.opertusmundi.common.service.PaymentService;

@Service
public class CreateConsumerTaskService extends AbstractCustomerTaskService {

    private static final Logger logger = LoggerFactory.getLogger(CreateConsumerTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.consumer-registration.lock-duration:10000}")
    private Long lockDurationMillis;

    @Autowired
    private ConsumerRegistrationService registrationService;

    @Autowired
    private PaymentService paymentService;

    @Override
    public String getTopicName() {
        return "createConsumer";
    }

    @Override
    protected long getLockDuration() {
        return this.lockDurationMillis;
    }

    @Override
    public void execute(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        try {
            final String taskId = externalTask.getId();

            logger.info("Received task {}", taskId);

            final UUID                       userKey         = this.getUserKey(externalTask, externalTaskService);
            final UUID                       registrationKey = this.getRegistrationKey(externalTask, externalTaskService);
            final UserRegistrationCommandDto command         = UserRegistrationCommandDto.of(userKey, registrationKey);

            logger.debug("Processing task {}: {}", taskId, externalTask);

            this.paymentService.createUser(command);

            this.paymentService.createWallet(command);

            this.registrationService.completeRegistration(userKey);

            externalTaskService.complete(externalTask);

            logger.info("Completed task {}", taskId);
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

}
