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
import eu.opertusmundi.common.model.payment.UserRegistrationCommand;
import eu.opertusmundi.common.service.PaymentService;
import eu.opertusmundi.common.service.ProviderRegistrationService;

@Service
public class UpdateProviderTaskService extends AbstractCustomerTaskService {

    private static final Logger logger = LoggerFactory.getLogger(UpdateProviderTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.provider-registration.lock-duration:10000}")
    private Long lockDurationMillis;

    @Autowired
    private ProviderRegistrationService registrationService;

    @Autowired
    private PaymentService paymentService;

    @Override
    public String getTopicName() {
        return "updateProvider";
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

            final UUID                    userKey         = this.getUserKey(externalTask, externalTaskService);
            final UUID                    registrationKey = this.getRegistrationKey(externalTask, externalTaskService);
            final UserRegistrationCommand command         = UserRegistrationCommand.of(userKey, registrationKey);
            
            logger.debug("Processing task {}: {}", taskId, externalTask);

            this.paymentService.updateUser(command);

            this.paymentService.updateBankAccount(command);

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
