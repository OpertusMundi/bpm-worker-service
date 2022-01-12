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
import eu.opertusmundi.common.model.account.EnumCustomerType;
import eu.opertusmundi.common.model.payment.UserRegistrationCommand;
import eu.opertusmundi.common.service.ProviderRegistrationService;
import eu.opertusmundi.common.service.mangopay.PaymentService;

@Service
public class CreateProviderTaskService extends AbstractCustomerTaskService {

    private static final Logger logger = LoggerFactory.getLogger(CreateProviderTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.provider-registration.lock-duration:10000}")
    private Long lockDurationMillis;

    @Autowired
    private ProviderRegistrationService registrationService;

    @Autowired
    private PaymentService paymentService;

    @Override
    public String getTopicName() {
        return "createProvider";
    }

    @Override
    protected long getLockDuration() {
        return this.lockDurationMillis;
    }

    @Override
    public void execute(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        try {
            final String           taskId       = externalTask.getId();
            final EnumCustomerType customerType = EnumCustomerType.PROVIDER;
            
            logger.info("Received task. [taskId={}]", taskId);

            final UUID                    userKey         = this.getUserKey(externalTask, externalTaskService);
            final UUID                    registrationKey = this.getRegistrationKey(externalTask, externalTaskService);
            final UserRegistrationCommand command         = UserRegistrationCommand.of(customerType, userKey, registrationKey);

            logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

            this.paymentService.createUser(command);

            this.paymentService.createWallet(command);

            this.paymentService.createBankAccount(command);
           
            this.registrationService.completeRegistration(userKey);
            
            // Initial update with values from MANGOPAY
            this.paymentService.updateCustomerWalletFunds(userKey, customerType);
            this.paymentService.updateUserBlockStatus(userKey, customerType);

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

}
