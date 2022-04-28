package eu.opertusmundi.bpm.worker.subscriptions.user;

import java.util.UUID;

import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import eu.opertusmundi.common.model.ServiceException;
import eu.opertusmundi.common.model.account.EnumCustomerType;
import eu.opertusmundi.common.model.payment.UserRegistrationCommand;
import eu.opertusmundi.common.service.ConsumerRegistrationService;
import eu.opertusmundi.common.service.mangopay.PaymentService;

@Service
public class UpdateConsumerTaskService extends AbstractCustomerTaskService {

    private static final Logger logger = LoggerFactory.getLogger(UpdateConsumerTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.consumer-registration.lock-duration:120000}")
    private Long lockDurationMillis;

    @Autowired
    private ConsumerRegistrationService registrationService;

    @Autowired
    private PaymentService paymentService;

    @Override
    public String getTopicName() {
        return "updateConsumer";
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

            final UUID                    userKey         = this.getUserKey(externalTask, externalTaskService);
            final UUID                    registrationKey = this.getRegistrationKey(externalTask, externalTaskService);
            final UserRegistrationCommand command         = UserRegistrationCommand.of(EnumCustomerType.CONSUMER, userKey, registrationKey);

            logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

            this.paymentService.updateUser(command);

            this.registrationService.completeRegistration(userKey);

            externalTaskService.complete(externalTask);

            logger.info("Completed task. [taskId={}]", taskId);
        } catch (final ServiceException ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleBpmnError(externalTaskService, externalTask, ex);
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleFailure(externalTaskService, externalTask, ex);
        }
    }

}
