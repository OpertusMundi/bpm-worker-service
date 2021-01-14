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
import eu.opertusmundi.common.service.ConsumerRegistrationService;
import eu.opertusmundi.common.service.PaymentService;

@Service
public class UpdateConsumerTaskService extends AbstractCustomerTaskService {

    private static final Logger logger = LoggerFactory.getLogger(UpdateConsumerTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.consumer-registration.lock-duration:10000}")
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

            logger.info("Received task {}", taskId);

            final UUID userKey         = this.getUserKey(externalTask, externalTaskService);
            final UUID registrationKey = this.getRegistrationKey(externalTask, externalTaskService);

            logger.debug("Processing task {}: {}", taskId, externalTask);

            this.paymentService.updateUser(userKey, registrationKey);

            this.registrationService.completeRegistration(userKey);

            externalTaskService.complete(externalTask);

            logger.info("Completed task {}", taskId);
        } catch (final BpmnWorkerException ex) {
            logger.error("[BPMN Worker] Operation has failed", ex);

            externalTaskService.handleFailure(externalTask, "[BPMN Worker] Operation has failed", null, 0, 0);
        } catch (final Exception ex) {
            logger.error("Unhandled error has occurred", ex);

            final int  retryCount   = 0;
            final long retryTimeout = 2000L;

            externalTaskService.handleFailure(externalTask, "Unhandled error has occurred", ex.getMessage(), retryCount, retryTimeout);

            return;
        }
    }

}
