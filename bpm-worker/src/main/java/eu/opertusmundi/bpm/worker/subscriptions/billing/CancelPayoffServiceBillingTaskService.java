package eu.opertusmundi.bpm.worker.subscriptions.billing;

import java.util.HashMap;
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
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.service.ServiceBillingService;

@Service
public class CancelPayoffServiceBillingTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(CancelPayoffServiceBillingTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.cancel-payoff-subscription-billing.lock-duration:120000}")
    private Long lockDurationMillis;

    @Autowired
    private ServiceBillingService serviceBillingService;

    @Override
    public String getTopicName() {
        return "cancelPayoffServiceBillingRecords";
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

            final UUID                payInKey  = UUID.fromString(externalTask.getBusinessKey());
            final Map<String, Object> variables = new HashMap<>();

            logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);
            this.serviceBillingService.cancelPayoff(payInKey);

            // Complete task
            externalTaskService.complete(externalTask, variables);
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