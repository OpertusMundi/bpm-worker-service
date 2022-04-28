package eu.opertusmundi.bpm.worker.subscriptions.payin;

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
import eu.opertusmundi.common.model.payment.PayInDto;
import eu.opertusmundi.common.service.mangopay.PaymentService;

@Service
public class UpdatePayInStatusTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(UpdatePayInStatusTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.update-payin-status.lock-duration:120000}")
    private Long lockDurationMillis;

    @Autowired
    private PaymentService paymentService;

    @Override
    public String getTopicName() {
        return "updatePayInStatus";
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
            final UUID                payInKey                = UUID.fromString(externalTask.getBusinessKey());
            final String              payInId                 = this.getVariableAsString(externalTask, externalTaskService, "payInId");
            final String              payInStatusVariableName = this.getVariableAsString(externalTask, externalTaskService, "payInStatusVariableName");
            final Map<String, Object> variables               = new HashMap<>();

            logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

            // Update PayIn
            final PayInDto    payIn = paymentService.updatePayIn(payInKey, payInId);
            // Set PayIn updated status in the workflow instance
            variables.put(payInStatusVariableName, payIn.getStatus().toString());

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