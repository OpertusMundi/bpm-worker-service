package eu.opertusmundi.bpm.worker.subscriptions.payout;

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
import eu.opertusmundi.common.model.ServiceException;
import eu.opertusmundi.common.model.payment.PayOutDto;
import eu.opertusmundi.common.service.PaymentService;

@Service
public class CreatePayOutTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(CreatePayOutTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.create-payout.lock-duration:10000}")
    private Long lockDurationMillis;

    @Autowired
    private PaymentService paymentService;
    
    @Override
    public String getTopicName() {
        return "createPayOut";
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
            final UUID                payOutKey = UUID.fromString(externalTask.getBusinessKey());
            final Map<String, Object> variables = new HashMap<>();

            logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

            // Update PayOut
            final PayOutDto payOut = paymentService.createPayOutAtProvider(payOutKey);
            // Set PayOut status and id
            variables.put("payOutId", payOut.getProviderPayOut());
            variables.put("payOutStatus", payOut.getStatus().toString());
            
            // Complete task
            externalTaskService.complete(externalTask, variables);

            logger.info("Completed task. [taskId={}]", taskId);
        } catch (final BpmnWorkerException ex) {
            logger.error(String.format("Operation has failed. [details=%s]", ex.getErrorDetails()), ex);
            
            externalTaskService.handleFailure(
                externalTask, ex.getMessage(), ex.getErrorDetails(), ex.getRetries(), ex.getRetryTimeout()
            );
        } catch (final ServiceException ex) {
            logger.error(String.format("Operation has failed. [details=%s]", ex.getMessage()), ex);
            
            externalTaskService.handleFailure(
                externalTask, ex.getMessage(), ex.getMessage(), DEFAULT_RETRY_COUNT, DEFAULT_RETRY_TIMEOUT
            );
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleError(externalTaskService, externalTask, ex);
        }
    }
    
}