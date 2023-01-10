package eu.opertusmundi.bpm.worker.subscriptions.refund;

import java.util.HashMap;
import java.util.Map;

import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.model.payment.EnumTransactionType;
import eu.opertusmundi.common.repository.RefundRepository;

@Service
public class RefundConsumerAssetsTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(RefundConsumerAssetsTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.refund-consumer-assets.lock-duration:20000}")
    private Long lockDurationMillis;

    @Autowired
    private RefundRepository refundRepository;

    @Override
    public String getTopicName() {
        return "refundConsumerAssets";
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

            final String              refundId  = this.getVariableAsString(externalTask, externalTaskService, "refundId");
            final Map<String, Object> variables = new HashMap<>();

            logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

            final var refund = refundRepository.findOneByTransactionId(refundId).orElse(null);
            // Asset registration is updated only for consumer PayIns
            if (refund.getInitialTransactionType() == EnumTransactionType.PAYIN) {
                // TODO: Remove registered assets
            }

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