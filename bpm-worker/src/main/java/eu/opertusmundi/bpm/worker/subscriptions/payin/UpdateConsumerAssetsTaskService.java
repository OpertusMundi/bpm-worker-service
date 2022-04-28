package eu.opertusmundi.bpm.worker.subscriptions.payin;

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
import eu.opertusmundi.common.service.OrderFulfillmentService;

@Service
public class UpdateConsumerAssetsTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(UpdateConsumerAssetsTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.update-consumer-assets.lock-duration:120000}")
    private Long lockDurationMillis;

    @Autowired
    private OrderFulfillmentService orderFulfillmentService;

    @Override
    public String getTopicName() {
        return "updateConsumerAssets";
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
            final UUID payInKey = UUID.fromString(externalTask.getBusinessKey());

            logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

            this.orderFulfillmentService.registerConsumerAssets(payInKey);

            // Complete task
            externalTaskService.complete(externalTask);

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