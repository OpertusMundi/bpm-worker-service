package eu.opertusmundi.bpm.worker.subscriptions;

import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class CreateOrderTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(CreateOrderTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.create-order.lock-duration:15000}")
    private Long lockDurationMillis;

    @Override
    public String getTopicName() {
        return "create-order-topic";
    }

    @Override
    protected long getLockDuration() {
        return this.lockDurationMillis;
    }

    @Override
    public void execute(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        // Todo: Put your business logic here

        // Get process variables
        final String orderId = (String) externalTask.getVariable("order-id");
        final Long   amount  = (Long) externalTask.getVariable("amount");

        logger.info("Creating order {} with amount of {}E", orderId, amount);

        // Complete the task
        externalTaskService.complete(externalTask);
    }
}
