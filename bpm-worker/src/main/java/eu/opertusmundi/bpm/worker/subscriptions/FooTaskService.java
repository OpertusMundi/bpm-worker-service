package eu.opertusmundi.bpm.worker.subscriptions;

import java.util.Collections;
import java.util.Map;

import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * A service for subscribing and processing `foo` tasks
 */
@Service
public class FooTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(FooTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.foo.lock-duration:15000}")
    private Long lockDurationMillis;

    @Override
    public String getTopicName() {
        return "foo";
    }

    @Override
    protected long getLockDuration() {
        return this.lockDurationMillis;
    }

    @Override
    public void execute(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        final String taskId = externalTask.getId();
        logger.info("Received task {}", taskId);

        // Report failure if something is wrong

        final String name1 = (String) externalTask.getVariable("name1");
        if (name1 == null || name1.isEmpty()) {
            logger.error("Expected name1 to be non empty!");
            final int  retryCount   = 0;
            final long retryTimeout = 2000L;
            externalTaskService.handleFailure(externalTask, "name1 is empty!", null, retryCount, retryTimeout);
            return;
        }

        // Simulate some processing

        logger.debug("Processing task {}: {}", taskId, externalTask);

        try {
            Thread.sleep(2000L);
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }

        // Complete successfully

        final Map<String, Object> outputVariables = Collections.singletonMap("score", 0.94051);
        externalTaskService.complete(externalTask, outputVariables);

        logger.info("Completed task {}", taskId);
    }

}