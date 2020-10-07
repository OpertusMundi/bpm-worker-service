package eu.opertusmundi.bpm.worker.subscriptions;

import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class RegisterAccountEmailingTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(RegisterAccountEmailingTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.registerAccountEmailing.lock-duration:15000}")
    private Long lockDurationMillis;

    @Override
    public String getTopicName() {
        return "register-account-send-activation-link-topic";
    }

    @Override
    protected long getLockDuration() {
        return this.lockDurationMillis;
    }

    @Override
    public void execute(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        // Todo: Put your business logic here

        logger.info("Email send");

        // Complete the task
        externalTaskService.complete(externalTask);
    }

}
