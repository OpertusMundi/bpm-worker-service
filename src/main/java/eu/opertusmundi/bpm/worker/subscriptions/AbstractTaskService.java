package eu.opertusmundi.bpm.worker.subscriptions;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.camunda.bpm.client.ExternalTaskClient;
import org.camunda.bpm.client.task.ExternalTaskHandler;
import org.camunda.bpm.client.topic.TopicSubscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

public abstract class AbstractTaskService implements ExternalTaskHandler {

    private static final Logger logger = LoggerFactory.getLogger(AbstractTaskService.class);

    @Autowired
    protected ExternalTaskClient externalTaskClient;

    protected TopicSubscription subscription;

    /**
     * Return the topic to subscribe to
     */
    public abstract String getTopicName();

    /**
     * Return the amount of time (milliseconds) to lock an extracted task
     */
    protected long getLockDuration() { return 10000L; }

    @PostConstruct
    protected void subscribe() {
        Assert.state(this.subscription == null, "Expected a non initialized subscription");

        final String topic = this.getTopicName();

        this.subscription = this.externalTaskClient.subscribe(topic)
            .lockDuration(this.getLockDuration())
            .handler(this)
            .open();

        logger.info("Created subscripion for topic: {}", topic);
    }

    @PreDestroy
    protected void unsubscribe() {
        final String topic = this.getTopicName();

        if (this.subscription != null) {
            logger.info("Removing subscripion for topic {}", topic);
            this.subscription.close();
        }
    }

}
