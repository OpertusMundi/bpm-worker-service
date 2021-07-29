package eu.opertusmundi.bpm.worker.subscriptions;

import java.util.UUID;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.camunda.bpm.client.ExternalTaskClient;
import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskHandler;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.camunda.bpm.client.topic.TopicSubscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.model.BpmnWorkerMessageCode;
import eu.opertusmundi.common.model.MessageCode;

public abstract class AbstractTaskService implements ExternalTaskHandler {

    private static final Logger logger = LoggerFactory.getLogger(AbstractTaskService.class);

    private static final String ErrorSeparator = "||";

    protected static final String DEFAULT_ERROR_MESSAGE = "Operation has failed";
    protected static final int    DEFAULT_RETRY_COUNT   = 0;
    protected static final long   DEFAULT_RETRY_TIMEOUT = 2000L;

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

        logger.info("Created subscription. [topic={}]", topic);
    }

    @PreDestroy
    protected void unsubscribe() {
        final String topic = this.getTopicName();

        if (this.subscription != null) {
            this.subscription.close();
            logger.info("Removing subscription. [topic={}]", topic);
        }
    }

    protected BpmnWorkerException buildVariableNotFoundException(String name) {
        return this.buildException(
            BpmnWorkerMessageCode.VARIABLE_NOT_FOUND,
            "Variable not found",
            String.format("Variable is empty. [name=%s]", name)
        );
    }

    protected BpmnWorkerException buildInvalidVariableValueException(String name, String value) {
        return this.buildException(
            BpmnWorkerMessageCode.INVALID_VARIABLE_VALUE,
            "Invalid variable value",
            String.format("Invalid variable value.[name=%s, value=%s]", name, value)
        );
    }

    protected BpmnWorkerException buildException(
        MessageCode code, String message, String errorDetails
    ) {
        return  BpmnWorkerException.builder()
        .code(code)
        .message(message)
        .errorDetails(errorDetails)
        .retries(0)
        .retryTimeout(0L)
        .build();
    }

    protected void handleError(ExternalTaskService externalTaskService, ExternalTask externalTask, Exception ex) {
        final String details = StringUtils.join(
            ExceptionUtils.getThrowableList(ex).stream()
                .map(ExceptionUtils::getMessage)
                .distinct()
                .collect(Collectors.toList()),
            ErrorSeparator
        );
        
        externalTaskService.handleFailure(
            externalTask, DEFAULT_ERROR_MESSAGE, details, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_TIMEOUT
        );
    }

    protected String getVariableAsString(
        ExternalTask externalTask, ExternalTaskService externalTaskService, String name
    ) throws BpmnWorkerException {
        final String value = (String) externalTask.getVariable(name);
        if (StringUtils.isBlank(value)) {
            logger.error("Expected non empty variable value. [name={}]", name);

            throw this.buildVariableNotFoundException(name);
        }

        return value;
    }

    protected UUID getVariableAsUUID(
        ExternalTaskService externalTaskService, ExternalTask externalTask, String name
    ) throws BpmnWorkerException {
        final String value = this.getVariableAsString(externalTask, externalTaskService, name);

        return UUID.fromString(value);
    }

}
