package eu.opertusmundi.bpm.worker.subscriptions;

import java.util.Map;
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

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.service.BaseWorkerService;
import eu.opertusmundi.common.model.BasicMessageCode;
import eu.opertusmundi.common.model.ServiceException;
import eu.opertusmundi.common.model.workflow.EnumProcessInstanceVariable;
import eu.opertusmundi.common.util.BpmInstanceVariablesBuilder;

public abstract class AbstractTaskService extends BaseWorkerService implements ExternalTaskHandler {

    private static final Logger logger = LoggerFactory.getLogger(AbstractTaskService.class);

    private static final String ErrorSeparator = "||";

    protected static final String DEFAULT_ERROR_MESSAGE = "Operation has failed";
    protected static final int    DEFAULT_RETRY_COUNT   = 0;
    protected static final long   DEFAULT_RETRY_TIMEOUT = 2000L;

    @Autowired
    protected ObjectMapper objectMapper;

    @Autowired
    protected ExternalTaskClient externalTaskClient;

    protected TopicSubscription subscription;

    /**
     * Return the amount of time (milliseconds) to lock an extracted task
     */
    protected abstract long getLockDuration(); 
    
    /**
     * Return the topic to subscribe to
     */
    public abstract String getTopicName();

    @PostConstruct
    protected void subscribe() {
        Assert.state(this.subscription == null, "Expected a non initialized subscription");

        final String topic = this.getTopicName();

        this.subscription = this.externalTaskClient.subscribe(topic)
            .lockDuration(this.getLockDuration())
            .handler(this)
            .open();

        logger.info("Add subscription. [topic={}]", topic);
    }

    @PreDestroy
    protected void unsubscribe() {
        final String topic = this.getTopicName();

        if (this.subscription != null) {
            this.subscription.close();
            logger.info("Remove subscription. [topic={}]", topic);
        }
    }

    /**
     * See {@link #handleFailure(ExternalTaskService, ExternalTask, String, Exception)}
     * 
     * @param externalTaskService
     * @param externalTask
     * @param ex
     */
    protected void handleFailure(ExternalTaskService externalTaskService, ExternalTask externalTask, Exception ex) {
        this.handleFailure(externalTaskService, externalTask, DEFAULT_ERROR_MESSAGE, ex);
    }

    /**
     * Reports a failure to execute a task and creates an incident for this task
     *
     * @param externalTaskService
     * @param externalTask
     * @param errorMessage
     * @param ex
     */
    protected void handleFailure(ExternalTaskService externalTaskService, ExternalTask externalTask, String errorMessage, Exception ex) {
        final String errorDetails = this.exceptionToString(ex);

        externalTaskService.handleFailure(
            externalTask, errorMessage, errorDetails, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_TIMEOUT
        );
    }

    /**
     * See {@link #handleBpmnError(ExternalTaskService, ExternalTask, String, String, ServiceException)
     * 
     * @param externalTaskService
     * @param externalTask
     * @param errorCode
     * @param ex
     */
    protected void handleBpmnError(ExternalTaskService externalTaskService, ExternalTask externalTask, String errorCode, ServiceException ex) {
        this.handleBpmnError(externalTaskService, externalTask, errorCode, DEFAULT_ERROR_MESSAGE, ex);
    }

    /**
     * Reports a business error in the context of a running task
     *
     * @param externalTaskService
     * @param externalTask
     * @param errorCode
     * @param errorMessage
     * @param ex
     */
    protected void handleBpmnError(
        ExternalTaskService externalTaskService, ExternalTask externalTask, String errorCode, String errorMessage, ServiceException ex
    ) {
        final String errorDetails   = this.exceptionToString(ex);
        String       messagesAsJson = "";
        try {
            // Set default message
            if (ex.getMessages().isEmpty()) {
                ex.addMessage(BasicMessageCode.InternalServerError, "Service could not process the request");
            }
            messagesAsJson = objectMapper.writeValueAsString(ex.getMessages());
        } catch (Exception sEx) {
            logger.error("Failed to serialize exception messages");
        }

        // Set variables
        final Map<String, Object> variables = BpmInstanceVariablesBuilder.builder()
            .variableAsString(EnumProcessInstanceVariable.BPMN_BUSINESS_ERROR_DETAILS.getValue(), errorDetails)
            .variableAsString(EnumProcessInstanceVariable.BPMN_BUSINESS_ERROR_MESSAGES.getValue(), messagesAsJson)
            .buildValues();

        externalTaskService.handleBpmnError(externalTask, errorCode, errorMessage, variables);
    }

    private String exceptionToString(Exception ex) {
        final var messages = ExceptionUtils.getThrowableList(ex).stream()
            .map(ExceptionUtils::getMessage)
            .distinct()
            .collect(Collectors.toList());
        if (ex instanceof BpmnWorkerException bpmnEx && !StringUtils.isBlank(bpmnEx.getErrorDetails())) {
            messages.add("ErrorDetails: " + bpmnEx.getErrorDetails());
        }
        final String result = StringUtils.join(messages, ErrorSeparator);

        return result;
    }
}
