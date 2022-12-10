package eu.opertusmundi.bpm.worker.service;

import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.model.BpmnWorkerMessageCode;
import eu.opertusmundi.common.model.MessageCode;
import eu.opertusmundi.common.model.workflow.EnumProcessInstanceVariable;

public class BaseWorkerService {

    private static final Logger logger = LoggerFactory.getLogger(BaseWorkerService.class);
    
    @Autowired
    protected ObjectMapper objectMapper;
    
    protected String getVariableAsString(
        ExternalTask externalTask, ExternalTaskService externalTaskService, String name
    ) throws BpmnWorkerException {
        return this.getVariableAsString(externalTask, externalTaskService, name, null);
    }

    protected String getVariableAsString(
        ExternalTask externalTask, ExternalTaskService externalTaskService, String name, String defaultValue
    ) throws BpmnWorkerException {
        final String value = (String) externalTask.getVariable(name);
        if (StringUtils.isBlank(value)) {
            if (defaultValue != null) {
                return defaultValue;
            }

            logger.error("Expected non empty variable value. [name={}]", name);

            throw this.buildVariableNotFoundException(name);
        }

        return value;
    }
    
    protected Integer getVariableAsInteger(
        ExternalTask externalTask, ExternalTaskService externalTaskService, String name
    ) throws BpmnWorkerException {
        final Integer value = (Integer) externalTask.getVariable(name);
        if (value == null) {
            logger.error("Expected non empty variable value. [name={}]", name);

            throw this.buildVariableNotFoundException(name);
        }

        return value;
    }

    protected boolean getVariableAsBoolean(
        ExternalTask externalTask, ExternalTaskService externalTaskService, String name
    ) throws BpmnWorkerException {
        return this.getVariableAsBoolean(externalTask, externalTaskService, name, null);
    }

    protected boolean getVariableAsBoolean(
        ExternalTask externalTask, ExternalTaskService externalTaskService, String name, Boolean defaultValue
    ) throws BpmnWorkerException {
        final Boolean value = (Boolean) externalTask.getVariable(name);
        if (value == null) {
            if (defaultValue != null) {
                return defaultValue;
            }
            logger.error("Expected non empty variable value. [name={}]", name);

            throw this.buildVariableNotFoundException(name);
        }

        return value;
    }

    protected boolean getVariableAsBooleanString(
        ExternalTask externalTask, ExternalTaskService externalTaskService, String name
    ) throws BpmnWorkerException {
        final String published = (String) externalTask.getVariable(name);

        if (StringUtils.isBlank(published)) {
            logger.error("Expected variable to be non empty. [name={}]", name);

            throw this.buildVariableNotFoundException(name);
        }

        return Boolean.parseBoolean(published);
    }

    protected UUID getVariableAsUUID(
        ExternalTask externalTask, ExternalTaskService externalTaskService, String name
    ) throws BpmnWorkerException {
        final String value = this.getVariableAsString(externalTask, externalTaskService, name);

        return UUID.fromString(value);
    }
    
    protected String getErrorDetails(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        final String errorDetails = (String) externalTask.getVariable(EnumProcessInstanceVariable.BPMN_BUSINESS_ERROR_DETAILS.getValue());

        return errorDetails;
    }

    protected String getErrorMessages(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        final String errorMessages = (String) externalTask.getVariable(EnumProcessInstanceVariable.BPMN_BUSINESS_ERROR_MESSAGES.getValue());

        return errorMessages;
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
    
    protected BpmnWorkerException buildVariableNotFoundException(String name) {
        return this.buildException(
            BpmnWorkerMessageCode.VARIABLE_NOT_FOUND,
            "Variable not found",
            String.format("Variable not found. [name=%s]", name)
        );
    }
    
    protected BpmnWorkerException buildInvalidVariableValueException(String name, String value) {
        return this.buildException(
            BpmnWorkerMessageCode.INVALID_VARIABLE_VALUE,
            "Invalid variable value",
            String.format("Invalid variable value.[name=%s, value=%s]", name, value)
        );
    }
    
}
