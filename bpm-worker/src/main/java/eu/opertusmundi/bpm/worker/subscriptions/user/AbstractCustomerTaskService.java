package eu.opertusmundi.bpm.worker.subscriptions.user;

import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.model.profiler.DataProfilerServiceException;

public abstract class AbstractCustomerTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(AbstractCustomerTaskService.class);

    protected static String VARIABLE_USER_KEY = "userKey";
    protected static String VARIABLE_REGISTRATION_KEY = "registrationKey";

    protected UUID getUserKey(ExternalTask externalTask, ExternalTaskService externalTaskService) throws DataProfilerServiceException {
        final String userKey = (String) externalTask.getVariable(VARIABLE_USER_KEY);
        if (StringUtils.isBlank(userKey)) {
            logger.error("Expected user key to be non empty!");

            throw this.buildVariableNotFoundException(VARIABLE_USER_KEY);
        }

        return UUID.fromString(userKey);
    }

    protected UUID getRegistrationKey(ExternalTask externalTask, ExternalTaskService externalTaskService) throws DataProfilerServiceException {
        final String registrationKey = (String) externalTask.getVariable(VARIABLE_REGISTRATION_KEY);
        if (StringUtils.isBlank(registrationKey)) {
            logger.error("Expected registration key to be non empty!");

            throw this.buildVariableNotFoundException(VARIABLE_REGISTRATION_KEY);
        }

        return UUID.fromString(registrationKey);
    }

}
