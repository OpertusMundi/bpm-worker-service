package eu.opertusmundi.bpm.worker.subscriptions.asset;

import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import eu.opertusmundi.bpm.worker.model.ErrorCodes;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.model.ServiceException;
import eu.opertusmundi.common.model.asset.EnumProviderAssetDraftStatus;
import eu.opertusmundi.common.service.ProviderAssetService;
import eu.opertusmundi.common.util.BpmInstanceVariablesBuilder;

@Service
public class PublishTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(PublishTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.publish-draft.lock-duration:120000}")
    private Long lockDurationMillis;

    @Autowired
    private ProviderAssetService providerAssetService;

    @Override
    public String getTopicName() {
        return "publishDraft";
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

            // Get draft key
            final String draftKey = (String) externalTask.getVariable("draftKey");
            if (StringUtils.isBlank(draftKey)) {
                logger.error("Expected draft key to be non empty");

                externalTaskService.handleFailure(externalTask, "Draft key is empty!", null, 0, 0);
                return;
            }

            // Get publisher key
            final String publisherKey = (String) externalTask.getVariable("publisherKey");
            if (StringUtils.isBlank(publisherKey)) {
                logger.error("Expected publisher key to be non empty");

                externalTaskService.handleFailure(externalTask, "Publisher key is empty", null, 0, 0);
                return;
            }

            logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

            // Update draft
            this.providerAssetService.publishDraft(UUID.fromString(publisherKey), UUID.fromString(publisherKey), UUID.fromString(draftKey));

            // Complete task
            final Map<String, Object> variables = BpmInstanceVariablesBuilder.builder()
                .variableAsString("status", EnumProviderAssetDraftStatus.PUBLISHED.toString())
                .buildValues();

            externalTaskService.complete(externalTask, variables);

            logger.info("Completed task. [taskId={}]", taskId);
        } catch (final ServiceException ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);
            if (ExceptionUtils.indexOfType(ex, feign.RetryableException.class) != -1) {
                // For feign client retryable exceptions, create a new incident
                // instead of canceling the process instance. Errors such as
                // network connectivity, unavailable services etc may be
                // automatically resolved after retrying the failed task
                //
                // See:
                // https://javadoc.io/doc/io.github.openfeign/feign-core/latest/feign/RetryableException.html
                //
                // "This exception is raised when the Response is deemed to be
                // retryable, typically via an ErrorDecoder when the status is
                // 503."
                this.handleFailure(externalTaskService, externalTask, ex);
            } else {
                this.handleBpmnError(externalTaskService, externalTask, ErrorCodes.PublishAsset, ex);
            }
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleFailure(externalTaskService, externalTask, ex);
        }
    }

}