package eu.opertusmundi.bpm.worker.subscriptions.asset;

import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.service.CatalogueService;

@Service
public class UnpublishAssetTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(UnpublishAssetTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.unpublish-asset.lock-duration:120000}")
    private Long lockDurationMillis;

    @Autowired
    private CatalogueService catalogueService;

    @Override
    public String getTopicName() {
        return "unpublishAsset";
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

            // Get asset PID
            final String id = (String) externalTask.getVariable("assetId");
            if (StringUtils.isBlank(id)) {
                logger.error("Expected asset PID to be non empty");

                externalTaskService.handleFailure(externalTask, "Asset PID is empty!", null, 0, 0);
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

            // Remove asset
            this.catalogueService.unpublish(UUID.fromString(publisherKey), id);

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