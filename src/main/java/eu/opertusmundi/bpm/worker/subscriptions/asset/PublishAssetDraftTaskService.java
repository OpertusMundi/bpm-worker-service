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

import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.service.AssetDraftException;
import eu.opertusmundi.common.service.ProviderAssetService;

@Service
public class PublishAssetDraftTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(ComputeAutomatedMetadataTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.publish-draft.lock-duration:10000}")
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

            logger.info("Received task {}", taskId);

            // Get draft key
            final String draftKey = (String) externalTask.getVariable("draftKey");
            if (StringUtils.isBlank(draftKey)) {
                logger.error("Expected draft key to be non empty!");

                externalTaskService.handleFailure(externalTask, "Draft key is empty!", null, 0, 0);
                return;
            }

            // Get publisher key
            final String publisherKey = (String) externalTask.getVariable("publisherKey");
            if (StringUtils.isBlank(publisherKey)) {
                logger.error("Expected publisher key to be non empty!");

                externalTaskService.handleFailure(externalTask, "Publisher key is empty!", null, 0, 0);
                return;
            }

            logger.debug("Processing task {}: {}", taskId, externalTask);

            // Update draft
            this.providerAssetService.publishDraft(UUID.fromString(publisherKey), UUID.fromString(draftKey));

            // Complete task
            externalTaskService.complete(externalTask);

            logger.info("Completed task {}", taskId);
        } catch (final AssetDraftException ex) {
            logger.error("[CATALOGUE Service] Operation has failed", ex);

            externalTaskService.handleFailure(externalTask, "[CATALOGUE Service] Operation has failed", null, 0, 0);
        } catch (final Exception ex) {
            logger.error("Unhandled error has occurred", ex);

            final int  retryCount   = 0;
            final long retryTimeout = 2000L;

            externalTaskService.handleFailure(externalTask, "Unhandled error has occurred", ex.getMessage(), retryCount, retryTimeout);

            return;
        }
    }

}