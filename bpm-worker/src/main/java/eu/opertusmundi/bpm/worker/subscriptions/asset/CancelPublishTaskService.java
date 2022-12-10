package eu.opertusmundi.bpm.worker.subscriptions.asset;

import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.model.EnumPublishRequestType;
import eu.opertusmundi.bpm.worker.service.AssetDraftWorkerService;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;

@Service
public class CancelPublishTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(CancelPublishTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.cancel-publish.lock-duration:120000}")
    private Long lockDurationMillis;

    private final AssetDraftWorkerService workerService;

    @Autowired
    public CancelPublishTaskService(AssetDraftWorkerService workerService) {
        this.workerService = workerService;
    }

    @Override
    public String getTopicName() {
        return "cancelPublish";
    }

    @Override
    protected long getLockDuration() {
        return this.lockDurationMillis;
    }

    @Override
    public void execute(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        final String taskId = externalTask.getId();

        logger.info("Received task. [taskId={}]", taskId);

        final String                 requestType = this.getVariableAsString(externalTask, externalTaskService, "requestType");
        final EnumPublishRequestType type        = EnumPublishRequestType.valueOf(requestType);

        logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

        try {
            switch (type) {
                case CATALOGUE_ASSET :
                    this.workerService.cancelPublishAsset(externalTask, externalTaskService);
                    break;
                case USER_SERVICE :
                    this.workerService.cancelPublishUserService(externalTask, externalTaskService);
                    break;
            }

            externalTaskService.complete(externalTask);
            logger.info("Completed task. [taskId={}]", taskId);

        } catch (final BpmnWorkerException ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            externalTaskService.handleFailure(
                externalTask, ex.getMessage(), ex.getErrorDetails(), ex.getRetries(), ex.getRetryTimeout()
            );
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleFailure(externalTaskService, externalTask, ex);
        }
    }

}
