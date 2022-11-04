package eu.opertusmundi.bpm.worker.subscriptions.asset;

import java.util.UUID;

import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.model.geodata.EnumGeodataWorkspace;
import eu.opertusmundi.common.service.IngestService;
import eu.opertusmundi.common.service.ogc.UserGeodataConfigurationResolver;

@Service
public class UnpublishUserServiceTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(UnpublishUserServiceTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.unpublish-user-service.lock-duration:120000}")
    private Long lockDurationMillis;

    @Value("${opertusmundi.geodata.table.prefix:_}")
    private String tablePrefix;

    @Autowired
    private UserGeodataConfigurationResolver userGeodataConfigurationResolver;

    @Autowired
    private IngestService ingestService;

    @Override
    public String getTopicName() {
        return "unpublishUserService";
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

            final UUID   ownerKey          = this.getVariableAsUUID(externalTask, externalTaskService, "ownerKey");
            final UUID   serviceKey        = this.getVariableAsUUID(externalTask, externalTaskService, "serviceKey");
            final var    userGeodataConfig = userGeodataConfigurationResolver.resolveFromUserKey(ownerKey, EnumGeodataWorkspace.PRIVATE);
            final String shard             = userGeodataConfig.getShard();
            final String workspace         = userGeodataConfig.getEffectiveWorkspace();
            final String table             = tablePrefix + serviceKey.toString();

            logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

            // Remove data/layer from PostgreSQL/GeoServer
            this.ingestService.removeDataAndLayer(shard, workspace, table);

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