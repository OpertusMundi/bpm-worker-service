package eu.opertusmundi.bpm.worker.subscriptions.catalogue;

import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.feign.client.CatalogueFeignClient;
import eu.opertusmundi.common.model.catalogue.client.EnumCatalogueType;

@Service
public class CatalogueHarvestTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(CatalogueHarvestTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.catalogue-harvest.lock-duration:10000}")
    private Long lockDurationMillis;

    @Autowired
    private ObjectProvider<CatalogueFeignClient> catalogueClient;

    @Override
    public String getTopicName() {
        return "harvestCatalogue";
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

            // Get parameters
            final String            catalogueUrl       = this.getVariableAsString(externalTask, externalTaskService, "catalogueUrl");
            final String            catalogueTypeValue = this.getVariableAsString(externalTask, externalTaskService, "catalogueType");
            final EnumCatalogueType catalogueType      = EnumCatalogueType.valueOf(catalogueTypeValue);
            
            logger.debug("Processing task {}: {}", taskId, externalTask);

            // Harvest catalogue
            
            // TODO: Add polling ...
            this.catalogueClient.getObject().harvestCatalogue(catalogueUrl, catalogueType.getValue().toString());

            // Complete task
            externalTaskService.complete(externalTask);

            logger.info("Completed task {}", taskId);
        } catch (final BpmnWorkerException ex) {
            logger.error(String.format("[Catalogue Service] Operation has failed. Error details: %s", ex.getErrorDetails()), ex);
            
            externalTaskService.handleFailure(
                externalTask, ex.getMessage(), ex.getErrorDetails(), ex.getRetries(), ex.getRetryTimeout()
            );
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleError(externalTaskService, externalTask, ex);
        }
    }

}