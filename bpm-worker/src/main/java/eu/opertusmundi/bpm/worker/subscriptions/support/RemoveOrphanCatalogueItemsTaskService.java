package eu.opertusmundi.bpm.worker.subscriptions.support;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Service;

import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.domain.AccountEntity;
import eu.opertusmundi.common.model.BasicMessageCode;
import eu.opertusmundi.common.model.ServiceException;
import eu.opertusmundi.common.model.catalogue.client.CatalogueAssetQuery;
import eu.opertusmundi.common.model.catalogue.server.CatalogueFeature;
import eu.opertusmundi.common.repository.AccountRepository;
import eu.opertusmundi.common.service.CatalogueService;
import eu.opertusmundi.common.service.ElasticSearchService;
import eu.opertusmundi.common.service.IngestService;
import eu.opertusmundi.common.util.StreamUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;

@Service
public class RemoveOrphanCatalogueItemsTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(DeleteAllUserDataTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.remove-orphan-catalogue-items.lock-duration:120000}")
    private Long lockDurationMillis;

    @Value("${opertusmundi.bpm.worker.tasks.remove-orphan-catalogue-items.batch-size:20}")
    private int batchSize;

    private final AccountRepository    accountRepository;
    private final CatalogueService     catalogueService;
    private final ElasticSearchService elasticSearchService;
    private final IngestService        ingestService;
    private final Path                 assetDirectory;

    @Autowired
    public RemoveOrphanCatalogueItemsTaskService(
        AccountRepository accountRepository, 
        CatalogueService catalogueService,
        ElasticSearchService elasticSearchService, 
        IngestService ingestService, 
        Path assetDirectory
    ) {
        this.accountRepository    = accountRepository;
        this.catalogueService     = catalogueService;
        this.elasticSearchService = elasticSearchService;
        this.ingestService        = ingestService;
        this.assetDirectory       = assetDirectory;
    }

    @Override
    protected long getLockDuration() {
        return this.lockDurationMillis;
    }

    @Override
    public String getTopicName() {
        return "removeOrphanCatalogueItems";
    }

    @Override
    public final void execute(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        try {
            final String taskId = externalTask.getId();
            logger.info("Received task. [taskId={}]", taskId);

            // Collect assets to delete
            CatalogueAssetQuery     query    = CatalogueAssetQuery.builder().page(0).size(batchSize).build();
            List<CatalogueFeature>  features = this.catalogueService.findAllFeatures(query);
            List<FeatureDeleteTask> tasks    = new ArrayList<>();

            while (!features.isEmpty()) {
                for (final CatalogueFeature feature : features) {
                    final String        pid          = feature.getId();
                    final UUID          publisherKey = feature.getProperties().getPublisherId();
                    final AccountEntity publisher    = this.accountRepository.findOneByKey(publisherKey).orElse(null);
                    if (publisher == null) {
                        logger.info("Removing asset due to missing publisher [pid={}, publisher={}]", pid, publisherKey);
                        
                        tasks.add(FeatureDeleteTask.of(feature, pid, publisherKey));
                    }
                }
                // Fetch next batch
                query    = query.next();
                features = this.catalogueService.findAllFeatures(query);
                // Extend lock duration
                externalTaskService.extendLock(externalTask, this.getLockDuration());
            }
            
            // Delete assets
            for (final FeatureDeleteTask task : tasks) {
                this.deleteAssetStatistics(externalTask, externalTaskService, task);
                this.deleteAsset(externalTask, externalTaskService, task);
                
                // Extend lock duration
                externalTaskService.extendLock(externalTask, this.getLockDuration());
            }

            externalTaskService.complete(externalTask);
            logger.info("Completed task. [taskId={}]", taskId);
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleFailure(externalTaskService, externalTask, ex);
        }
    }
    
    private void deleteAsset(ExternalTask externalTask, ExternalTaskService externalTaskService, FeatureDeleteTask task) {
        StreamUtils.from(task.feature.getProperties().getIngestionInfo()).forEach(d -> {
            this.ingestService.removeLayerAndData(d.getTableName(), null, null);
        });

        try {
            final Path assetPath = Paths.get(this.assetDirectory.toString(), task.feature.getId());
            final Path draftPath = assetPath.toFile().exists() ? assetPath.toRealPath() : null;
            if (assetPath.toFile().exists()) {
                FileUtils.deleteQuietly(assetPath.toFile());
            }
            if (draftPath != null && draftPath.toFile().exists()) {
                FileUtils.deleteQuietly(draftPath.toFile());
            }
        } catch (IOException ex) {
            logger.info(String.format("Failed to delete asset resources [pid=%s]", task.pid), ex);
        }
        
        this.catalogueService.unpublish(task.publisherKey, task.pid);
    }
    
    private void deleteAssetStatistics(ExternalTask externalTask, ExternalTaskService externalTaskService, FeatureDeleteTask task) {
        try {
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            final BoolQueryBuilder    query         = QueryBuilders.boolQuery();
            query.should(QueryBuilders.matchQuery("id.keyword", task.pid));
            query.minimumShouldMatch(1);
            final String entity = searchBuilder.query(query).toString();

            this.elasticSearchService.performRequest(HttpMethod.POST, "/assets_view/_delete_by_query", entity);
            this.elasticSearchService.performRequest(HttpMethod.POST, "/assets_view_aggregate/_delete_by_query", entity);
        } catch (Exception ex) {
            final String message = String.format("Failed to delete asset statistics [pid=%s]", task.pid);
            throw new ServiceException(BasicMessageCode.InternalServerError, message, ex);
        }
    }

    @AllArgsConstructor(staticName = "of")
    @Builder
    private static class FeatureDeleteTask {

        private CatalogueFeature feature;
        private String           pid;
        private UUID             publisherKey;

    }
    
}
 