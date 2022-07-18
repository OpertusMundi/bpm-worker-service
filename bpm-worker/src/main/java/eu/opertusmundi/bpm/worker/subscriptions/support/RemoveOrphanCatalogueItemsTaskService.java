package eu.opertusmundi.bpm.worker.subscriptions.support;

import java.nio.file.Path;
import java.nio.file.Paths;
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

@Service
public class RemoveOrphanCatalogueItemsTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(DeleteAllUserDataTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.remove-orphan-catalogue-items.lock-duration:120000}")
    private Long lockDurationMillis;

    @Autowired
    private Path assetDirectory;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private CatalogueService catalogueService;

    @Autowired
    private IngestService ingestService;

    @Autowired
    private ElasticSearchService elasticSearchService;
    
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
            final String  taskId            = externalTask.getId();

            logger.info("Received task. [taskId={}]", taskId);

            CatalogueAssetQuery    query    = CatalogueAssetQuery.builder().page(0).size(20).build();
            List<CatalogueFeature> features = this.catalogueService.findAllFeatures(query);

            while (!features.isEmpty()) {
                for (final CatalogueFeature f : features) {
                    final String        pid          = f.getId();
                    final UUID          publisherKey = f.getProperties().getPublisherId();
                    final AccountEntity publisher    = this.accountRepository.findOneByKey(publisherKey).orElse(null);
                    if (publisher == null) {
                        logger.info("Removing asset due to missing publisher [pid={}, publisher={}]", pid, publisherKey);

                        this.deleteAssetStatistics(externalTask, externalTaskService, pid);
                        
                        StreamUtils.from(f.getProperties().getIngestionInfo()).forEach(d -> {
                            this.ingestService.removeLayerAndData(d.getTableName(), null, null);
                        });

                        final Path assetPath = Paths.get(this.assetDirectory.toString(), f.getId());
                        final Path draftPath = assetPath.toFile().exists() ? assetPath.toRealPath() : null;
                        if (assetPath.toFile().exists()) {
                            FileUtils.deleteQuietly(assetPath.toFile());
                        }
                        if (draftPath != null && draftPath.toFile().exists()) {
                            FileUtils.deleteQuietly(draftPath.toFile());
                        }

                        this.catalogueService.unpublish(publisherKey, pid);
                    }
                }
                // Fetch next batch
                query    = query.next();
                features = this.catalogueService.findAllFeatures(query);
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
    
    private void deleteAssetStatistics(ExternalTask externalTask, ExternalTaskService externalTaskService, String pid) {
        try {
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            final BoolQueryBuilder    query         = QueryBuilders.boolQuery();
            query.should(QueryBuilders.matchQuery("id.keyword", pid));
            query.minimumShouldMatch(1);
            final String entity = searchBuilder.query(query).toString();

            this.elasticSearchService.performRequest(HttpMethod.POST, "/assets_view/_delete_by_query", entity);
            this.elasticSearchService.performRequest(HttpMethod.POST, "/assets_view_aggregate/_delete_by_query", entity);
        } catch (Exception ex) {
            final String message = String.format("Failed to delete asset statistics [pid=%s]", pid);
            throw new ServiceException(BasicMessageCode.InternalServerError, message, ex);
        } finally {
            // Extend lock duration
            externalTaskService.extendLock(externalTask, this.getLockDuration());
        }
    }
    
}
 