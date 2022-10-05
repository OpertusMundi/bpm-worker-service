package eu.opertusmundi.bpm.worker.subscriptions.support;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.sql.DataSource;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.camunda.bpm.engine.rest.dto.runtime.VariableInstanceDto;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.config.GeodataConfiguration;
import eu.opertusmundi.common.domain.ProviderAssetDraftEntity;
import eu.opertusmundi.common.model.BasicMessageCode;
import eu.opertusmundi.common.model.EnumRole;
import eu.opertusmundi.common.model.PageResultDto;
import eu.opertusmundi.common.model.ServiceException;
import eu.opertusmundi.common.model.account.AccountClientDto;
import eu.opertusmundi.common.model.account.AccountDto;
import eu.opertusmundi.common.model.asset.service.UserServiceDto;
import eu.opertusmundi.common.model.catalogue.CatalogueResult;
import eu.opertusmundi.common.model.catalogue.client.CatalogueAssetQuery;
import eu.opertusmundi.common.model.catalogue.client.CatalogueItemDetailsDto;
import eu.opertusmundi.common.model.catalogue.client.CatalogueItemDto;
import eu.opertusmundi.common.model.keycloak.server.UserDto;
import eu.opertusmundi.common.model.keycloak.server.UserQueryDto;
import eu.opertusmundi.common.model.workflow.EnumProcessInstanceVariable;
import eu.opertusmundi.common.repository.AccountRepository;
import eu.opertusmundi.common.repository.DraftRepository;
import eu.opertusmundi.common.service.AccountClientService;
import eu.opertusmundi.common.service.CatalogueService;
import eu.opertusmundi.common.service.ElasticSearchService;
import eu.opertusmundi.common.service.IngestService;
import eu.opertusmundi.common.service.KeycloakAdminService;
import eu.opertusmundi.common.service.UserServiceService;
import eu.opertusmundi.common.util.BpmEngineUtils;
import eu.opertusmundi.common.util.StreamUtils;
import feign.FeignException.FeignClientException;
import lombok.AllArgsConstructor;
import lombok.Builder;

@Service
public class DeleteAllUserDataTaskService extends AbstractTaskService implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(DeleteAllUserDataTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.delete-all-user-data.lock-duration:120000}")
    private Long lockDurationMillis;

    @Autowired
    private ResourceLoader resourceLoader;

    @Autowired
    private DataSource dataSource;

    private JdbcTemplate jdbcTemplate;

    @Autowired
    private GeodataConfiguration geodataConfiguration;

    @Autowired
    private Path assetDirectory;

    @Autowired
    private Path contractDirectory;

    @Autowired
    private Path draftDirectory;

    @Autowired
    private Path invoiceDirectory;

    @Autowired
    private Path orderDirectory;

    @Autowired
    private Path userDirectory;

    @Autowired
    private Path userServiceDirectory;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private DraftRepository draftRepository;

    @Autowired
    private CatalogueService catalogueService;

    @Autowired
    private UserServiceService userServiceService;

    @Autowired
    private IngestService ingestService;

    @Autowired
    private KeycloakAdminService keycloakAdminService;

    @Autowired
    private AccountClientService accountClientService;

    @Autowired
    private ElasticSearchService elasticSearchService;

    @Autowired
    private BpmEngineUtils bpmEngineUtils;

    @Override
    public void afterPropertiesSet() throws Exception {
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    protected long getLockDuration() {
        return this.lockDurationMillis;
    }

    @Override
    public String getTopicName() {
        return "deleteAllUserData";
    }

    @Override
    public final void execute(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        try {
            final String  taskId            = externalTask.getId();
            final Integer userId            = this.getVariableAsInteger(externalTask, externalTaskService, "userId");
            final UUID    userKey           = this.getVariableAsUUID(externalTask, externalTaskService, "userKey");
            final UUID    userParentKey     = this.getVariableAsUUID(externalTask, externalTaskService, "userParentKey");
            final String  userName          = this.getVariableAsString(externalTask, externalTaskService, "userName");
            final boolean fileSystemDeleted = this.getVariableAsBoolean(externalTask, externalTaskService, "fileSystemDeleted");
            final boolean contractsDeleted  = this.getVariableAsBoolean(externalTask, externalTaskService, "contractsDeleted");
            final boolean accountDeleted    = this.getVariableAsBoolean(externalTask, externalTaskService, "accountDeleted");

            logger.info("Received task. [taskId={}]", taskId);

            // Get user
            final AccountDto account = this.accountRepository.findOneByKeyObject(userKey).orElse(null);
            if (account != null && !account.hasRole(EnumRole.ROLE_TESTER)) {
                logger.warn("Account does not have role ROLE_TESTER [userKey={}]", userKey);
                return;
            }
            final String userGeodataShard = account.getProfile().getGeodataShard();

            // Build context
            final OperationContext ctx = OperationContext.builder()
                .userId(userId)
                .userKey(userKey)
                .userParentKey(userParentKey)
                .userName(userName)
                .userGeodataShard(userGeodataShard)
                .accountDeleted(accountDeleted)
                .contractsDeleted(contractsDeleted)
                .fileSystemDeleted(fileSystemDeleted)
                .build();

            this.deleteAllUserData(externalTask, externalTaskService, ctx);

            externalTaskService.complete(externalTask);

            logger.info("Completed task. [taskId={}]", taskId);
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleFailure(externalTaskService, externalTask, ex);
        }
    }

    /**
     * Deletes all user data for the given user key.
     *
     * <p>
     * This method will delete the following resources:
     *
     * <ul>
     * <li>Workflow process instances related to the specified user, including
     * both running process instances and historic data
     *
     * <li>User services published by the user
     *
     * <li>Assets that this user has published
     *
     * <li>Asset statistics stored in Elasticsearch
     *
     * <li>All files in the user's file system if either
     * {@link OperationContext#accountDeleted} or
     * {@link OperationContext#fileSystemDeleted} is set to {@code true}
     *
     * <li>OAuth clients created by the user
     *
     * <li>All database records that refer to the specified user. If
     * {@link OperationContext#accountDeleted} is {@code true}, the user is also
     * deleted
     *
     * <li>The corresponding user in the IDP if
     * {@link OperationContext#accountDeleted} is set to {@code true}
     * </ul>
     *
     * @param externalTask
     * @param externalTaskService
     * @param ctx
     * @throws ServiceException
     */
    @Transactional
    public void deleteAllUserData(ExternalTask externalTask, ExternalTaskService externalTaskService, OperationContext ctx) throws ServiceException {
        this.deleteWorkflowInstances(externalTask, externalTaskService, ctx);

        this.deleteUserServices(externalTask, externalTaskService, ctx);

        // Published assets must be deleted before the database records. This
        // method updates the context with additional information about the
        // published assets PIDs. This information is required for deleting
        // database records that only refer an asset PID
        this.deleteAssets(externalTask, externalTaskService, ctx);

        // Collect all asset PIDs from the draft table. This is required because
        // the task may have failed after deleteAssets was executed and we
        // cannot retrieve the published asset PIDs
        this.collectAssetPids(externalTask, externalTaskService, ctx);

        this.deleteAssetStatistics(externalTask, externalTaskService, ctx);

        if (ctx.fileSystemDeleted || ctx.accountDeleted) {
            this.deleteAllFiles(externalTask, externalTaskService, ctx);
        }

        this.deleteOAuthClients(externalTask, externalTaskService, ctx);

        if (ctx.accountDeleted) {
            this.deleteIdpUser(externalTask, externalTaskService, ctx);
            this.deleteUserProfile(externalTask, externalTaskService, ctx);
        }

        this.deleteDatabaseRecords(externalTask, externalTaskService, ctx);
    }

    private void deleteWorkflowInstances(ExternalTask externalTask, ExternalTaskService externalTaskService, OperationContext ctx) {
        try {
            final List<String>              processInstances = new ArrayList<>();
            final List<VariableInstanceDto> variables        = this.bpmEngineUtils.getVariables(
                EnumProcessInstanceVariable.START_USER_KEY.getValue(), ctx.userKey.toString()
            );
            for (final VariableInstanceDto v : variables) {
                if (!processInstances.contains(v.getProcessInstanceId())) {
                    processInstances.add(v.getProcessInstanceId());
                }
            }

            final List<String>              processHistoryInstances = new ArrayList<>();
            final List<VariableInstanceDto> historyVariables        = this.bpmEngineUtils.getHistoryVariables(
                EnumProcessInstanceVariable.START_USER_KEY.getValue(), ctx.userKey.toString()
            );
            for (final VariableInstanceDto v : historyVariables) {
                if (!processHistoryInstances.contains(v.getProcessInstanceId())) {
                    processHistoryInstances.add(v.getProcessInstanceId());
                }
            }

            for (final String id : processInstances) {
                // Attempt to delete process instance
                boolean checkHistory = !this.deleteProcessInstance(id, false);
                // If delete operation fails, try to delete a historic process
                // instance
                if (checkHistory) {
                    this.deleteProcessInstance(id, true);
                }
            }
            for (final String id : processHistoryInstances) {
                this.deleteProcessInstance(id, true);
            }
        } catch (Exception ex) {
            final String message = String.format("Failed to delete workflow instances [userKey=%s]", ctx.userKey);
            throw new ServiceException(BasicMessageCode.InternalServerError, message, ex);
        } finally {
            // Extend lock duration
            externalTaskService.extendLock(externalTask, this.getLockDuration());
        }
    }

    private boolean deleteProcessInstance(String id, boolean historic) {
        try {
            if (historic) {
                this.bpmEngineUtils.deleteHistoryProcessInstance(id);
            } else {
                this.bpmEngineUtils.deleteProcessInstance(id);
            }
        } catch (FeignClientException ex) {
            if (ex.status() != HttpStatus.NOT_FOUND.value()) {
                throw ex;
            }
        }
        return true;
    }

    private void deleteUserServices(ExternalTask externalTask, ExternalTaskService externalTaskService, OperationContext ctx) {
        try {
            int index = 0;
            PageResultDto<UserServiceDto> services = this.userServiceService.findAll(
                ctx.userKey, ctx.userParentKey, null, null, index, 10
            );

            while (!services.getItems().isEmpty()) {
                for (final UserServiceDto service : services.getItems()) {
                    this.ingestService.removeDataAndLayer(ctx.userGeodataShard, ctx.userKey.toString(), service.getKey().toString());
                }
                index++;
                services = this.userServiceService.findAll(ctx.userKey, ctx.userParentKey, null, null, index, 10);

                // Extend lock duration
                externalTaskService.extendLock(externalTask, this.getLockDuration());
            }
        } catch (Exception ex) {
            final String message = String.format("Failed to delete user services [userKey=%s]", ctx.userKey);
            throw new ServiceException(BasicMessageCode.InternalServerError, message, ex);
        } finally {
            // Extend lock duration
            externalTaskService.extendLock(externalTask, this.getLockDuration());
        }
    }

    private void deleteAssets(ExternalTask externalTask, ExternalTaskService externalTaskService, OperationContext ctx) {
        try {
            CatalogueAssetQuery query = CatalogueAssetQuery.builder()
                .publisherKey(ctx.userKey.toString())
                .page(0)
                .size(20)
                .build();

            CatalogueResult<CatalogueItemDto> result = this.catalogueService.findAll(null, query);

            while (!result.getResult().getItems().isEmpty()) {
                for(final CatalogueItemDto item: result.getResult().getItems()) {
                    ctx.pid.add(item.getId());
                    final CatalogueItemDetailsDto itemDetails = this.catalogueService.unpublish(ctx.userKey, item.getId());

                    // Delete ingested data
                    StreamUtils.from(itemDetails.getIngestionInfo()).forEach(d -> {
                        this.ingestService.removeDataAndLayer(ctx.userGeodataShard, ctx.userParentKey.toString(), d.getTableName());
                    });
                }

                // Fetch next batch
                query = CatalogueAssetQuery.builder()
                    .publisherKey(ctx.userKey.toString())
                    .page(0)
                    .size(20)
                    .build();

                result = this.catalogueService.findAll(null, query);

                // Extend lock duration
                externalTaskService.extendLock(externalTask, this.getLockDuration());
            }
        } catch (Exception ex) {
            final String message = String.format("Failed to delete published assets [userKey=%s]", ctx.userKey);
            throw new ServiceException(BasicMessageCode.InternalServerError, message, ex);
        } finally {
            // Extend lock duration
            externalTaskService.extendLock(externalTask, this.getLockDuration());
        }
    }

    private void collectAssetPids(ExternalTask externalTask, ExternalTaskService externalTaskService, OperationContext ctx) {
        try {
            final Direction direction   = Direction.ASC;
            PageRequest     pageRequest = PageRequest.of(0, 10, Sort.by(direction, "id"));

            Page<ProviderAssetDraftEntity> drafts = this.draftRepository.findAllByPublisher(ctx.userKey, pageRequest);

            while (!drafts.isEmpty()) {
                for (final ProviderAssetDraftEntity d : drafts.getContent()) {
                    if (!StringUtils.isBlank(d.getAssetPublished()) && !ctx.pid.contains(d.getAssetPublished())) {
                        ctx.pid.add(d.getAssetPublished());
                    }
                }
                // Fetch next batch
                pageRequest = pageRequest.next();
                drafts      = this.draftRepository.findAllByPublisher(ctx.userKey, pageRequest);
                // Extend lock duration
                externalTaskService.extendLock(externalTask, this.getLockDuration());
            }
        } catch (Exception ex) {
            final String message = String.format("Failed to delete IDP user [userKey=%s]", ctx.userKey);
            throw new ServiceException(BasicMessageCode.InternalServerError, message, ex);
        } finally {
            // Extend lock duration
            externalTaskService.extendLock(externalTask, this.getLockDuration());
        }
    }

    private void deleteAssetStatistics(ExternalTask externalTask, ExternalTaskService externalTaskService, OperationContext ctx) {
        try {
            if (ctx.pid.isEmpty()) {
                return;
            }
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            final BoolQueryBuilder    query         = QueryBuilders.boolQuery();
            for (final String pid : ctx.pid) {
                query.should(QueryBuilders.matchQuery("id.keyword", pid));
            }
            query.minimumShouldMatch(1);
            final String entity = searchBuilder.query(query).toString();

            this.elasticSearchService.performRequest(HttpMethod.POST, "/assets_view/_delete_by_query", entity);
            this.elasticSearchService.performRequest(HttpMethod.POST, "/assets_view_aggregate/_delete_by_query", entity);
        } catch (Exception ex) {
            final String message = String.format("Failed to delete asset statistics [userKey=%s, pid=%s]", ctx.userKey, ctx.pid);
            throw new ServiceException(BasicMessageCode.InternalServerError, message, ex);
        } finally {
            // Extend lock duration
            externalTaskService.extendLock(externalTask, this.getLockDuration());
        }
    }

    private void deleteAllFiles(
        ExternalTask externalTask, ExternalTaskService externalTaskService, OperationContext ctx
    ) {
        try {
            // Asset symbolic links to draft data
            for (final String p : ctx.pid) {
                final Path assetPath = Paths.get(this.assetDirectory.toString(), p);
                FileUtils.deleteQuietly(assetPath.toFile());
            }

            // Delete all contracts specific to the user
            final Path contractPath = Paths.get(this.contractDirectory.toString(), ctx.userId.toString());
            FileUtils.deleteQuietly(contractPath.toFile());

            // Delete user data
            final Path userPath = Paths.get(this.userDirectory.toString(), ctx.userName.toString());
            if (userPath.toFile().exists()) {
                final File     userDir  = userPath.toFile();
                final String[] allFiles = userDir.list();
                for (final String fileName : allFiles) {
                    if (!fileName.startsWith(".")) {
                        final Path filePath = Paths.get(userPath.toString(), fileName);
                        FileUtils.deleteQuietly(filePath.toFile());
                    }
                }
            }
            if (ctx.accountDeleted) {
                FileUtils.deleteQuietly(userPath.toFile());
            }

            // Delete all user drafts
            final Path draftPath = Paths.get(this.draftDirectory.toString(), ctx.userKey.toString());
            FileUtils.deleteQuietly(draftPath.toFile());

            // Delete all invoices specific to the user
            final Path invoicePath = Paths.get(this.invoiceDirectory.toString(), ctx.userId.toString());
            FileUtils.deleteQuietly(invoicePath.toFile());

            // Delete all orders specific to the user
            final Path orderPath = Paths.get(this.orderDirectory.toString(), ctx.userId.toString());
            FileUtils.deleteQuietly(orderPath.toFile());

            // Delete all user service files
            final Path userServicePath = Paths.get(this.userServiceDirectory.toString(), ctx.userKey.toString());
            FileUtils.deleteQuietly(userServicePath.toFile());

            // Extend lock duration
            externalTaskService.extendLock(externalTask, this.getLockDuration());
        } catch (Exception ex) {
            final String message = String.format("Failed to delete files [userKey=%s]", ctx.userKey);
            throw new ServiceException(BasicMessageCode.InternalServerError, message, ex);
        } finally {
            // Extend lock duration
            externalTaskService.extendLock(externalTask, this.getLockDuration());
        }
    }

    private void deleteDatabaseRecords(
        ExternalTask externalTask, ExternalTaskService externalTaskService, OperationContext ctx
    ) throws ServiceException {
        try (final InputStream resource = this.resourceLoader.getResource("classpath:sql/delete-all-user-data.sql").getInputStream();) {
            final String sqlTemplate = IOUtils.toString(resource, StandardCharsets.UTF_8);

            Map<String, String> parameters = new HashMap<>();
            parameters.put("accountId", ctx.userId.toString());
            parameters.put("accountKey", ctx.userKey.toString());
            parameters.put("pid", StringUtils.join(ctx.pid.stream().map(s -> "'" + s + "'").toArray(), ","));
            parameters.put("accountDeleted", Boolean.valueOf(ctx.accountDeleted).toString());
            parameters.put("contractsDeleted", Boolean.valueOf(ctx.contractsDeleted).toString());

            StringSubstitutor stringSubstitutor = new StringSubstitutor(parameters);

            final String sql = stringSubstitutor.replace(sqlTemplate);

            this.jdbcTemplate.execute(sql);

            // Extend lock duration
            externalTaskService.extendLock(externalTask, this.getLockDuration());
        } catch (Exception ex) {
            final String message = String.format("Failed to delete database records [userKey=%s]", ctx.userKey);
            throw new ServiceException(BasicMessageCode.InternalServerError, message, ex);
        } finally {
            // Extend lock duration
            externalTaskService.extendLock(externalTask, this.getLockDuration());
        }
    }

    private void deleteOAuthClients(ExternalTask externalTask, ExternalTaskService externalTaskService, OperationContext ctx) {
        try {
            final Direction   direction   = Direction.ASC;
            PageRequest pageRequest = PageRequest.of(0, 10, Sort.by(direction, "id"));

            PageResultDto<AccountClientDto> clients = this.accountClientService.findAll(ctx.userKey, pageRequest);

            while (!clients.getItems().isEmpty()) {
                for (final AccountClientDto c : clients.getItems()) {
                    this.accountClientService.revoke(ctx.userId, c.getClientId());
                }
                // Fetch next batch
                pageRequest = pageRequest.next();
                clients = this.accountClientService.findAll(ctx.userKey, pageRequest);
                // Extend lock duration
                externalTaskService.extendLock(externalTask, this.getLockDuration());
            }
        } catch (Exception ex) {
            final String message = String.format("Failed to delete OAuth client [userKey=%s]", ctx.userKey);
            throw new ServiceException(BasicMessageCode.InternalServerError, message, ex);
        } finally {
            // Extend lock duration
            externalTaskService.extendLock(externalTask, this.getLockDuration());
        }
    }

    private void deleteIdpUser(ExternalTask externalTask, ExternalTaskService externalTaskService, OperationContext ctx) {
        try {
            final UserQueryDto queryForUsername = new UserQueryDto();
            queryForUsername.setUsername(ctx.userName);
            queryForUsername.setExact(true);

            final List<UserDto> usersForUsername = keycloakAdminService.findUsers(queryForUsername);

            Assert.state(
                usersForUsername.size() < 2,
                () -> "expected no more than one IDP user for a given username [username=" + ctx.userName + "]"
            );

            if (!usersForUsername.isEmpty()) {
                this.keycloakAdminService.deleteUser(usersForUsername.get(0).getId());
            }
        } catch (Exception ex) {
            final String message = String.format("Failed to delete IDP user [userKey=%s]", ctx.userKey);
            throw new ServiceException(BasicMessageCode.InternalServerError, message, ex);
        } finally {
            // Extend lock duration
            externalTaskService.extendLock(externalTask, this.getLockDuration());
        }
    }

    private void deleteUserProfile(ExternalTask externalTask, ExternalTaskService externalTaskService, OperationContext ctx) {
        try {
            this.elasticSearchService.removeProfile(ctx.userKey);
        } catch (Exception ex) {
            final String message = String.format("Failed to delete user profile from Elasticsearch [userKey=%s]", ctx.userKey);
            throw new ServiceException(BasicMessageCode.InternalServerError, message, ex);
        } finally {
            // Extend lock duration
            externalTaskService.extendLock(externalTask, this.getLockDuration());
        }
    }

    @AllArgsConstructor
    @Builder
    public static class OperationContext {
        private Integer userId;

        private UUID userKey;

        private UUID userParentKey;

        private String userGeodataShard;

        private String userName;

        private boolean accountDeleted;

        private boolean contractsDeleted;

        private boolean fileSystemDeleted;

        @Builder.Default
        private List<String> pid = new ArrayList<>();

        public boolean isFileSystemDeleted() {
            return this.accountDeleted || this.fileSystemDeleted;
        }
    }
}