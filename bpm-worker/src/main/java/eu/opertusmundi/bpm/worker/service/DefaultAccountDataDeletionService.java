package eu.opertusmundi.bpm.worker.service;

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
import org.camunda.bpm.engine.rest.dto.VariableValueDto;
import org.camunda.bpm.engine.rest.dto.runtime.ProcessInstanceDto;
import org.camunda.bpm.engine.rest.dto.runtime.VariableInstanceDto;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import eu.opertusmundi.bpm.worker.model.DeleteAccountOperationContext;
import eu.opertusmundi.common.domain.ProviderAssetDraftEntity;
import eu.opertusmundi.common.model.BasicMessageCode;
import eu.opertusmundi.common.model.EnumAccountType;
import eu.opertusmundi.common.model.EnumRole;
import eu.opertusmundi.common.model.PageResultDto;
import eu.opertusmundi.common.model.ServiceException;
import eu.opertusmundi.common.model.account.AccountClientDto;
import eu.opertusmundi.common.model.account.AccountDto;
import eu.opertusmundi.common.model.account.EnumAccountActiveTask;
import eu.opertusmundi.common.model.asset.service.UserServiceDto;
import eu.opertusmundi.common.model.catalogue.CatalogueResult;
import eu.opertusmundi.common.model.catalogue.client.CatalogueAssetQuery;
import eu.opertusmundi.common.model.catalogue.client.CatalogueItemDetailsDto;
import eu.opertusmundi.common.model.catalogue.client.CatalogueItemDto;
import eu.opertusmundi.common.model.keycloak.server.UserDto;
import eu.opertusmundi.common.model.keycloak.server.UserQueryDto;
import eu.opertusmundi.common.model.workflow.EnumProcessInstanceVariable;
import eu.opertusmundi.common.model.workflow.EnumWorkflow;
import eu.opertusmundi.common.repository.AccountRepository;
import eu.opertusmundi.common.repository.DraftRepository;
import eu.opertusmundi.common.service.AccountClientService;
import eu.opertusmundi.common.service.CatalogueService;
import eu.opertusmundi.common.service.ElasticSearchService;
import eu.opertusmundi.common.service.IngestService;
import eu.opertusmundi.common.service.KeycloakAdminService;
import eu.opertusmundi.common.service.UserServiceService;
import eu.opertusmundi.common.util.BpmEngineUtils;
import eu.opertusmundi.common.util.BpmInstanceVariablesBuilder;
import eu.opertusmundi.common.util.StreamUtils;
import feign.FeignException.FeignClientException;

@Service
public class DefaultAccountDataDeletionService implements AccountDataDeletionService, InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(DefaultAccountDataDeletionService.class);
    
    @Autowired
    private ResourceLoader resourceLoader;

    @Autowired
    private DataSource dataSource;

    private JdbcTemplate jdbcTemplate;

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
    public DeleteAccountOperationContext createContext(
        Integer userId, 
        UUID userKey, 
        UUID userParentKey, 
        String userName, 
        EnumAccountType userTypeName,
        String userGeodataShard,
        boolean accountDeleted,
        boolean contractsDeleted, 
        boolean fileSystemDeleted,
        Runnable extendLock
    ) {
        final AccountDto account = this.accountRepository.findOneByKeyObject(userKey).orElse(null);
        if (account != null && !account.hasRole(EnumRole.ROLE_TESTER)) {
            logger.warn("Account does not have role ROLE_TESTER [userKey={}]", userKey);
        }
        
        final DeleteAccountOperationContext ctx = DeleteAccountOperationContext.builder()
            .userId(userId)
            .userKey(userKey)
            .userParentKey(userParentKey)
            .userName(userName)
            .userType(userTypeName)
            .userGeodataShard(userGeodataShard)
            .accountDeleted(accountDeleted)
            .contractsDeleted(contractsDeleted)
            .fileSystemDeleted(fileSystemDeleted)
            .extendLock(extendLock)
            .build();
        
        return ctx;
    }

    @Override
    public void deleteAllUserData(ExternalTask externalTask, ExternalTaskService externalTaskService, DeleteAccountOperationContext ctx) throws ServiceException {
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

        if (ctx.isFileSystemDeleted() || ctx.isAccountDeleted()) {
            this.deleteAllFiles(externalTask, externalTaskService, ctx);
        }

        this.deleteOAuthClients(externalTask, externalTaskService, ctx);

        if (ctx.isAccountDeleted()) {
            this.deleteIdpUser(externalTask, externalTaskService, ctx);
            this.deleteUserProfile(externalTask, externalTaskService, ctx);          
        }

        this.deleteDatabaseRecords(externalTask, externalTaskService, ctx);
    }

    @Override
    public void deleteOrphanVendorAccounts(DeleteAccountOperationContext ctx) {
        this.accountRepository.findAllOrphanVendor().stream().forEach(account -> {
            final String       businessKey = account.getKey().toString() + "::DELETE";
            ProcessInstanceDto instance    = this.bpmEngineUtils.findInstance(businessKey);

            if (account.getActiveTask() == EnumAccountActiveTask.NONE || instance == null) {
                account.setActiveTask(EnumAccountActiveTask.DELETE);
                this.accountRepository.saveAndFlush(account);
                
                final Map<String, VariableValueDto> variables = BpmInstanceVariablesBuilder.builder()
                    .variableAsString(EnumProcessInstanceVariable.START_USER_KEY.getValue(), ctx.getUserKey().toString())
                    .variableAsInteger("userId", account.getId())
                    .variableAsString("userKey", account.getKey().toString())
                    .variableAsString("userParentKey", account.getParentKey().toString())
                    .variableAsString("userName", account.getEmail())
                    .variableAsString("userType", account.getType().toString())
                    .variableAsString("userGeodataShard", account.getProfile().getGeodataShard())
                    .variableAsBoolean("accountDeleted", true)
                    .variableAsBoolean("fileSystemDeleted", true)
                    .variableAsBoolean("contractsDeleted", true)
                    .build();

                instance = this.bpmEngineUtils.startProcessDefinitionByKey(
                    EnumWorkflow.SYSTEM_MAINTENANCE_DELETE_USER, businessKey, variables
                );
            }
        });
    }
    
    private void deleteWorkflowInstances(ExternalTask externalTask, ExternalTaskService externalTaskService, DeleteAccountOperationContext ctx) {
        try {
            final List<String>              processInstances = new ArrayList<>();
            final List<VariableInstanceDto> variables        = this.bpmEngineUtils.getVariables(
                EnumProcessInstanceVariable.START_USER_KEY.getValue(), ctx.getUserKey().toString()
            );
            for (final VariableInstanceDto v : variables) {
                if (!processInstances.contains(v.getProcessInstanceId())) {
                    processInstances.add(v.getProcessInstanceId());
                }
            }

            final List<String>              processHistoryInstances = new ArrayList<>();
            final List<VariableInstanceDto> historyVariables        = this.bpmEngineUtils.getHistoryVariables(
                EnumProcessInstanceVariable.START_USER_KEY.getValue(), ctx.getUserKey().toString()
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
            final String message = String.format("Failed to delete workflow instances [userKey=%s]", ctx.getUserKey());
            throw new ServiceException(BasicMessageCode.InternalServerError, message, ex);
        } finally {
            // Extend lock duration
            ctx.getExtendLock().run();
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

    private void deleteUserServices(ExternalTask externalTask, ExternalTaskService externalTaskService, DeleteAccountOperationContext ctx) {
        try {
            int index = 0;
            PageResultDto<UserServiceDto> services = this.userServiceService.findAll(
                ctx.getUserKey(), ctx.getUserParentKey(), null, null, null, index, 10
            );

            while (!services.getItems().isEmpty()) {
                for (final UserServiceDto service : services.getItems()) {
                    this.ingestService.removeDataAndLayer(
                        // Since we are not using a UserGeodataConfiguration
                        // instance, we compose the workspace and service
                        // names manually by adding the required prefixes
                        ctx.getUserGeodataShard(), "p_" + ctx.getUserParentKey().toString(), "_" + service.getKey().toString()
                    );
                }
                index++;
                services = this.userServiceService.findAll(ctx.getUserKey(), ctx.getUserParentKey(), null, null, null, index, 10);

                // Extend lock duration
                ctx.getExtendLock().run();
            }
        } catch (Exception ex) {
            final String message = String.format("Failed to delete user services [userKey=%s]", ctx.getUserKey());
            throw new ServiceException(BasicMessageCode.InternalServerError, message, ex);
        } finally {
            // Extend lock duration
            ctx.getExtendLock().run();
        }
    }

    private void deleteAssets(ExternalTask externalTask, ExternalTaskService externalTaskService, DeleteAccountOperationContext ctx) {
        // All assets are published under the parent account
        if (ctx.getUserType() == EnumAccountType.VENDOR) {
            return;
        }
        try {         
            CatalogueAssetQuery query = CatalogueAssetQuery.builder()
                .publisherKey(ctx.getUserKey().toString())
                .page(0)
                .size(20)
                .build();

            CatalogueResult<CatalogueItemDto> result = this.catalogueService.findAll(null, query);

            while (!result.getResult().getItems().isEmpty()) {
                for(final CatalogueItemDto item: result.getResult().getItems()) {
                    ctx.getPid().add(item.getId());
                    final CatalogueItemDetailsDto itemDetails = this.catalogueService.unpublish(ctx.getUserKey(), item.getId());

                    // Delete ingested data
                    StreamUtils.from(itemDetails.getIngestionInfo()).forEach(d -> {
                        // Since we are not using a UserGeodataConfiguration
                        // instance, we compose the workspace name manually by
                        // adding the required prefix
                        this.ingestService.removeDataAndLayer(ctx.getUserGeodataShard(), "_" + ctx.getUserParentKey().toString(), d.getTableName());
                    });
                }

                // Fetch next batch
                query = CatalogueAssetQuery.builder()
                    .publisherKey(ctx.getUserKey().toString())
                    .page(0)
                    .size(20)
                    .build();

                result = this.catalogueService.findAll(null, query);

                // Extend lock duration
                ctx.getExtendLock().run();
            }
        } catch (Exception ex) {
            final String message = String.format("Failed to delete published assets [userKey=%s]", ctx.getUserKey());
            throw new ServiceException(BasicMessageCode.InternalServerError, message, ex);
        } finally {
            // Extend lock duration
            ctx.getExtendLock().run();
        }
    }

    private void collectAssetPids(ExternalTask externalTask, ExternalTaskService externalTaskService, DeleteAccountOperationContext ctx) {
        // All assets are published under the parent account
        if (ctx.getUserType() == EnumAccountType.VENDOR) {
            return;
        }
        try {
            final Direction direction   = Direction.ASC;
            PageRequest     pageRequest = PageRequest.of(0, 10, Sort.by(direction, "id"));

            Page<ProviderAssetDraftEntity> drafts = this.draftRepository.findAllByPublisher(ctx.getUserKey(), pageRequest);

            while (!drafts.isEmpty()) {
                for (final ProviderAssetDraftEntity d : drafts.getContent()) {
                    if (!StringUtils.isBlank(d.getAssetPublished()) && !ctx.getPid().contains(d.getAssetPublished())) {
                        ctx.getPid().add(d.getAssetPublished());
                    }
                }
                // Fetch next batch
                pageRequest = pageRequest.next();
                drafts      = this.draftRepository.findAllByPublisher(ctx.getUserKey(), pageRequest);
                // Extend lock duration
                ctx.getExtendLock().run();
            }
        } catch (Exception ex) {
            final String message = String.format("Failed to delete IDP user [userKey=%s]", ctx.getUserKey());
            throw new ServiceException(BasicMessageCode.InternalServerError, message, ex);
        } finally {
            // Extend lock duration
            ctx.getExtendLock().run();
        }
    }

    private void deleteAssetStatistics(ExternalTask externalTask, ExternalTaskService externalTaskService, DeleteAccountOperationContext ctx) {
        try {
            if (ctx.getPid().isEmpty()) {
                return;
            }
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            final BoolQueryBuilder    query         = QueryBuilders.boolQuery();
            for (final String pid : ctx.getPid()) {
                query.should(QueryBuilders.matchQuery("id.keyword", pid));
            }
            query.minimumShouldMatch(1);
            final String entity = searchBuilder.query(query).toString();

            this.elasticSearchService.performRequest(HttpMethod.POST, "/assets_view/_delete_by_query", entity);
            this.elasticSearchService.performRequest(HttpMethod.POST, "/assets_view_aggregate/_delete_by_query", entity);
        } catch (Exception ex) {
            final String message = String.format("Failed to delete asset statistics [userKey=%s, pid=%s]", ctx.getUserKey(), ctx.getPid());
            throw new ServiceException(BasicMessageCode.InternalServerError, message, ex);
        } finally {
            // Extend lock duration
            ctx.getExtendLock().run();
        }
    }

    private void deleteAllFiles(
        ExternalTask externalTask, ExternalTaskService externalTaskService, DeleteAccountOperationContext ctx
    ) {
        try {
            // Asset symbolic links to draft data
            for (final String p : ctx.getPid()) {
                final Path assetPath = Paths.get(this.assetDirectory.toString(), p);
                FileUtils.deleteQuietly(assetPath.toFile());
            }

            // Delete all contracts specific to the user
            final Path contractPath = Paths.get(this.contractDirectory.toString(), ctx.getUserId().toString());
            FileUtils.deleteQuietly(contractPath.toFile());

            // Delete user data
            final Path userPath = Paths.get(this.userDirectory.toString(), ctx.getUserName().toString());
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
            if (ctx.isAccountDeleted()) {
                FileUtils.deleteQuietly(userPath.toFile());
            }

            // Delete all user drafts
            final Path draftPath = Paths.get(this.draftDirectory.toString(), ctx.getUserKey().toString());
            FileUtils.deleteQuietly(draftPath.toFile());

            // Delete all invoices specific to the user
            final Path invoicePath = Paths.get(this.invoiceDirectory.toString(), ctx.getUserId().toString());
            FileUtils.deleteQuietly(invoicePath.toFile());

            // Delete all orders specific to the user
            final Path orderPath = Paths.get(this.orderDirectory.toString(), ctx.getUserId().toString());
            FileUtils.deleteQuietly(orderPath.toFile());

            // Delete all user service files
            final Path userServicePath = Paths.get(this.userServiceDirectory.toString(), ctx.getUserKey().toString());
            FileUtils.deleteQuietly(userServicePath.toFile());

            // Extend lock duration
            ctx.getExtendLock().run();
        } catch (Exception ex) {
            final String message = String.format("Failed to delete files [userKey=%s]", ctx.getUserKey());
            throw new ServiceException(BasicMessageCode.InternalServerError, message, ex);
        } finally {
            // Extend lock duration
            ctx.getExtendLock().run();
        }
    }

    private void deleteDatabaseRecords(
        ExternalTask externalTask, ExternalTaskService externalTaskService, DeleteAccountOperationContext ctx
    ) throws ServiceException {
        try (final InputStream resource = this.resourceLoader.getResource("classpath:sql/delete-all-user-data.sql").getInputStream();) {
            final String sqlTemplate = IOUtils.toString(resource, StandardCharsets.UTF_8);

            Map<String, String> parameters = new HashMap<>();
            parameters.put("accountId", ctx.getUserId().toString());
            parameters.put("accountKey", ctx.getUserKey().toString());
            parameters.put("pid", StringUtils.join(ctx.getPid().stream().map(s -> "'" + s + "'").toArray(), ","));
            parameters.put("accountDeleted", Boolean.valueOf(ctx.isAccountDeleted()).toString());
            parameters.put("contractsDeleted", Boolean.valueOf(ctx.isContractsDeleted()).toString());

            StringSubstitutor stringSubstitutor = new StringSubstitutor(parameters);

            final String sql = stringSubstitutor.replace(sqlTemplate);

            this.jdbcTemplate.execute(sql);

            // Extend lock duration
            ctx.getExtendLock().run();
        } catch (Exception ex) {
            final String message = String.format("Failed to delete database records [userKey=%s]", ctx.getUserKey());
            throw new ServiceException(BasicMessageCode.InternalServerError, message, ex);
        } finally {
            // Extend lock duration
            ctx.getExtendLock().run();
        }
    }

    private void deleteOAuthClients(ExternalTask externalTask, ExternalTaskService externalTaskService, DeleteAccountOperationContext ctx) {
        try {
            final Direction   direction   = Direction.ASC;
            PageRequest pageRequest = PageRequest.of(0, 10, Sort.by(direction, "id"));

            PageResultDto<AccountClientDto> clients = this.accountClientService.findAll(ctx.getUserKey(), pageRequest);

            while (!clients.getItems().isEmpty()) {
                for (final AccountClientDto c : clients.getItems()) {
                    this.accountClientService.revoke(ctx.getUserId(), c.getClientId());
                }
                // Fetch next batch
                pageRequest = pageRequest.next();
                clients = this.accountClientService.findAll(ctx.getUserKey(), pageRequest);
                // Extend lock duration
                ctx.getExtendLock().run();
            }
        } catch (Exception ex) {
            final String message = String.format("Failed to delete OAuth client [userKey=%s]", ctx.getUserKey());
            throw new ServiceException(BasicMessageCode.InternalServerError, message, ex);
        } finally {
            // Extend lock duration
            ctx.getExtendLock().run();
        }
    }

    private void deleteIdpUser(ExternalTask externalTask, ExternalTaskService externalTaskService, DeleteAccountOperationContext ctx) {
        try {
            final UserQueryDto queryForUsername = new UserQueryDto();
            queryForUsername.setUsername(ctx.getUserName());
            queryForUsername.setExact(true);

            final List<UserDto> usersForUsername = keycloakAdminService.findUsers(queryForUsername);

            Assert.state(
                usersForUsername.size() < 2,
                () -> "expected no more than one IDP user for a given username [username=" + ctx.getUserName() + "]"
            );

            if (!usersForUsername.isEmpty()) {
                this.keycloakAdminService.deleteUser(usersForUsername.get(0).getId());
            }
        } catch (Exception ex) {
            final String message = String.format("Failed to delete IDP user [userKey=%s]", ctx.getUserKey());
            throw new ServiceException(BasicMessageCode.InternalServerError, message, ex);
        } finally {
            // Extend lock duration
            ctx.getExtendLock().run();
        }
    }

    private void deleteUserProfile(ExternalTask externalTask, ExternalTaskService externalTaskService, DeleteAccountOperationContext ctx) {
        try {
            this.elasticSearchService.removeProfile(ctx.getUserKey());
        } catch (Exception ex) {
            final String message = String.format("Failed to delete user profile from Elasticsearch [userKey=%s]", ctx.getUserKey());
            throw new ServiceException(BasicMessageCode.InternalServerError, message, ex);
        } finally {
            // Extend lock duration
            ctx.getExtendLock().run();
        }
    }

}
