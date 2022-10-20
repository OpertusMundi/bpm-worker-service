package eu.opertusmundi.bpm.worker.subscriptions.support;

import java.util.UUID;

import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import eu.opertusmundi.bpm.worker.model.DeleteAccountOperationContext;
import eu.opertusmundi.bpm.worker.service.AccountDataDeletionService;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.model.EnumAccountType;

@Service
public class DeleteAllUserDataTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(DeleteAllUserDataTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.delete-all-user-data.lock-duration:120000}")
    private Long lockDurationMillis;
  
    @Autowired
    private AccountDataDeletionService accountDataDeletionService;
    
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
            final String  userGeodataShard  = this.getVariableAsString(externalTask, externalTaskService, "userGeodataShard", "");
            final boolean fileSystemDeleted = this.getVariableAsBoolean(externalTask, externalTaskService, "fileSystemDeleted", false);
            final boolean contractsDeleted  = this.getVariableAsBoolean(externalTask, externalTaskService, "contractsDeleted", false);
            final boolean accountDeleted    = this.getVariableAsBoolean(externalTask, externalTaskService, "accountDeleted", false);

            final String userTypeName = this.getVariableAsString(
                externalTask, externalTaskService, "userType", EnumAccountType.OPERTUSMUNDI.toString()
            );
            final EnumAccountType userType = EnumAccountType.valueOf(userTypeName);
            
            logger.info("Received task. [taskId={}]", taskId);

            final DeleteAccountOperationContext ctx = this.accountDataDeletionService.createContext(
                userId, userKey, userParentKey, userName, userType, userGeodataShard, accountDeleted, contractsDeleted, fileSystemDeleted,
                () -> {
                    externalTaskService.extendLock(externalTask, this.getLockDuration());
                }
            );
            
            this.accountDataDeletionService.deleteAllUserData(externalTask, externalTaskService, ctx);

            // Delete all orphan vendor accounts
            if (accountDeleted && ctx.getUserType() == EnumAccountType.OPERTUSMUNDI) {
                this.accountDataDeletionService.deleteOrphanVendorAccounts(ctx);
            }

            externalTaskService.complete(externalTask);

            logger.info("Completed task. [taskId={}]", taskId);
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleFailure(externalTaskService, externalTask, ex);
        }
    }
}