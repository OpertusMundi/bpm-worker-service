package eu.opertusmundi.bpm.worker.service;

import java.util.UUID;

import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;

import eu.opertusmundi.bpm.worker.model.DeleteAccountOperationContext;
import eu.opertusmundi.common.model.EnumAccountType;
import eu.opertusmundi.common.model.ServiceException;

public interface AccountDataDeletionService {

    /**
     * Create account delete operation context
     * 
     * @param userId
     * @param userKey
     * @param userParentKey
     * @param userName
     * @param userTypeName
     * @param userGeodataShard
     * @param accountDeleted
     * @param contractsDeleted
     * @param fileSystemDeleted
     * @param completeTask
     * @return
     */
    DeleteAccountOperationContext createContext(
        Integer userId, 
        UUID userKey, 
        UUID userParentKey, 
        String userName, 
        EnumAccountType userTypeName,
        String userGeodataShard, 
        boolean accountDeleted,
        boolean contractsDeleted, 
        boolean fileSystemDeleted,
        Runnable completeTask
    );
    
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
     * {@link DeleteAccountOperationContext#accountDeleted} or
     * {@link DeleteAccountOperationContext#fileSystemDeleted} is set to {@code true}
     *
     * <li>OAuth clients created by the user
     *
     * <li>All database records that refer to the specified user. If
     * {@link DeleteAccountOperationContext#accountDeleted} is {@code true}, the user is also
     * deleted
     * 
     * <li>The corresponding user in the IDP if
     * {@link DeleteAccountOperationContext#accountDeleted} is set to {@code true}
     * </ul>
     *
     * @param externalTask
     * @param externalTaskService
     * @param ctx
     * @throws ServiceException
     */
    void deleteAllUserData(
        ExternalTask externalTask, ExternalTaskService externalTaskService, DeleteAccountOperationContext ctx
    ) throws ServiceException;
    

    /**
     * Deletes all orphan vendor account
     * 
     * @param ctx
     */
    void deleteOrphanVendorAccounts(DeleteAccountOperationContext ctx);
}
