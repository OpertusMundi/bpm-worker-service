package eu.opertusmundi.bpm.worker.subscriptions.support;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.repository.AccountRepository;

@Service
public class DeleteOrphanFileSystemEntriesTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(DeleteOrphanFileSystemEntriesTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.delete-orphan-file-system-entries.lock-duration:120000}")
    private Long lockDurationMillis;

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

    @Override
    protected long getLockDuration() {
        return this.lockDurationMillis;
    }

    @Override
    public String getTopicName() {
        return "deleteOrphanFileSystemEntries";
    }

    @Override
    public final void execute(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        try {
            final String  taskId            = externalTask.getId();

            logger.info("Received task. [taskId={}]", taskId);

            this.deleteProviderAssetDir();
            externalTaskService.extendLock(externalTask, this.getLockDuration());
            
            this.deleteUserContractDir();
            externalTaskService.extendLock(externalTask, this.getLockDuration());
            
            this.deleteProviderDraftDir();
            externalTaskService.extendLock(externalTask, this.getLockDuration());
            
            this.deleteUserInvoiceDir();
            externalTaskService.extendLock(externalTask, this.getLockDuration());
            
            this.deleteUserOrderDir();
            externalTaskService.extendLock(externalTask, this.getLockDuration());
            
            this.deleteUserDataDir();
            externalTaskService.extendLock(externalTask, this.getLockDuration());
            
            this.deleteUserServiceDir();
            externalTaskService.complete(externalTask);

            logger.info("Completed task. [taskId={}]", taskId);
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleFailure(externalTaskService, externalTask, ex);
        }
    }

    private void deleteProviderAssetDir() {
        // Provider asset directories are symbolic links to draft
        // directories. A draft directory is composed by the root path for
        // drafts, the provider unique key and the draft unique key
        for (final File providerAssetDir : this.assetDirectory.toFile().listFiles()) {
            if (providerAssetDir.isFile()) {
                logger.warn(
                    "A file was found in provider asset directory. Expected only symbolic links to directories [file={}]",
                    providerAssetDir
                );
                continue;
            }

            try {
                final Path targetPath = providerAssetDir.toPath().toRealPath();
                if (targetPath != null && targetPath.toFile().exists()) {
                    final int    count           = targetPath.getNameCount();
                    final String userKeyAsString = targetPath.getName(count - 2).toString();
                    try {
                        final UUID userKey = UUID.fromString(userKeyAsString);
                        if (!this.userExists(userKey)) {
                            FileUtils.deleteQuietly(providerAssetDir);
                            logger.warn("Deleted asset symbolic link. Provider was not found [link={}]", providerAssetDir);
                        }
                    } catch (IllegalArgumentException ex) {
                        logger.warn("Found invalid draft path. Expected a UUID value for the provider key [path={}]", targetPath);
                    }
                } else {
                    // Delete invalid link
                    FileUtils.deleteQuietly(providerAssetDir);
                    logger.warn("Deleted symbolic link. Target was not found [link={}]", providerAssetDir);
                }
            } catch (IOException e) {
                logger.warn("Failed to resolve symbolic link target [link={}]", providerAssetDir);
            }
        }
    }

    private void deleteUserContractDir() {
        for (final File userContractDir : this.contractDirectory.toFile().listFiles()) {
            if (userContractDir.isFile()) {
                logger.warn("A file was found in user contract directory. Expected only directories [file={}]", userContractDir);
                continue;
            }
            final Path   userContractPath = userContractDir.toPath();
            final int    count            = userContractPath.getNameCount();
            final String userIdAsString   = userContractPath.getName(count - 1).toString();
            try {
                final Integer userId = Integer.parseInt(userIdAsString);
                if (!this.userExists(userId)) {
                    FileUtils.deleteQuietly(userContractDir);
                    logger.warn("Deleted contract directory. User was not found [link={}]", userContractDir);
                }
            } catch (NumberFormatException ex) {
                logger.warn("Found invalid contract path. Expected a Integer value for the user id [path={}]", userContractDir);
            }
        }
    }

    private void deleteProviderDraftDir() {
        for (final File providerDraftDir : this.draftDirectory.toFile().listFiles()) {
            if (providerDraftDir.isFile()) {
                logger.warn("A file was found in user draft directory. Expected only directories [file={}]", providerDraftDir);
                continue;
            }
            final Path   providerDraftPath   = providerDraftDir.toPath();
            final int    count               = providerDraftPath.getNameCount();
            final String providerKeyAsString = providerDraftPath.getName(count - 1).toString();
            try {
                final UUID providerKey = UUID.fromString(providerKeyAsString);
                if (!this.userExists(providerKey)) {
                    FileUtils.deleteQuietly(providerDraftDir);
                    logger.warn("Deleted draft directory. Provider was not found [link={}]", providerDraftDir);
                }
            } catch (IllegalArgumentException ex) {
                logger.warn("Found invalid draft path. Expected a UUID value for the provider key [path={}]", providerDraftDir);
            }
        }
    }

    private void deleteUserInvoiceDir() {
        for (final File userInvoiceDir : this.invoiceDirectory.toFile().listFiles()) {
            if (userInvoiceDir.isFile()) {
                logger.warn("A file was found in user invoice directory. Expected only directories [file={}]", userInvoiceDir);
                continue;
            }
            final Path   userInvoicePath = userInvoiceDir.toPath();
            final int    count           = userInvoicePath.getNameCount();
            final String userIdAsString  = userInvoicePath.getName(count - 1).toString();
            try {
                final Integer userId = Integer.parseInt(userIdAsString);
                if (!this.userExists(userId)) {
                    FileUtils.deleteQuietly(userInvoiceDir);
                    logger.warn("Deleted invoice directory. User was not found [link={}]", userInvoiceDir);
                }
            } catch (NumberFormatException ex) {
                logger.warn("Found invalid invoice path. Expected a Integer value for the user id [path={}]", userInvoiceDir);
            }
        }
    }

    private void deleteUserOrderDir() {
        for (final File userOrderDir : this.orderDirectory.toFile().listFiles()) {
            if (userOrderDir.isFile()) {
                logger.warn("A file was found in user orders directory. Expected only directories [file={}]", userOrderDir);
                continue;
            }
            final Path   userOrderPath  = userOrderDir.toPath();
            final int    count          = userOrderPath.getNameCount();
            final String userIdAsString = userOrderPath.getName(count - 1).toString();
            try {
                final Integer userId = Integer.parseInt(userIdAsString);
                if (!this.userExists(userId)) {
                    FileUtils.deleteQuietly(userOrderDir);
                    logger.warn("Deleted orders directory. User was not found [link={}]", userOrderDir);
                }
            } catch (NumberFormatException ex) {
                logger.warn("Found invalid orders path. Expected a Integer value for the user id [path={}]", userOrderDir);
            }
        }
    }

    private void deleteUserDataDir() {
        for (final File userDataDir : this.userDirectory.toFile().listFiles()) {
            if (userDataDir.isFile()) {
                logger.warn("A file was found in user data directory. Expected only directories [file={}]", userDataDir);
                continue;
            }
            final Path   userDataPath = userDataDir.toPath();
            final int    count        = userDataPath.getNameCount();
            final String userEmail    = userDataPath.getName(count - 1).toString();
            if (!this.userExists(userEmail)) {
                FileUtils.deleteQuietly(userDataDir);
                logger.warn("Deleted user data directory. User was not found [link={}]", userDataDir);
            }
        }
    }

    private void deleteUserServiceDir() {
        for (final File userServiceDir : this.userServiceDirectory.toFile().listFiles()) {
            if (userServiceDir.isFile()) {
                logger.warn("A file was found in user services directory. Expected only directories [file={}]", userServiceDir);
                continue;
            }
            final Path   userServicePath = userServiceDir.toPath();
            final int    count           = userServicePath.getNameCount();
            final String userKeyAsString = userServicePath.getName(count - 1).toString();
            try {
                final UUID userKey = UUID.fromString(userKeyAsString);
                if (!this.userExists(userKey)) {
                    FileUtils.deleteQuietly(userServiceDir);
                    logger.warn("Deleted user services directory. User was not found [link={}]", userServiceDir);
                }
            } catch (IllegalArgumentException ex) {
                logger.warn("Found invalid user services path. Expected a UUID value for the user key [path={}]", userServiceDir);
            }
        }
    }

    private boolean userExists(Integer id) {
        return this.accountRepository.findById(id).isPresent();
    }

    private boolean userExists(UUID key) {
        return this.accountRepository.findOneByKey(key).isPresent();
    }

    private boolean userExists(String email) {
        return this.accountRepository.findOneByEmail(email).isPresent();
    }

}