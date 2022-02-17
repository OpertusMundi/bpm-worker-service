package eu.opertusmundi.bpm.worker.subscriptions.user;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;

import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.domain.AccountEntity;
import eu.opertusmundi.common.model.account.AccountMessageCode;
import eu.opertusmundi.common.model.account.EnumActivationStatus;
import eu.opertusmundi.common.model.account.SimpleAccountDto;
import eu.opertusmundi.common.model.file.EnumUserFileReservedEntry;
import eu.opertusmundi.common.model.file.UserFileNamingStrategyContext;
import eu.opertusmundi.common.repository.AccountRepository;
import eu.opertusmundi.common.service.DefaultUserFileNamingStrategy;

@Service
public class ActivateAccountTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(ActivateAccountTaskService.class);

    private static final String VARIABLE_USER_KEY = "userKey";

    private static final FileAttribute<Set<PosixFilePermission>> DEFAULT_DIRECTORY_ATTRIBUTE =
        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxrwxr-x"));

    @Value("${opertusmundi.bpm.worker.tasks.activate-account.lock-duration:120000}")
    private Long lockDurationMillis;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private DefaultUserFileNamingStrategy userFileNamingStrategy;

    @Override
    public String getTopicName() {
        return "activateAccount";
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

            final UUID userKey = this.getVariableAsUUID(externalTaskService, externalTask, VARIABLE_USER_KEY);
            logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

            // Complete account registration
            final SimpleAccountDto accountDto = this.activateAccount(userKey);

            // Prepare home directory for the new account
            this.setupHomeDirectory(accountDto);

            // Done
            externalTaskService.complete(externalTask);
            logger.info("Completed task. [taskId={}]", taskId);
        } catch (final BpmnWorkerException ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);
            externalTaskService.handleFailure(
                externalTask, ex.getMessage(), ex.getErrorDetails(), ex.getRetries(), ex.getRetryTimeout()
            );
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);
            this.handleError(externalTaskService, externalTask, ex);
        }
    }

    @Transactional
    private SimpleAccountDto activateAccount(UUID userKey) throws BpmnWorkerException {
        final AccountEntity account = this.accountRepository.findOneByKey(userKey).orElse(null);

        if (account == null) {
            final String message = String.format("Account not found [userKey=%s]", userKey);
            throw this.buildException(AccountMessageCode.ACCOUNT_NOT_FOUND, message, message);
        }

        if (account.getActivationStatus() != EnumActivationStatus.PROCESSING) {
            final String message = String.format(
                "Invalid account status. Expected status [PROCESSING]. Found [%s]. [userKey=%s]",
                account.getActivationStatus(), userKey
            );
            throw this.buildException(AccountMessageCode.INVALID_ACCOUNT_STATUS, message, message);
        }

        account.setActivationStatus(EnumActivationStatus.COMPLETED);
        account.setActive(true);

        this.accountRepository.saveAndFlush(account);
        return account.toSimpleDto();
    }

    private void setupHomeDirectory(SimpleAccountDto accountDto) throws IOException
    {
        final String userName = accountDto.getUsername();
        final UUID userKey = accountDto.getKey();
        logger.info("Setting up home directory for user {} [key={}]", userName, userKey);

        final Path homeDir = userFileNamingStrategy.getDir(
            UserFileNamingStrategyContext.of(userName, false /*strict*/, true /*createIfNotExists*/));
        logger.info("Created home for user {} [key={}]: {}", userName, userKey, homeDir);

        // Create directory structure under home

        for (EnumUserFileReservedEntry r: EnumSet.of(
                EnumUserFileReservedEntry.NOTEBOOKS_FOLDER,
                EnumUserFileReservedEntry.QUOTA_FOLDER))
        {
            final Path d = homeDir.resolve(r.entryName());
            try {
                Files.createDirectory(d, DEFAULT_DIRECTORY_ATTRIBUTE);
            } catch (FileAlreadyExistsException ex) {
                // noop
            }
        }
    }
}
