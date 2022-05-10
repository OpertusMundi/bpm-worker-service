package eu.opertusmundi.bpm.worker.subscriptions.user;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.annotation.PostConstruct;

import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.passay.CharacterRule;
import org.passay.EnglishCharacterData;
import org.passay.PasswordGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.domain.AccountEntity;
import eu.opertusmundi.common.feign.client.EmailServiceFeignClient;
import eu.opertusmundi.common.model.BaseResponse;
import eu.opertusmundi.common.model.account.AccountMessageCode;
import eu.opertusmundi.common.model.account.EnumActivationStatus;
import eu.opertusmundi.common.model.account.SimpleAccountDto;
import eu.opertusmundi.common.model.email.EmailAddressDto;
import eu.opertusmundi.common.model.email.EnumMailType;
import eu.opertusmundi.common.model.email.MessageDto;
import eu.opertusmundi.common.model.file.EnumUserFileReservedEntry;
import eu.opertusmundi.common.model.file.UserFileNamingStrategyContext;
import eu.opertusmundi.common.model.keycloak.server.UserDto;
import eu.opertusmundi.common.model.keycloak.server.UserQueryDto;
import eu.opertusmundi.common.model.account.EnumAccountAttribute;
import eu.opertusmundi.common.repository.AccountRepository;
import eu.opertusmundi.common.service.DefaultUserFileNamingStrategy;
import eu.opertusmundi.common.service.KeycloakAdminService;
import eu.opertusmundi.common.service.messaging.MailMessageHelper;
import eu.opertusmundi.common.service.messaging.MailModelBuilder;

import feign.FeignException;

@Service
public class ActivateAccountTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(ActivateAccountTaskService.class);

    private static final String VARIABLE_USER_KEY = "userKey";
    
    private static final int MIN_PASSWORD_LENGTH = 8;
    
    private static final CharacterRule[] PASSWORD_POLICY = new CharacterRule[] {
        // note: do not include special characters as a password will be emailed into an HTML body
        new CharacterRule(EnglishCharacterData.Alphabetical),
        new CharacterRule(EnglishCharacterData.LowerCase),
        new CharacterRule(EnglishCharacterData.UpperCase),
        new CharacterRule(EnglishCharacterData.Digit),
    };
    
    private static final PasswordGenerator PASSWORD_GENERATOR = new PasswordGenerator();
    
    private static final FileAttribute<Set<PosixFilePermission>> DEFAULT_DIRECTORY_ATTRIBUTE =
        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxrwxr-x"));

    @Value("${opertusmundi.bpm.worker.tasks.activate-account.lock-duration:120000}")
    private Long lockDurationMillis;
    
    @Value("${opertusmundi.bpm.worker.tasks.activate-account.otp-length:12}")
    private int otpLength;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private DefaultUserFileNamingStrategy userFileNamingStrategy;

    @Autowired
    private MailMessageHelper mailMessageHelper;

    @Autowired
    private ObjectProvider<EmailServiceFeignClient> mailClient;

    @Autowired
    private KeycloakAdminService keycloakAdminService;

    @PostConstruct
    void checkPostConstruct()
    {
        Assert.isTrue(this.otpLength >= MIN_PASSWORD_LENGTH, 
            () -> "Expected OTP password length to be >= " + MIN_PASSWORD_LENGTH);
    }
    
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
            this.completeAccountRegistration(userKey);

            externalTaskService.complete(externalTask);
            logger.info("Completed task. [taskId={}]", taskId);
        } catch (final BpmnWorkerException ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);
            externalTaskService.handleFailure(
                externalTask, ex.getMessage(), ex.getErrorDetails(), ex.getRetries(), ex.getRetryTimeout()
            );
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);
            this.handleFailure(externalTaskService, externalTask, ex);
        }
    }

    @Transactional
    public void completeAccountRegistration(UUID userKey) throws BpmnWorkerException {
        // Verify that the account exists and has the appropriate status
        final SimpleAccountDto account = this.verifyAccount(userKey);

        // Prepare home directory for the new account
        this.setupHomeDirectory(account);

        // Create OTP
        final String password = PASSWORD_GENERATOR.generatePassword(otpLength, PASSWORD_POLICY);

        // Register account to IDP (Keycloak)
        this.registerAccountToIdp(account, password);

        // Complete account registration
        this.activateAccount(userKey);
        
        // Send OTP to user
        this.sendMail(account, password);
    }

    /**
     * Check that an account already exists for the specified key and its status
     * is {@link EnumActivationStatus#PROCESSING}
     *
     *
     * @param userKey
     * @return
     * @throws BpmnWorkerException
     */
    private SimpleAccountDto verifyAccount(UUID userKey) throws BpmnWorkerException {
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

        return account.toSimpleDto();
    }

    /**
     * Setup user home directory and initialize directory structure. If any
     * directory exists, it is not created
     *
     * @param accountDto
     * @throws BpmnWorkerException
     */
    private void setupHomeDirectory(SimpleAccountDto account) throws BpmnWorkerException {
        Assert.notNull(account, "Expected a non-null account");

        final String userName = account.getUsername();
        final UUID   userKey  = account.getKey();

        try {
            logger.info("Setting up home directory for user {} [key={}]", userName, userKey);

            final Path homeDir = userFileNamingStrategy.getDir(
                UserFileNamingStrategyContext.of(userName, false /*strict*/, true /*createIfNotExists*/)
            );

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
        } catch (final IOException ex) {
            final String message = String.format(
                "Failed to initialize user home directory. [userKey=%s, userName=%s]",
                userKey, userName
            );
            throw this.buildException(AccountMessageCode.IO_ERROR, message, ex.getMessage());
        }
    }

    /**
     * Registers a new account to the IDP
     *
     * @param account
     * @param password
     * @throws BpmnWorkerException
     */
    private void registerAccountToIdp(SimpleAccountDto account, String password) throws BpmnWorkerException {
        Assert.notNull(account, "Expected a non-null account");
        Assert.hasText(password, "Expected a non-empty password");

        final String userName = account.getUsername();
        final String userEmail = userName;
        final UUID userKey  = account.getKey();
        
        Assert.hasText(userName, "Expected an non-empty username");
        logger.info("Setting up IDP account for user {} [key={}]", userName, userKey);
        
        final UserQueryDto queryForUsername = new UserQueryDto();
        queryForUsername.setUsername(userName);
        queryForUsername.setExact(true);
        
        try {
            final List<UserDto> usersForUsername = keycloakAdminService.findUsers(queryForUsername);
            Assert.state(usersForUsername.size() < 2,
                () -> "expected no more than one IDP user for a given username [username=" + userName + "]");
    
            UserDto user = null;
            if (usersForUsername.isEmpty()) {
                // Create the user
                user = new UserDto();
                user.setUsername(userName);
                user.setEmail(userEmail);
                // Add opertusmundi-specific attributes (accountType etc.)
                user.setAttributes(Collections.singletonMap(
                    EnumAccountAttribute.ACCOUNT_TYPE.key(), new String[] { account.getType().name() }));
                UUID userId = keycloakAdminService.createUser(user);
                logger.info("Created user [{}] on the IDP [id={}]", userName, userId);
                user = keycloakAdminService.getUser(userId).get();
            } else {
                // The user is present; just retrieve first of singleton result
                user = usersForUsername.get(0);
            }
    
            Assert.state(user.getId() != null, "expected a non-null user identifier (from the IDP side)");
            keycloakAdminService.resetPasswordForUser(user.getId(), password, true /* temporary */);
            logger.info("The user [{}] is reset to have a temporary (OTP) password [id={}]", 
                userName, user.getId());
            
        } catch (final Exception ex) {
            final String message = String.format(
                "Failed to create new IDP account. [userKey=%s, userName=%s]",
                userKey, userName
            );
            throw this.buildException(AccountMessageCode.IDP_OPERATION_ERROR, message, ex.getMessage());
        }
    }

    /**
     * Send email with the OTP
     *
     * @param account
     * @param password
     * @throws BpmnWorkerException
     */
    private void sendMail(SimpleAccountDto account, String password) throws BpmnWorkerException {
        Assert.notNull(account, "Expected a non-null account");
        Assert.notNull(password, "Expected a non-empty password");

        final String userName = account.getUsername();
        final UUID   userKey  = account.getKey();
        
        try {
            final EnumMailType     type     = EnumMailType.ACCOUNT_ACTIVATION_SUCCESS;
            final MailModelBuilder builder  = MailModelBuilder.builder()
                .add("userKey", userKey.toString())
                .add("otp", password);
    
            final Map<String, Object>             model    = this.mailMessageHelper.createModel(type, builder);
            final EmailAddressDto                 sender   = this.mailMessageHelper.getSender(type, model);
            final String                          subject  = this.mailMessageHelper.composeSubject(type, model);
            final String                          template = this.mailMessageHelper.resolveTemplate(type, model);
            final MessageDto<Map<String, Object>> message  = new MessageDto<>();
    
            message.setSender(sender);
            message.setSubject(subject);
            message.setTemplate(template);
            message.setModel(model);
    
            message.setRecipients(builder.getAddress());

            final ResponseEntity<BaseResponse> response = this.mailClient.getObject().sendMail(message);

            if (!response.getBody().getSuccess()) {
                final String serviceMessage = String.format(
                    "Failed to send OTP mail. [userKey=%s, userName=%s]",
                    userKey, userName
                );
                throw this.buildException(AccountMessageCode.MAIL_OPERATION_ERROR, serviceMessage, serviceMessage);
            }
        } catch (final FeignException fex) {
            final String clientMessage = String.format(
                "Mail service operation has failed. [userKey=%s, userName=%s]",
                userKey, userName
            );
            throw this.buildException(AccountMessageCode.FEIGN_CLIENT_ERROR, clientMessage, fex.getMessage());
        }
    }

    /**
     * Activate account
     *
     * @param userKey
     * @return
     * @throws BpmnWorkerException
     */
    private void activateAccount(UUID userKey) throws BpmnWorkerException {
        final AccountEntity account = this.accountRepository.findOneByKey(userKey).orElse(null);

        account.setActivationStatus(EnumActivationStatus.COMPLETED);
        account.setActive(true);

        this.accountRepository.saveAndFlush(account);
    }
}
