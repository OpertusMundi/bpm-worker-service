package eu.opertusmundi.bpm.worker.service;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.PostConstruct;

import org.passay.CharacterRule;
import org.passay.EnglishCharacterData;
import org.passay.PasswordGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.common.config.GeodataConfiguration;
import eu.opertusmundi.common.domain.AccountEntity;
import eu.opertusmundi.common.feign.client.EmailServiceFeignClient;
import eu.opertusmundi.common.model.BaseResponse;
import eu.opertusmundi.common.model.EnumAccountType;
import eu.opertusmundi.common.model.MessageCode;
import eu.opertusmundi.common.model.ServiceException;
import eu.opertusmundi.common.model.account.AccountClientCommandDto;
import eu.opertusmundi.common.model.account.AccountDto;
import eu.opertusmundi.common.model.account.AccountMessageCode;
import eu.opertusmundi.common.model.account.ConsumerIndividualCommandDto;
import eu.opertusmundi.common.model.account.EnumAccountAttribute;
import eu.opertusmundi.common.model.account.EnumActivationStatus;
import eu.opertusmundi.common.model.email.EmailAddressDto;
import eu.opertusmundi.common.model.email.EnumMailType;
import eu.opertusmundi.common.model.email.MessageDto;
import eu.opertusmundi.common.model.file.EnumUserFileReservedEntry;
import eu.opertusmundi.common.model.file.UserFileNamingStrategyContext;
import eu.opertusmundi.common.model.geodata.Shard;
import eu.opertusmundi.common.model.keycloak.server.UserDto;
import eu.opertusmundi.common.model.keycloak.server.UserQueryDto;
import eu.opertusmundi.common.repository.AccountRepository;
import eu.opertusmundi.common.service.AccountClientService;
import eu.opertusmundi.common.service.ConsumerRegistrationService;
import eu.opertusmundi.common.service.DefaultUserFileNamingStrategy;
import eu.opertusmundi.common.service.KeycloakAdminService;
import eu.opertusmundi.common.service.messaging.MailMessageHelper;
import eu.opertusmundi.common.service.messaging.MailModelBuilder;
import feign.FeignException;

@Service
@Transactional
public class DefaultAccountActivationService implements AccountActivationService {

    private static final Logger logger = LoggerFactory.getLogger(DefaultAccountActivationService.class);

    private static final String DEFAULT_OAUTH_CLIENT_ALIAS = "DEFAULT";

    private static final CharacterRule[] PASSWORD_POLICY = new CharacterRule[] {
        new CharacterRule(EnglishCharacterData.Alphabetical),
        new CharacterRule(EnglishCharacterData.LowerCase),
        new CharacterRule(EnglishCharacterData.UpperCase),
        new CharacterRule(EnglishCharacterData.Digit),
    };

    private static final PasswordGenerator PASSWORD_GENERATOR = new PasswordGenerator();

    private static final FileAttribute<Set<PosixFilePermission>> DEFAULT_DIRECTORY_ATTRIBUTE =
        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxrwxr-x"));

    private static final int MIN_PASSWORD_LENGTH = 8;

    @Value("${opertusmundi.bpm.worker.tasks.activate-account.otp-length:12}")
    private int otpLength;

    private final AccountClientService                    accountClientService;
    private final AccountRepository                       accountRepository;
    private final ConsumerRegistrationService             consumerRegistrationService;
    private final DefaultUserFileNamingStrategy           userFileNamingStrategy;
    private final GeodataConfiguration                    geodataConfiguration;
    private final KeycloakAdminService                    keycloakAdminService;
    private final MailMessageHelper                       mailMessageHelper;
    private final ObjectProvider<EmailServiceFeignClient> mailClient;

    public DefaultAccountActivationService(
        AccountClientService                    accountClientService,
        AccountRepository                       accountRepository,
        ConsumerRegistrationService             consumerRegistrationService,
        DefaultUserFileNamingStrategy           userFileNamingStrategy,
        GeodataConfiguration                    geodataConfiguration,
        KeycloakAdminService                    keycloakAdminService,
        MailMessageHelper                       mailMessageHelper,
        ObjectProvider<EmailServiceFeignClient> mailClient
    ) {
        this.accountClientService        = accountClientService;
        this.accountRepository           = accountRepository;
        this.consumerRegistrationService = consumerRegistrationService;
        this.geodataConfiguration        = geodataConfiguration;
        this.keycloakAdminService        = keycloakAdminService;
        this.mailClient                  = mailClient;
        this.mailMessageHelper           = mailMessageHelper;
        this.userFileNamingStrategy      = userFileNamingStrategy;
    }

    @PostConstruct
    void checkPostConstruct() {
        Assert.isTrue(this.otpLength >= MIN_PASSWORD_LENGTH, () -> "Expected OTP password length to be >= " + MIN_PASSWORD_LENGTH);
    }

    public AccountDto completeAccountRegistration(UUID userKey, boolean registerConsumer) throws BpmnWorkerException {
        // Verify that the account exists and has the appropriate status
        AccountDto    account  = this.verifyAccount(userKey);
        final boolean external = account.getIdpName() != null && account.getIdpName().isExternal();

        return external ? this.registerExternalAccount(account) : this.registerLocalAccount(account, registerConsumer);
    }

    private AccountDto registerLocalAccount(AccountDto account, boolean registerConsumer) {
        // Prepare home directory for the new account
        this.setupHomeDirectory(account);

        // Create OTP
        final String password = PASSWORD_GENERATOR.generatePassword(otpLength, PASSWORD_POLICY);
        // Register account to IDP (Keycloak)
        this.registerAccountToIdp(account, password);
        // Set local password
        this.accountRepository.setPassword(account.getId(), password);

        // Configure consumer registration
        if (registerConsumer) {
            this.registerConsumer(account);
        }

        // Create default OAuth2 client
        this.createDefaultOAuth2Client(account);

        // Complete account registration
        account = this.activateAccount(account.getKey());

        // Send OTP to user
        this.sendMail(account, password);

        return account;
    }

    private AccountDto registerExternalAccount(AccountDto account) {
        // Prepare home directory for the new account
        this.setupHomeDirectory(account);

        // Create default OAuth2 client
        this.createDefaultOAuth2Client(account);

        // Complete account registration
        account = this.activateAccount(account.getKey());

        return account;
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
    private AccountDto verifyAccount(UUID userKey) throws BpmnWorkerException {
        final AccountDto account = this.accountRepository.findOneByKeyObject(userKey).orElse(null);

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

        return account;
    }

    /**
     * Setup user home directory and initialize directory structure. If any
     * directory exists, it is not created
     *
     * @param accountDto
     * @throws BpmnWorkerException
     */
    private void setupHomeDirectory(AccountDto account) throws BpmnWorkerException {
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
    private void registerAccountToIdp(AccountDto account, String password) throws BpmnWorkerException {
        Assert.notNull(account, "Expected a non-null account");
        Assert.hasText(password, "Expected a non-empty password");

        final String userName  = account.getUsername();
        final String userEmail = userName;
        final UUID   userKey   = account.getKey();

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
                user.setEmailVerified(true);
                user.setEnabled(true);
                // Add opertusmundi-specific attributes (accountType etc.)
                user.setAttributes(
                    Collections.singletonMap(EnumAccountAttribute.ACCOUNT_TYPE.key(), new String[]{account.getType().name()})
                );
                UUID userId = keycloakAdminService.createUser(user);
                logger.info("Created user [{}] on the IDP [id={}]", userName, userId);
                user = keycloakAdminService.getUser(userId).get();
            } else {
                // The user is present; just retrieve first of singleton result
                user = usersForUsername.get(0);
            }

            Assert.state(user.getId() != null, "expected a non-null user identifier (from the IDP side)");
            keycloakAdminService.resetPasswordForUser(user.getId(), password, true /* temporary */);
            logger.info("The user [{}] is reset to have a temporary (OTP) password [id={}]", userName, user.getId());
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
    private void sendMail(AccountDto account, String password) throws BpmnWorkerException {
        Assert.notNull(account, "Expected a non-null account");
        Assert.notNull(password, "Expected a non-empty password");

        final String userName = account.getUsername();
        final UUID   userKey  = account.getKey();

        try {
            final EnumMailType type = account.getType() == EnumAccountType.OPERTUSMUNDI
                ? EnumMailType.ACCOUNT_ACTIVATION_SUCCESS
                : EnumMailType.VENDOR_ACCOUNT_ACTIVATION_SUCCESS;

            final MailModelBuilder builder = MailModelBuilder.builder()
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
     * Initialize consumer registration for the specified account
     *
     * @param account
     */
    private void registerConsumer(AccountDto account) {
        // Do not initialize a consumer registration workflow instance. The
        // registration task will be executed as part of the current
        // workflow
        ConsumerIndividualCommandDto registrationCommand = ConsumerIndividualCommandDto.builder()
            .email(account.getEmail())
            .firstName(account.getProfile().getFirstName())
            .lastName(account.getProfile().getLastName())
            .userId(account.getId())
            .workflowInstanceRequired(false)
            .build();

        this.consumerRegistrationService.submitRegistration(registrationCommand);
    }

    /**
     * Select a geodata shard for the specified user key
     *
     * @param userKey
     * @return
     */
    private String selectGeodataShard(UUID userKey) {
        final AccountDto user  = this.accountRepository.findOneByKeyObject(userKey).get();
        String           shard = null;

        if (user.getParentKey() != null) {
            // If this is a vendor account i.e. parent key is not null, we set
            // the geodata shard to the parent's value to make the private OGC
            // services of the new account visible to all organization users
            final AccountDto parentUser = this.accountRepository.findOneByKeyObject(user.getParentKey()).get();
            shard = parentUser.getProfile().getGeodataShard();
        } else {
            final List<Shard> shards     = Optional.ofNullable(this.geodataConfiguration.getShards()).orElse(Collections.emptyList());
            final int         shardCount = shards.size();
            shard = shardCount == 0 ? null : shards.get(ThreadLocalRandom.current().nextInt(shardCount)).getId();
        }

        return shard;
    }

    /**
     * Activate account
     *
     * @param userKey
     * @return
     * @throws BpmnWorkerException
     */
    private AccountDto activateAccount(UUID userKey) throws BpmnWorkerException {
        final String geodataShard = this.selectGeodataShard(userKey);

        AccountEntity account = this.accountRepository.findOneByKey(userKey).orElse(null);
        account.setActivationStatus(EnumActivationStatus.COMPLETED);
        account.setActive(true);
        account.getProfile().setGeodataShard(geodataShard);
        account = this.accountRepository.saveAndFlush(account);

        return account.toDto(true);
    }

    private void createDefaultOAuth2Client(AccountDto account) {
        try {
            final var clientCommand = AccountClientCommandDto.builder()
                .alias(DEFAULT_OAUTH_CLIENT_ALIAS)
                .clientId(Optional.of(account.getKey()))
                .accountId(account.getId())
                .build();
            this.accountClientService.create(clientCommand);
        } catch (ServiceException ex) {
            if (ex.getCode() == AccountMessageCode.ACCOUNT_CLIENT_NOT_UNIQUE_ALIAS) {
                // Ignore error if default client already exists
                return;
            }
            throw ex;
        }
    }

    private BpmnWorkerException buildException(
        MessageCode code, String message, String errorDetails
    ) {
        return  BpmnWorkerException.builder()
        .code(code)
        .message(message)
        .errorDetails(errorDetails)
        .retries(0)
        .retryTimeout(0L)
        .build();
    }
}
