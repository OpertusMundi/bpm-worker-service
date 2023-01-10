package eu.opertusmundi.bpm.worker.subscriptions.user;

import java.util.UUID;

import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import eu.opertusmundi.bpm.worker.model.ErrorCodes;
import eu.opertusmundi.common.model.ServiceException;
import eu.opertusmundi.common.model.account.EnumCustomerType;
import eu.opertusmundi.common.model.payment.UserRegistrationCommand;
import eu.opertusmundi.common.service.ProviderRegistrationService;
import eu.opertusmundi.common.service.contract.ProviderTemplateContractService;
import eu.opertusmundi.common.service.mangopay.BankAccountService;
import eu.opertusmundi.common.service.mangopay.UserService;
import eu.opertusmundi.common.service.mangopay.WalletService;

@Service
public class CreateProviderTaskService extends AbstractCustomerTaskService {

    private static final Logger logger = LoggerFactory.getLogger(CreateProviderTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.provider-registration.lock-duration:120000}")
    private Long lockDurationMillis;

    final private BankAccountService              bankAccountService;
    final private ProviderTemplateContractService providerTemplateContractService;
    final private ProviderRegistrationService     registrationService;
    final private UserService                     userService;
    final private WalletService                   walletService;

    @Autowired
    public CreateProviderTaskService(
        BankAccountService              bankAccountService,
        ProviderTemplateContractService providerTemplateContractService,
        ProviderRegistrationService     registrationService,
        UserService                     userService,
        WalletService                   walletService
    ) {
        super();

        this.bankAccountService              = bankAccountService;
        this.providerTemplateContractService = providerTemplateContractService;
        this.registrationService             = registrationService;
        this.userService                     = userService;
        this.walletService                   = walletService;
    }

    @Override
    public String getTopicName() {
        return "createProvider";
    }

    @Override
    protected long getLockDuration() {
        return this.lockDurationMillis;
    }

    @Override
    public void execute(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        try {
            final String           taskId       = externalTask.getId();
            final EnumCustomerType customerType = EnumCustomerType.PROVIDER;

            logger.info("Received task. [taskId={}]", taskId);

            final UUID                    userKey         = this.getUserKey(externalTask, externalTaskService);
            final UUID                    registrationKey = this.getRegistrationKey(externalTask, externalTaskService);
            final UserRegistrationCommand command         = UserRegistrationCommand.of(customerType, userKey, registrationKey);

            logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

            this.userService.createUser(command);
            this.walletService.createWallet(command);
            this.bankAccountService.createBankAccount(command);

            this.providerTemplateContractService.updateDefaultContracts(userKey);

            this.registrationService.completeRegistration(userKey);

            // Initial update with values from MANGOPAY
            this.walletService.refreshUserWallets(userKey, customerType);
            this.userService.updateUserBlockStatus(userKey, customerType);

            externalTaskService.complete(externalTask);

            logger.info("Completed task. [taskId={}]", taskId);
        } catch (final ServiceException ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleBpmnError(externalTaskService, externalTask, ErrorCodes.ProviderRegistration, ex);
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleFailure(externalTaskService, externalTask, ex);
        }
    }

}
