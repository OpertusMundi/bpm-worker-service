package eu.opertusmundi.bpm.worker.subscriptions.user;

import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.stereotype.Service;

import eu.opertusmundi.bpm.worker.model.ErrorCodes;
import eu.opertusmundi.common.model.ServiceException;
import eu.opertusmundi.common.model.account.AccountDto;
import eu.opertusmundi.common.repository.AccountRepository;
import eu.opertusmundi.common.service.contract.ProviderTemplateContractService;

@Service
public class SetDefaultProviderContractTaskService extends AbstractCustomerTaskService {

    private static final Logger logger = LoggerFactory.getLogger(SetDefaultProviderContractTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.set-provider-default-contract.lock-duration:120000}")
    private Long lockDurationMillis;

    final private AccountRepository               accountRepository;
    final private ProviderTemplateContractService providerTemplateContractService;

    @Autowired
    public SetDefaultProviderContractTaskService(
        AccountRepository accountRepository,
        ProviderTemplateContractService providerTemplateContractService
    ) {
        super();

        this.accountRepository               = accountRepository;
        this.providerTemplateContractService = providerTemplateContractService;
    }

    @Override
    public String getTopicName() {
        return "setDefaultProviderContract";
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
            logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);


            final Direction direction   = Direction.ASC;
            PageRequest     pageRequest = PageRequest.of(0, 10, Sort.by(direction, "id"));
            var             accounts    = this.accountRepository.findAllProvidersObjects(null, pageRequest, false);

            while (!accounts.isEmpty()) {
                for (final AccountDto a : accounts.getContent()) {
                    this.providerTemplateContractService.createDefaultContract(a.getKey());
                }
                // Extend lock duration
                externalTaskService.extendLock(externalTask, this.getLockDuration());

                pageRequest = pageRequest.next();
                accounts    = this.accountRepository.findAllProvidersObjects(null, pageRequest, false);
            }

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
