package eu.opertusmundi.bpm.worker.subscriptions.billing;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.model.BasicMessageCode;
import eu.opertusmundi.common.model.ServiceException;
import eu.opertusmundi.common.model.account.AccountDto;
import eu.opertusmundi.common.model.payment.ServiceBillingDto;
import eu.opertusmundi.common.model.payment.ServiceBillingBatchDto;
import eu.opertusmundi.common.repository.AccountRepository;
import eu.opertusmundi.common.service.ServiceBillingService;

@Service
public class MonthlyServiceBillingTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(MonthlyServiceBillingTaskService.class);

    private static final String PARAMETER_YEAR  = "year";
    private static final String PARAMETER_MONTH = "month";

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private ServiceBillingService serviceBillingService;

    @Value("${opertusmundi.bpm.worker.tasks.monthly-subscription-billing.lock-duration:300000}")
    private Long lockDurationMillis;

    @Value("${opertusmundi.bpm.worker.tasks.monthly-subscription-billing.batch-size:20}")
    private int batchSize;

    @Override
    public String getTopicName() {
        return "monthlyServiceBilling";
    }

    @Override
    protected long getLockDuration() {
        return this.lockDurationMillis;
    }

    @Override
    public void execute(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        final String  taskId      = externalTask.getId();
        final UUID    businessKey = UUID.fromString(externalTask.getBusinessKey());
        final Integer year        = this.getVariableAsInteger(externalTask, externalTaskService, PARAMETER_YEAR);
        final Integer month       = this.getVariableAsInteger(externalTask, externalTaskService, PARAMETER_MONTH);

        try {
            logger.info("Received task. [taskId={}]", taskId);

            final ServiceBillingBatchDto batch = this.serviceBillingService.findOneBillingIntervalByKey(businessKey).orElse(null);
            if (batch == null) {
                throw new ServiceException(BasicMessageCode.RecordNotFound, String.format(
                    "Subscription billing batch was not found [businessKey=%s]", businessKey
                ));
            }

            logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

            int              pageIndex              = 0;
            int              totalSubscriptions     = 0;
            BigDecimal       totalPrice             = BigDecimal.ZERO;
            BigDecimal       totalPriceExcludingTax = BigDecimal.ZERO;
            BigDecimal       totalTax               = BigDecimal.ZERO;
            Page<AccountDto> page                   = accountRepository.findAllObjects(null, PageRequest.of(pageIndex, batchSize), false);

            while (!page.isEmpty()) {
                for (final AccountDto account : page.getContent()) {
                    final List<ServiceBillingDto> result = this.serviceBillingService.create(
                        account.getKey(), year, month, false
                    );
                    totalSubscriptions += result.size();
                    for (final ServiceBillingDto b : result) {
                        totalPrice             = totalPrice.add(b.getTotalPrice());
                        totalPriceExcludingTax = totalPriceExcludingTax.add(b.getTotalPriceExcludingTax());
                        totalTax               = totalTax.add(b.getTotalTax());
                    } ;
                }

                // Extend lock duration
                externalTaskService.extendLock(externalTask, this.getLockDuration());

                pageIndex++;
                page = accountRepository.findAllConsumersObjects(null, PageRequest.of(pageIndex, batchSize), false);
            }

            this.serviceBillingService.complete(businessKey, totalSubscriptions, totalPrice, totalPriceExcludingTax, totalTax);

            // Complete task
            externalTaskService.complete(externalTask);

            logger.info("Completed task. [taskId={}]", taskId);
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);
            
            this.serviceBillingService.fail(businessKey);
            
            this.handleFailure(externalTaskService, externalTask, ex);
        }
    }

}