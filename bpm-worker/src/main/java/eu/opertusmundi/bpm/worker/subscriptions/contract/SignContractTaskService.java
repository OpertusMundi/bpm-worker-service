package eu.opertusmundi.bpm.worker.subscriptions.contract;

import java.util.UUID;

import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.domain.AccountEntity;
import eu.opertusmundi.common.domain.OrderEntity;
import eu.opertusmundi.common.domain.PayInEntity;
import eu.opertusmundi.common.domain.PayInItemEntity;
import eu.opertusmundi.common.domain.PayInOrderItemEntity;
import eu.opertusmundi.common.model.contract.consumer.ConsumerContractCommand;
import eu.opertusmundi.common.model.payment.PaymentException;
import eu.opertusmundi.common.model.payment.PaymentMessageCode;
import eu.opertusmundi.common.repository.PayInRepository;
import eu.opertusmundi.common.service.contract.ConsumerContractService;

@Service
public class SignContractTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(SignContractTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.sign-order-contract.lock-duration:120000}")
    private Long lockDurationMillis;

    @Autowired
    private PayInRepository payInRepository;
    
    @Autowired
    private ConsumerContractService contractService;

    @Override
    public String getTopicName() {
        return "signOrderContract";
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

            // Get parameters
            final UUID        payInKey = UUID.fromString(externalTask.getBusinessKey());
            final PayInEntity payIn    = payInRepository.findOneEntityByKey(payInKey).orElse(null);

            Assert.notNull(payIn, "Expected a non-null PayIn");
            Assert.isTrue(payIn.getItems().size() == 1, "Expected PayIn with a single item");
            
            logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

            // Update account profile
            for (final PayInItemEntity item : payIn.getItems()) {
                switch (item.getType()) {
                    case ORDER : {
                        final PayInOrderItemEntity orderItem = (PayInOrderItemEntity) item;
                        final AccountEntity        consumer  = payIn.getConsumer();
                        final OrderEntity          order     = orderItem.getOrder();

                        // Paths will be resolved by the contract service
                        final ConsumerContractCommand command = ConsumerContractCommand.builder()
                            .userId(consumer.getId())
                            .orderKey(order.getKey())
                            .itemIndex(1)
                            .build();
                        
                        

                        this.contractService.sign(command);
                        break;
                    }
                    
                    default :
                        throw new PaymentException(PaymentMessageCode.PAYIN_ITEM_TYPE_NOT_SUPPORTED, String.format(
                            "PayIn item type not supported [payIn=%s, index=%d, type=%s]",
                            payInKey, item.getIndex(), item.getType()
                        ));
                }
            }
            
            // Complete task
            externalTaskService.complete(externalTask);

            logger.info("Completed task. [taskId={}]", taskId);
        } catch (final BpmnWorkerException ex) {
            logger.error(String.format("Operation has failed. [details=%s]", ex.getErrorDetails()), ex);

            externalTaskService.handleFailure(externalTask, ex.getMessage(), ex.getErrorDetails(), ex.getRetries(), ex.getRetryTimeout());
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleError(externalTaskService, externalTask, ex);
        }
    }

}