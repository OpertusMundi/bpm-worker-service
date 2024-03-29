package eu.opertusmundi.bpm.worker.subscriptions.invoice;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.model.Parameters;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.model.payment.EnumInvoiceType;
import eu.opertusmundi.common.repository.PayInRepository;
import eu.opertusmundi.common.service.invoice.InvoiceGeneratorService;
import eu.opertusmundi.common.util.BpmInstanceVariablesBuilder;

@Service
public class CreateInvoiceTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(CreateInvoiceTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.create-invoice.lock-duration:120000}")
    private Long lockDurationMillis;

    @Autowired
    private PayInRepository payInRepository;

    @Autowired
    private InvoiceGeneratorService invoiceGeneratorService;

    @Override
    public String getTopicName() {
        return "createInvoice";
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
            final var invoiceType      = this.getVariableAsString(externalTask, externalTaskService, "invoiceType", "ORDER_INVOICE");
            final var invoiceTypeValue = EnumInvoiceType.valueOf(invoiceType);
            final var payInKey         = this.resolvePayInKey(externalTask, externalTaskService, invoiceTypeValue);

            Map<String, Object> variables = new HashMap<>();

            logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);


            // Create invoice
            final String invoice = this.invoiceGeneratorService.generateInvoicePdf(invoiceTypeValue, payInKey);

            // Complete task
            variables = BpmInstanceVariablesBuilder.builder()
                .variableAsString(Parameters.SEND_MAIL_ATTACHMENT_PREFIX + "Invoice", invoice)
                .buildValues();

            externalTaskService.complete(externalTask, variables);

            logger.info("Completed task. [taskId={}]", taskId);
        } catch (final BpmnWorkerException ex) {
            logger.error(String.format("Operation has failed. [details=%s]", ex.getErrorDetails()), ex);

            externalTaskService.handleFailure(
                externalTask, ex.getMessage(), ex.getErrorDetails(), ex.getRetries(), ex.getRetryTimeout()
            );
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleFailure(externalTaskService, externalTask, ex);
        }
    }
    
    private UUID resolvePayInKey(ExternalTask externalTask, ExternalTaskService externalTaskService, EnumInvoiceType invoiceType) {
        return switch (invoiceType) {
            case ORDER_INVOICE, 
                 SERVICE_BILLING_INVOICE -> UUID.fromString(externalTask.getBusinessKey());
            case REFUND_INVOICE          -> {
                final var businessKey = externalTask.getBusinessKey();
                final var tokens      = businessKey.split("::");
                final var refundId    = tokens[2];
                final var payInKey    = this.payInRepository.findOneByRefundTransactionId(refundId).get().getKey();
                yield payInKey;
            }
        };

    }

}