package eu.opertusmundi.bpm.worker.subscriptions.message;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FilenameUtils;
import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.model.Parameters;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.domain.AccountEntity;
import eu.opertusmundi.common.feign.client.EmailServiceFeignClient;
import eu.opertusmundi.common.model.BaseResponse;
import eu.opertusmundi.common.model.ServiceException;
import eu.opertusmundi.common.model.email.AttachmentDto;
import eu.opertusmundi.common.model.email.EmailAddressDto;
import eu.opertusmundi.common.model.email.EnumMailType;
import eu.opertusmundi.common.model.email.MailMessageCode;
import eu.opertusmundi.common.model.email.MessageDto;
import eu.opertusmundi.common.repository.AccountRepository;
import eu.opertusmundi.common.service.messaging.MailMessageHelper;
import eu.opertusmundi.common.service.messaging.MailModelBuilder;
import feign.FeignException;

@Service
public class MailSendTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(MailSendTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.mail-send.lock-duration:120000}")
    private Long lockDurationMillis;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private MailMessageHelper messageHelper;

    @Autowired
    private ObjectProvider<EmailServiceFeignClient> mailClient;

    @Override
    public String getTopicName() {
        return "sendMail";
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
            final String              mailType      = this.getVariableAsString(externalTask, externalTaskService, "mailType");
            final UUID                mailRecipient = this.getVariableAsUUID(externalTask, externalTaskService, "mailRecipient");
            final EnumMailType        type          = EnumMailType.valueOf(mailType);
            final Map<String, Object> variables     = externalTask.getAllVariables();

            logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

            this.sendMail(type, mailRecipient, variables);

            // Complete task
            externalTaskService.complete(externalTask);

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

    private void sendMail(EnumMailType type, UUID recipientKey, Map<String, Object> variables) throws IOException {
        // Resolve recipient
        final AccountEntity account = accountRepository.findOneByKey(recipientKey).orElse(null);
        if (account == null) {
            throw new ServiceException(
                MailMessageCode.RECIPIENT_NOT_FOUND,
                String.format("Recipient was not found [userKey=%s]", recipientKey)
            );
        }

        // Compose message
        final MailModelBuilder builder = MailModelBuilder.builder()
            .add("userKey", recipientKey.toString())
            .addAll(variables);

        final Map<String, Object>             model    = this.messageHelper.createModel(type, builder);
        final EmailAddressDto                 sender   = this.messageHelper.getSender(type, model);
        final String                          subject  = this.messageHelper.composeSubject(type, model);
        final String                          template = this.messageHelper.resolveTemplate(type, model);
        final MessageDto<Map<String, Object>> message  = new MessageDto<>();

        message.setSender(sender);
        message.setSubject(subject);
        message.setTemplate(template);
        message.setModel(model);

        // Add attachments
        for (final String key : variables.keySet()) {
            // If a variable name starts with the prefix
            // `Parameters.SEND_MAIL_ATTACHMENT_PREFIX`, it is an attachment
            if (key.startsWith(Parameters.SEND_MAIL_ATTACHMENT_PREFIX)) {
                final String        path       = variables.get(key).toString();
                final AttachmentDto attachment = new AttachmentDto(
                    FilenameUtils.getName(path), Files.readAllBytes(Paths.get(path)), MediaType.APPLICATION_PDF_VALUE
                );
                message.addAttachment(attachment);
            }
        }

        message.setRecipients(builder.getAddress());

        try {
            final ResponseEntity<BaseResponse> response = this.mailClient.getObject().sendMail(message);

            if (!response.getBody().getSuccess()) {
                throw new ServiceException(
                    MailMessageCode.SEND_MAIL_FAILED,
                    String.format("Failed to send mail [userKey=%s]", recipientKey)
                );
            }
        } catch (final FeignException fex) {
            throw new ServiceException(
                MailMessageCode.SEND_MAIL_FAILED,
                String.format("Failed to send mail [userKey=%s]", recipientKey),
                fex
            );
        }
    }

}