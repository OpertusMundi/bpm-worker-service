package eu.opertusmundi.bpm.worker.subscriptions.message;

import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.domain.AccountEntity;
import eu.opertusmundi.common.feign.client.EmailServiceFeignClient;
import eu.opertusmundi.common.model.BaseResponse;
import eu.opertusmundi.common.model.ServiceException;
import eu.opertusmundi.common.model.email.EmailAddressDto;
import eu.opertusmundi.common.model.email.EnumMailType;
import eu.opertusmundi.common.model.email.MailMessageCode;
import eu.opertusmundi.common.model.email.MessageDto;
import eu.opertusmundi.common.repository.AccountRepository;
import eu.opertusmundi.common.service.messaging.MailMessageHelper;
import feign.FeignException;

@Service
public class MailSendTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(MailSendTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.mail-send.lock-duration:10000}")
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
            final UUID                mailRecipient = this.getVariableAsUUID(externalTaskService, externalTask, "mailRecipient");
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
        } catch (final ServiceException ex) {
            logger.error(String.format("Operation has failed. [details=%s]", ex.getMessage()), ex);

            externalTaskService.handleFailure(
                externalTask, ex.getMessage(), ex.getMessage(), DEFAULT_RETRY_COUNT, DEFAULT_RETRY_TIMEOUT
            );
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleError(externalTaskService, externalTask, ex);
        }
    }

    private void sendMail(EnumMailType type, UUID recipientKey, Map<String, Object> variables) {
        // Resolve recipient
        final AccountEntity account = accountRepository.findOneByKey(recipientKey).orElse(null);
        if (account == null) {
            throw new ServiceException(
                MailMessageCode.RECIPIENT_NOT_FOUND,
                String.format("Recipient was not found [userKey=%s]", recipientKey)
            );
        }

        // Compose message
        final Map<String, Object>             model    = this.messageHelper.createModel(type, variables);
        final EmailAddressDto                 sender   = this.messageHelper.getSender(type, model);
        final String                          subject  = this.messageHelper.composeSubject(type, model);
        final String                          template = this.messageHelper.resolveTemplate(type, model);
        final MessageDto<Map<String, Object>> message  = new MessageDto<>();

        message.setSender(sender);
        message.setSubject(subject);
        message.setTemplate(template);
        message.setModel(model);

        if (StringUtils.isBlank(account.getFirstName())) {
            message.setRecipients(account.getEmail());
        } else {
            message.setRecipients(account.getEmail(), account.getFirstName());
        }

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