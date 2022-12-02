package eu.opertusmundi.bpm.worker.service;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FilenameUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import eu.opertusmundi.bpm.worker.model.Parameters;
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
import eu.opertusmundi.common.util.StreamUtils;
import feign.FeignException;

@Service
public class DefaultMailService implements MailService {

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private MailMessageHelper messageHelper;

    @Autowired
    private ObjectProvider<EmailServiceFeignClient> mailClient;
    
    @Retryable(include = {ServiceException.class}, maxAttempts = 3, backoff = @Backoff(delay = 10000, maxDelay = 60000))
    public void sendMail(EnumMailType type, UUID recipientKey, Map<String, Object> variables) {
        try {
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

            final ResponseEntity<BaseResponse> response = this.mailClient.getObject().sendMail(message);

            if (!response.getBody().getSuccess()) {
                String errorMessage = StreamUtils.from(response.getBody().getMessages())
                    .map(m -> m.getDescription())
                    .findFirst()
                    .orElse(null);
                throw new ServiceException(
                    MailMessageCode.SEND_MAIL_FAILED,
                    String.format("Failed to send mail [userKey=%s, message=%s]", recipientKey, errorMessage)
                );
            }
        } catch (final FeignException fex) {
            throw new ServiceException(
                MailMessageCode.SERVICE_ERROR,
                String.format("Failed to send mail [userKey=%s]", recipientKey),
                fex
            );
        } catch (final ServiceException ex) {
            throw ex;
        } catch (final Exception ex) {
            throw new ServiceException(
                MailMessageCode.SEND_MAIL_FAILED,
                String.format("Failed to send mail [userKey=%s]", recipientKey),
                ex
            );
        }
    }

}
