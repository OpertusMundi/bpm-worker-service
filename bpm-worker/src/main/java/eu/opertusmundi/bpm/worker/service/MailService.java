package eu.opertusmundi.bpm.worker.service;

import java.util.Map;
import java.util.UUID;

import eu.opertusmundi.common.model.email.EnumMailType;

public interface MailService {

    void sendMail(EnumMailType type, UUID recipientKey, Map<String, Object> variables);
    
}
