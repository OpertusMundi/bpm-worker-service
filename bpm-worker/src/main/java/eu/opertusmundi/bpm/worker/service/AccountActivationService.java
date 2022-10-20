package eu.opertusmundi.bpm.worker.service;

import java.util.UUID;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.common.model.account.AccountDto;

public interface AccountActivationService {

    AccountDto completeAccountRegistration(UUID userKey, boolean registerConsumer) throws BpmnWorkerException;

}
