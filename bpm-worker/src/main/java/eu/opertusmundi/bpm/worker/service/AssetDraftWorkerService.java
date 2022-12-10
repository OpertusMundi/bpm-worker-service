package eu.opertusmundi.bpm.worker.service;

import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

public interface AssetDraftWorkerService {

    void cancelPublishAsset(ExternalTask externalTask, ExternalTaskService externalTaskService) throws JsonMappingException, JsonProcessingException;

    void cancelPublishUserService(ExternalTask externalTask, ExternalTaskService externalTaskService) throws JsonMappingException, JsonProcessingException;
}
