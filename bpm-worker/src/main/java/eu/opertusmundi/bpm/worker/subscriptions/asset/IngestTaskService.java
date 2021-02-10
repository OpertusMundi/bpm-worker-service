package eu.opertusmundi.bpm.worker.subscriptions.asset;

import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.springframework.stereotype.Service;

import eu.opertusmundi.bpm.worker.support.BaseIngestTaskService;

@Service
public class IngestTaskService extends BaseIngestTaskService {

    @Override
    public String getTopicName() {
        return "ingestDraft";
    }

    @Override
    protected String getPublisherKeyVariableName(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        return "publisherKey";
    }
    
    @Override
    protected String getAssetKeyVariableName(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        return "draftKey";
    }

}