package eu.opertusmundi.bpm.worker.subscriptions.asset;

import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.springframework.stereotype.Service;

import eu.opertusmundi.bpm.worker.support.BaseIngestTaskService;

@Service
public class IngestAssetDraftTaskService extends BaseIngestTaskService {

    @Override
    public String getTopicName() {
        return "ingestDraft";
    }

    @Override
    protected final String getUserIdVariableName(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        return "userId";
    }

    @Override
    protected final String getSourceVariableName(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        return "source";
    }

}