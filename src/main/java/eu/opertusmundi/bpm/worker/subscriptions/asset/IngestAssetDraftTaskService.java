package eu.opertusmundi.bpm.worker.subscriptions.asset;

import java.nio.file.Path;

import org.apache.commons.lang3.StringUtils;
import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.model.IngestServiceException;
import eu.opertusmundi.common.model.file.FilePathCommand;
import eu.opertusmundi.common.model.ingest.ClientEndpointsDto;
import eu.opertusmundi.common.model.ingest.ClientStatusDto;
import eu.opertusmundi.common.model.ingest.IngestServiceMessageCode;
import eu.opertusmundi.common.model.ingest.ServerIngestDeferredResponseDto;
import eu.opertusmundi.common.service.FileManager;
import eu.opertusmundi.common.service.IngestService;

@Service
public class IngestAssetDraftTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(IngestAssetDraftTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.ingest-asset-draft.lock-duration:60000}")
    private Long lockDurationMillis;

    @Autowired
    private FileManager fileManager;

    @Override
    public String getTopicName() {
        return "ingestDraft";
    }

    @Override
    protected long getLockDuration() {
        return this.lockDurationMillis;
    }

    @Autowired
    private IngestService ingestService;

    @Override
    public void execute(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        try {
            final String taskId = externalTask.getId();

            logger.info("Received task {}", taskId);

            // Get user id key
            final String userId = (String) externalTask.getVariable("userId");
            if (StringUtils.isBlank(userId)) {
                logger.error("Expected user id to be non empty!");

                externalTaskService.handleFailure(externalTask, "User id is empty!", null, 0, 0);
                return;
            }


            // Get draft key
            final String draftKey = (String) externalTask.getVariable("draftKey");
            if (StringUtils.isBlank(draftKey)) {
                logger.error("Expected draft key to be non empty!");

                externalTaskService.handleFailure(externalTask, "Draft key is empty!", null, 0, 0);
                return;
            }

            // Get publisher key
            final String publisherKey = (String) externalTask.getVariable("publisherKey");
            if (StringUtils.isBlank(publisherKey)) {
                logger.error("Expected publisher key to be non empty!");

                externalTaskService.handleFailure(externalTask, "Publisher key is empty!", null, 0, 0);
                return;
            }

            // Get source
            final String source = (String) externalTask.getVariable("source");
            if (StringUtils.isBlank(source)) {
                logger.error("Expected source to be non empty!");

                externalTaskService.handleFailure(externalTask, "Source is empty!", null, 0, 0);
                return;
            }
            
            // Resolve source
            final FilePathCommand command = FilePathCommand.builder()
                .userId(Integer.parseInt(userId))
                .path(source)
                .build();

            final Path path = this.fileManager.resolveFilePath(command);

            logger.debug("Processing task {}: {}", taskId, externalTask);

            final ServerIngestDeferredResponseDto ingestResponse = this.ingestService.ingestAsync(path.toString());
            final String                          ticket         = ingestResponse.getTicket();
            ClientStatusDto                       result         = null;
            int                                   counter        = 0;

            // TODO: Add a parameter for preventing infinite loops
            while (counter++ < 100) {
                // NOTE: Polling time must be less than lock duration
                Thread.sleep(30000);

                result = this.ingestService.getStatus(ticket);
                if (result.isCompleted()) {
                    break;
                }

                // Extend lock duration
                externalTaskService.extendLock(externalTask, this.getLockDuration());
            }

            if (result != null && result.isCompleted() && result.isSuccess()) {
                // Update metadata
                final ClientEndpointsDto endpointsResponse = this.ingestService.getEndpoints(ticket);

                // TODO: Update endpoints
                logger.warn(endpointsResponse.toString());
            } else {
                throw new IngestServiceException(IngestServiceMessageCode.SERVICE_ERROR,
                    String.format("Ingest operation [%s] has failed", ticket)
                );
            }

            // Complete task
            externalTaskService.complete(externalTask);

            logger.info("Completed task {}", taskId);
        } catch (final IngestServiceException ex) {
            logger.error("[INGEST Service] Operation has failed", ex);

            externalTaskService.handleFailure(externalTask, "[INGEST Service] Operation has failed", null, 0, 0);
        } catch (final Exception ex) {
            logger.error("Unhandled error has occurred", ex);

            final int  retryCount   = 0;
            final long retryTimeout = 2000L;

            externalTaskService.handleFailure(externalTask, "Unhandled error has occurred", ex.getMessage(), retryCount, retryTimeout);
        }
    }

}