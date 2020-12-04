package eu.opertusmundi.bpm.worker.support;

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
import eu.opertusmundi.common.model.file.FilePathCommand;
import eu.opertusmundi.common.model.file.FileSystemException;
import eu.opertusmundi.common.model.ingest.IngestServiceException;
import eu.opertusmundi.common.model.ingest.IngestServiceMessageCode;
import eu.opertusmundi.common.model.ingest.ServerIngestDeferredResponseDto;
import eu.opertusmundi.common.model.ingest.ServerIngestEndpointsResponseDto;
import eu.opertusmundi.common.model.ingest.ServerIngestStatusResponseDto;
import eu.opertusmundi.common.service.FileManager;
import eu.opertusmundi.common.service.IngestService;

@Service
public abstract class BaseIngestTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(BaseIngestTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.ingest.lock-duration:60000}")
    private Long lockDurationMillis;

    @Override
    protected long getLockDuration() {
        return this.lockDurationMillis;
    }

    @Autowired
    private FileManager fileManager;

    @Autowired
    private IngestService ingestService;

    @Override
    public final void execute(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        try {
            final String taskId = externalTask.getId();

            logger.info("Received task {}", taskId);

            this.preExecution(externalTask, externalTaskService);

            logger.debug("Processing task {}: {}", taskId, externalTask);

            final String                          source         = this.getSource(externalTask, externalTaskService);
            final ServerIngestDeferredResponseDto ingestResponse = this.ingestService.ingestAsync(source);
            final String                          ticket         = ingestResponse.getTicket();
            ServerIngestStatusResponseDto         result         = null;
            int                                   counter        = 0;

            // TODO: Add a parameter for preventing infinite loops
            while (counter++ < 100) {
                // NOTE: Polling time must be less than lock duration
                Thread.sleep(30000);

                // Extend lock duration
                externalTaskService.extendLock(externalTask, this.getLockDuration());

                result = this.ingestService.getStatus(ticket);
                if (result.isCompleted()) {
                    break;
                }
            }

            if (result != null && result.isCompleted() && result.isSuccess()) {
                // GET endpoints
                final ServerIngestEndpointsResponseDto endpointsResponse = this.ingestService.getEndpoints(ticket);

                // TODO: Update endpoints
                logger.warn(endpointsResponse.toString());
            } else {
                throw new IngestServiceException(IngestServiceMessageCode.SERVICE_ERROR,
                    String.format("Ingest operation [%s] has failed", ticket)
                );
            }

            // Complete task
            logger.debug("Completed task {}: {}", taskId, externalTask);

            this.postExecution(externalTask, externalTaskService);

            externalTaskService.complete(externalTask);

            logger.info("Completed task {}", taskId);
        } catch (final IngestServiceException ex) {
            logger.error("[INGEST Service] Operation has failed", ex);

            externalTaskService.handleFailure(
                externalTask, "[INGEST Service] Operation has failed", ex.getErrorDetails(), ex.getRetries(), ex.getRetryTimeout()
            );
        } catch (final Exception ex) {
            logger.error("Unhandled error has occurred", ex);

            final int  retryCount   = 0;
            final long retryTimeout = 2000L;

            externalTaskService.handleFailure(externalTask, "Unhandled error has occurred", ex.getMessage(), retryCount, retryTimeout);
        }
    }

    private String getSource(ExternalTask externalTask, ExternalTaskService externalTaskService) throws IngestServiceException {
        try {
            final String userIdVariableName = this.getUserIdVariableName(externalTask, externalTaskService);
            final String sourceVariableName = this.getSourceVariableName(externalTask, externalTaskService);

            // Get user id key
            final String userId = (String) externalTask.getVariable(userIdVariableName);
            if (StringUtils.isBlank(userId)) {
                logger.error("Expected {} to be non empty!", userIdVariableName);

                throw this.buildVariableNotFoundException(userIdVariableName);
            }
            // Get source
            final String source = (String) externalTask.getVariable(sourceVariableName);
            if (StringUtils.isBlank(source)) {
                logger.error("Expected {} to be non empty!", sourceVariableName);

                throw this.buildVariableNotFoundException(sourceVariableName);
            }

            // Resolve source
            final FilePathCommand command = FilePathCommand.builder()
                .userId(Integer.parseInt(userId))
                .path(source)
                .build();

            final Path path = this.fileManager.resolveFilePath(command);

            return path.toString();
        } catch(final FileSystemException ex) {
            throw IngestServiceException.builder()
                .code(IngestServiceMessageCode.SOURCE_NOT_FOUND)
                .message("Failed to resolve source file")
                .errorDetails(ex.getMessage())
                .build();
        }
    }

    private IngestServiceException buildVariableNotFoundException(String name) {
        return  IngestServiceException.builder()
        .code(IngestServiceMessageCode.VARIABLE_NOT_FOUND)
        .message("Variable not found")
        .errorDetails(String.format("Variable [%s] is empty", name))
        .retries(0)
        .retryTimeout(0L)
        .build();
    }

    abstract protected String getUserIdVariableName(ExternalTask externalTask, ExternalTaskService externalTaskService);

    abstract protected String getSourceVariableName(ExternalTask externalTask, ExternalTaskService externalTaskService);

    protected void preExecution(ExternalTask externalTask, ExternalTaskService externalTaskService) {

    }

    protected void postExecution(ExternalTask externalTask, ExternalTaskService externalTaskService) {

    }

}