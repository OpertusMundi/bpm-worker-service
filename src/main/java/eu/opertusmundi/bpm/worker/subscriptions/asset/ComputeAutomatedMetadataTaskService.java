package eu.opertusmundi.bpm.worker.subscriptions.asset;

import java.nio.file.Path;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;

import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.model.asset.AssetDraftSetStatusCommandDto;
import eu.opertusmundi.common.model.asset.EnumProviderAssetDraftStatus;
import eu.opertusmundi.common.model.file.FileSystemException;
import eu.opertusmundi.common.model.profiler.DataProfilerDeferredResponseDto;
import eu.opertusmundi.common.model.profiler.DataProfilerServiceException;
import eu.opertusmundi.common.model.profiler.DataProfilerServiceMessageCode;
import eu.opertusmundi.common.model.profiler.DataProfilerStatusResponseDto;
import eu.opertusmundi.common.model.profiler.EnumDataProfilerSourceType;
import eu.opertusmundi.common.service.AssetFileManager;
import eu.opertusmundi.common.service.DataProfilerService;
import eu.opertusmundi.common.service.ProviderAssetService;

@Service
public class ComputeAutomatedMetadataTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(ComputeAutomatedMetadataTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.data-profiler.lock-duration:60000}")
    private Long lockDurationMillis;

    @Autowired
    private AssetFileManager fileManager;

    @Autowired
    private DataProfilerService profilerService;

    @Autowired
    private ProviderAssetService providerAssetService;

    @Override
    public String getTopicName() {
        return "computeAutomatedMetadata";
    }

    @Override
    protected long getLockDuration() {
        return this.lockDurationMillis;
    }

    @Override
    public void execute(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        try {
            final String taskId = externalTask.getId();

            logger.info("Received task {}", taskId);

            final String                     draftKey     = this.getDraftKey(externalTask, externalTaskService);
            final String                     publisherKey = this.getPublisherKey(externalTask, externalTaskService);
            final String                     source       = this.getSource(externalTask, externalTaskService);
            final EnumDataProfilerSourceType type         = this.getType(externalTask, externalTaskService);

            logger.debug("Processing task {}: {}", taskId, externalTask);

            final DataProfilerDeferredResponseDto profilerResponse = this.profilerService.profile(type, source);
            final String                          ticket           = profilerResponse.getTicket();
            DataProfilerStatusResponseDto         result           = null;
            int                                   counter          = 0;

            // TODO: Add a parameter for preventing infinite loops
            while (counter++ < 100) {
                // NOTE: Polling time must be less than lock duration
                Thread.sleep(10000);

                // Extend lock duration
                externalTaskService.extendLock(externalTask, this.getLockDuration());

                result = this.profilerService.getStatus(ticket);
                if (result.isCompleted()) {
                    break;
                }
            }

            if (result != null && result.isCompleted() && result.isSuccess()) {
                // Get metadata
                final JsonNode endpointsResponse = this.profilerService.getMetadata(ticket);

                // TODO: Update metadata
                // logger.warn(endpointsResponse.toString());
            } else {
                throw new DataProfilerServiceException(DataProfilerServiceMessageCode.SERVICE_ERROR,
                    String.format("Data profiler operation [%s] has failed", ticket)
                );
            }

            // Update draft
            final AssetDraftSetStatusCommandDto command = new AssetDraftSetStatusCommandDto();

            command.setAssetKey(UUID.fromString(draftKey));
            command.setPublisherKey(UUID.fromString(publisherKey));
            command.setStatus(EnumProviderAssetDraftStatus.PENDING_HELPDESK_REVIEW);

            this.providerAssetService.updateStatus(command);

            // Complete task
            externalTaskService.complete(externalTask);

            logger.info("Completed task {}", taskId);
        } catch (final DataProfilerServiceException ex) {
            logger.error("[DATA PROFILER Service] Operation has failed", ex);

            externalTaskService.handleFailure(
                externalTask, "[DATA PROFILER Service] Operation has failed", ex.getErrorDetails(), ex.getRetries(), ex.getRetryTimeout()
            );
        } catch (final Exception ex) {
            logger.error("Unhandled error has occurred", ex);

            final int  retryCount   = 0;
            final long retryTimeout = 2000L;

            externalTaskService.handleFailure(externalTask, "Unhandled error has occurred", ex.getMessage(), retryCount, retryTimeout);

            return;
        }
    }

    private String getDraftKey(ExternalTask externalTask, ExternalTaskService externalTaskService) throws DataProfilerServiceException {
        final String draftKey = (String) externalTask.getVariable("draftKey");
        if (StringUtils.isBlank(draftKey)) {
            logger.error("Expected draft key to be non empty!");

            throw this.buildVariableNotFoundException("draftKey");
        }

        return draftKey;
    }

    private String getPublisherKey(ExternalTask externalTask, ExternalTaskService externalTaskService) throws DataProfilerServiceException {
        final String publisherKey = (String) externalTask.getVariable("publisherKey");
        if (StringUtils.isBlank(publisherKey)) {
            logger.error("Expected publisher key to be non empty!");

            throw this.buildVariableNotFoundException("publisherKey");
        }

        return publisherKey;
    }

    private EnumDataProfilerSourceType getType(
        ExternalTask externalTask, ExternalTaskService externalTaskService
    ) throws DataProfilerServiceException {
        // Get source
        final String sourceType = (String) externalTask.getVariable("sourceType");
        if (StringUtils.isBlank(sourceType)) {
            logger.error("Expected source to be non empty!");

            throw this.buildVariableNotFoundException("sourceType");
        }

        try {
            final EnumDataProfilerSourceType result = EnumDataProfilerSourceType.valueOf(sourceType);

            return result;
        } catch(final Exception ex) {
            logger.error("Expected a valid source type!");

            throw this.buildInvalidVariableValueException("sourceType", sourceType);
        }
    }

    private String getSource(
        ExternalTask externalTask, ExternalTaskService externalTaskService
    ) throws DataProfilerServiceException {
        try {
            // Get draft key
            final String draftKey = (String) externalTask.getVariable("draftKey");
            if (StringUtils.isBlank(draftKey)) {
                logger.error("Expected draft key to be non empty!");

                throw this.buildVariableNotFoundException("draftKey");
            }
            // Get source
            final String source = (String) externalTask.getVariable("source");
            if (StringUtils.isBlank(source)) {
                logger.error("Expected source to be non empty!");

                throw this.buildVariableNotFoundException("source");
            }

            // Resolve source
            final Path path = this.fileManager.resolveFilePath(UUID.fromString(draftKey), source);

            return path.toString();
        } catch(final FileSystemException ex) {
            throw DataProfilerServiceException.builder()
                .code(DataProfilerServiceMessageCode.SOURCE_NOT_FOUND)
                .message("Failed to resolve source file")
                .errorDetails(ex.getMessage())
                .build();
        }
    }

    private DataProfilerServiceException buildVariableNotFoundException(String name) {
        return this.buildVariableException(
            DataProfilerServiceMessageCode.VARIABLE_NOT_FOUND,
            "Variable not found",
            String.format("Variable [%s] is empty", name)
        );
    }

    private DataProfilerServiceException buildInvalidVariableValueException(String name, String value) {
        return this.buildVariableException(
            DataProfilerServiceMessageCode.INVALID_VARIABLE_VALUE,
            "Invalid variable value",
            String.format("Value [%s] is valied for variable [%s]", value, name)
        );
    }

    private DataProfilerServiceException buildVariableException(
        DataProfilerServiceMessageCode code, String message, String errorDetails
    ) {
        return  DataProfilerServiceException.builder()
        .code(code)
        .message(message)
        .errorDetails(errorDetails)
        .retries(0)
        .retryTimeout(0L)
        .build();
    }

}