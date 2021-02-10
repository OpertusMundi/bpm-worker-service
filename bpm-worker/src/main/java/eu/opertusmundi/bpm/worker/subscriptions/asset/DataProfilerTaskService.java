package eu.opertusmundi.bpm.worker.subscriptions.asset;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
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

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.domain.AssetFileTypeEntity;
import eu.opertusmundi.common.model.asset.AssetDraftDto;
import eu.opertusmundi.common.model.asset.AssetDraftSetStatusCommandDto;
import eu.opertusmundi.common.model.asset.AssetResourceDto;
import eu.opertusmundi.common.model.asset.EnumProviderAssetDraftStatus;
import eu.opertusmundi.common.model.file.FileSystemException;
import eu.opertusmundi.common.model.profiler.DataProfilerDeferredResponseDto;
import eu.opertusmundi.common.model.profiler.DataProfilerOptions;
import eu.opertusmundi.common.model.profiler.DataProfilerServiceException;
import eu.opertusmundi.common.model.profiler.DataProfilerServiceMessageCode;
import eu.opertusmundi.common.model.profiler.DataProfilerStatusResponseDto;
import eu.opertusmundi.common.model.profiler.EnumDataProfilerSourceType;
import eu.opertusmundi.common.repository.AssetFileTypeRepository;
import eu.opertusmundi.common.service.AssetFileManager;
import eu.opertusmundi.common.service.DataProfilerService;
import eu.opertusmundi.common.service.ProviderAssetService;

@Service
public class DataProfilerTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(DataProfilerTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.data-profiler.lock-duration:60000}")
    private Long lockDurationMillis;

    @Value("${opertusmundi.data-profiler.parameters.aspect-ratio:}")
    private BigDecimal aspectRatio;

    @Value("${opertusmundi.data-profiler.parameters.height:}")
    private Integer height;

    @Value("${opertusmundi.data-profiler.parameters.width:1920}")
    private Integer width;
    
    @Autowired
    private AssetFileManager fileManager;

    @Autowired
    private DataProfilerService profilerService;

    @Autowired
    private ProviderAssetService providerAssetService;
    
    @Autowired
    private AssetFileTypeRepository assetFileTypeRepository;

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

            final UUID                       draftKey     = this.getDraftKey(externalTask, externalTaskService);
            final UUID                       publisherKey = this.getPublisherKey(externalTask, externalTaskService);
            
            final AssetDraftDto draft = providerAssetService.findOneDraft(publisherKey, draftKey);
            
            final List<AssetResourceDto>     resources = draft.getCommand().getResources();
            final EnumDataProfilerSourceType type      = mapFormatToSourceType(draft.getCommand().getFormat());
            
            logger.debug("Processing task {}: {}", taskId, externalTask);

            // Process all resources
            for (AssetResourceDto resource : resources) {
                final JsonNode metadata = this.profile(
                    externalTask, externalTaskService, publisherKey, draftKey, type, resource.getFileName()
                );

                // Update metadata for the specific file
                providerAssetService.updateMetadata(publisherKey, draftKey, resource.getId().toString(), metadata);
            }

            // Update draft status
            final AssetDraftSetStatusCommandDto command = new AssetDraftSetStatusCommandDto();

            command.setAssetKey(draftKey);
            command.setPublisherKey(publisherKey);
            command.setStatus(EnumProviderAssetDraftStatus.PENDING_HELPDESK_REVIEW);

            this.providerAssetService.updateStatus(command);

            // Complete task
            externalTaskService.complete(externalTask);

            logger.info("Completed task {}", taskId);
        } catch (final BpmnWorkerException ex) {
            logger.error(String.format("[Data Profiler Service] Operation has failed. Error details: %s", ex.getErrorDetails()), ex);
            
            externalTaskService.handleFailure(
                externalTask, ex.getMessage(), ex.getErrorDetails(), ex.getRetries(), ex.getRetryTimeout()
            );
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleError(externalTaskService, externalTask, ex);
        }
    }
    
    private JsonNode profile(
        ExternalTask externalTask, ExternalTaskService externalTaskService,
        UUID publisherKey, UUID draftKey, EnumDataProfilerSourceType type, String resource
    ) throws InterruptedException {
        
        final DataProfilerOptions options = DataProfilerOptions.builder()
                .aspectRatio(this.aspectRatio)
                .height(this.height)
                .width(this.width)
                .build();
        
        final String path = this.getResource(externalTask, externalTaskService, publisherKey, draftKey, resource);
        
        final DataProfilerDeferredResponseDto profilerResponse = this.profilerService.profile(type, path.toString(), options);
        final String                          ticket           = profilerResponse.getTicket();
        DataProfilerStatusResponseDto         result           = null;
        int                                   counter          = 0;

        // TODO: Add a parameter for preventing infinite loops
        while (counter++ < 100) {
            // NOTE: Polling time must be less than lock duration
            Thread.sleep(10000);

            // Extend lock duration
            externalTaskService.extendLock(externalTask, this.getLockDuration());

            try {
                result = this.profilerService.getStatus(ticket);
                
                if (result.isCompleted()) {
                    break;
                }
            } catch (Exception ex) {
                // Ignore exception since the remote server may not have
                // initialized the job 
            }
        }

        if (result != null && result.isCompleted() && result.isSuccess()) {
            final JsonNode metadata = this.profilerService.getMetadata(ticket);
            
            return metadata;
        } else {
            throw new DataProfilerServiceException(DataProfilerServiceMessageCode.SERVICE_ERROR,
                String.format("Data profiler operation [%s] has failed", ticket)
            );
        }        
    }
    
    private UUID getDraftKey(ExternalTask externalTask, ExternalTaskService externalTaskService) throws BpmnWorkerException {
        final String draftKey = (String) externalTask.getVariable("draftKey");
        if (StringUtils.isBlank(draftKey)) {
            logger.error("Expected draft key to be non empty!");

            throw this.buildVariableNotFoundException("draftKey");
        }

        return UUID.fromString(draftKey);
    }

    private UUID getPublisherKey(ExternalTask externalTask, ExternalTaskService externalTaskService) throws BpmnWorkerException {
        final String publisherKey = (String) externalTask.getVariable("publisherKey");
        if (StringUtils.isBlank(publisherKey)) {
            logger.error("Expected publisher key to be non empty!");

            throw this.buildVariableNotFoundException("publisherKey");
        }

        return UUID.fromString(publisherKey);
    }

    private EnumDataProfilerSourceType mapFormatToSourceType(String format) throws BpmnWorkerException {
        final Optional<AssetFileTypeEntity> fileType = this.assetFileTypeRepository.findOneByFormat(format);

        if (fileType.isPresent()) {
            return fileType.get().getCategory();
        }

        throw BpmnWorkerException.builder()
            .code(DataProfilerServiceMessageCode.FORMAT_NOT_SUPPORTED)
            .message(String.format("Failed to map format [%s] to source type", format))
            .build();
    }
    
    private String getResource(
        ExternalTask externalTask, ExternalTaskService externalTaskService, UUID publisherKey, UUID draftKey, String source
    ) throws BpmnWorkerException {
        try {
            final Path path = this.fileManager.resolveResourcePath(publisherKey, draftKey, source);
            
            return path.toString();
        } catch(final FileSystemException ex) {
            throw BpmnWorkerException.builder()
                .code(DataProfilerServiceMessageCode.SOURCE_NOT_FOUND)
                .message("Failed to resolve source file")
                .errorDetails(ex.getMessage())
                .build();
        }
    }

}