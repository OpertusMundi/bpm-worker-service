package eu.opertusmundi.bpm.worker.subscriptions.asset;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.UUID;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.model.EnumPublishRequestType;
import eu.opertusmundi.bpm.worker.model.ErrorCodes;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.model.BasicMessageCode;
import eu.opertusmundi.common.model.ServiceException;
import eu.opertusmundi.common.model.asset.AssetDraftDto;
import eu.opertusmundi.common.model.asset.AssetDraftSetStatusCommandDto;
import eu.opertusmundi.common.model.asset.EnumProviderAssetDraftStatus;
import eu.opertusmundi.common.model.asset.EnumResourceType;
import eu.opertusmundi.common.model.asset.FileResourceDto;
import eu.opertusmundi.common.model.asset.ResourceDto;
import eu.opertusmundi.common.model.catalogue.client.EnumAssetType;
import eu.opertusmundi.common.model.file.FileSystemException;
import eu.opertusmundi.common.model.ipr.IprServiceMessageCode;
import eu.opertusmundi.common.model.ipr.ServerIprJobStatusResponseDto;
import eu.opertusmundi.common.model.pricing.EnumPricingModel;
import eu.opertusmundi.common.model.profiler.DataProfilerServiceMessageCode;
import eu.opertusmundi.common.service.DraftFileManager;
import eu.opertusmundi.common.service.IprService;
import eu.opertusmundi.common.service.ProviderAssetService;
import eu.opertusmundi.common.util.BpmInstanceVariablesBuilder;

@Service
public class IprTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(IprTaskService.class);

    private static int BUFFER_SIZE = 65536;
    
    @Value("${opertusmundi.bpm.worker.tasks.ipr-protection.lock-duration:120000}")
    private Long lockDurationMillis;

    @Override
    protected long getLockDuration() {
        return this.lockDurationMillis;
    }

    @Value("${opertusmundi.feign.ipr.input}")
    private String inputDir;
    
    @Value("${opertusmundi.feign.ipr.output}")
    private String outputDir;
    
    @Autowired
    private DraftFileManager draftFileManager;

    @Autowired
    private ProviderAssetService providerAssetService;

    @Autowired
    private IprService iprService;

    @Override
    public String getTopicName() {
        return "enableIprProtection";
    }

    @Override
    public final void execute(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        final String taskId = externalTask.getId();

        logger.info("Received task. [taskId={}]", taskId);

        final String                 requestType = this.getVariableAsString(externalTask, externalTaskService, "requestType");
        final EnumPublishRequestType type        = EnumPublishRequestType.valueOf(requestType);

        logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

        try {
            // Complete task in asset specific methods
            switch (type) {
                case CATALOGUE_ASSET :
                    this.protectCatalogueAsset(externalTask, externalTaskService);
                    break;
                case USER_SERVICE :
                    this.protectUserService(externalTask, externalTaskService);
                    break;
            }
        } catch (final ServiceException ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);
            if (ExceptionUtils.indexOfType(ex, feign.RetryableException.class) != -1) {
                // For feign client retryable exceptions, create a new incident
                // instead of canceling the process instance. Errors such as
                // network connectivity, unavailable services etc may be
                // automatically resolved after retrying the failed task
                //
                // See:
                // https://javadoc.io/doc/io.github.openfeign/feign-core/latest/feign/RetryableException.html
                //
                // "This exception is raised when the Response is deemed to be
                // retryable, typically via an ErrorDecoder when the status is
                // 503."
                this.handleFailure(externalTaskService, externalTask, ex);
            } else {
                this.handleBpmnError(externalTaskService, externalTask, this.getErrorCode(type), ex);
            }
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleFailure(externalTaskService, externalTask, ex);
        }
    }

    private final void protectCatalogueAsset(
        ExternalTask externalTask, ExternalTaskService externalTaskService
    ) throws InterruptedException, IOException {
        final String taskId       = externalTask.getId();
        final UUID   draftKey     = this.getVariableAsUUID(externalTask, externalTaskService, "draftKey");
        final UUID   publisherKey = this.getVariableAsUUID(externalTask, externalTaskService, "publisherKey");
        
        final AssetDraftDto     draft     = providerAssetService.findOneDraft(publisherKey, draftKey, false);
        final List<ResourceDto> resources = draft.getCommand().getResources();
        
        if (draft.isIprProtectionEnabled()) {
            // Process all resources
            for (ResourceDto resource : resources) {
                if (resource.getType() != EnumResourceType.FILE) {
                    continue;
                }
                final FileResourceDto fileResource      = (FileResourceDto) resource;
                final String          idempotentKey     = fileResource.getId();
                final String          fileName          = fileResource.getFileName();
                final String          initialResource   = this.getResource(externalTask, externalTaskService, publisherKey, draftKey, fileName, false, true);
                final String          protectedResource = this.getResource(externalTask, externalTaskService, publisherKey, draftKey, fileName, true, false);
    
                final Path sourcePath = this.copyInputResource(idempotentKey, initialResource);
    
                final var result = this.protect(externalTask, externalTaskService, idempotentKey, sourcePath, fileResource);
    
                if (result.isCompleted() && result.isSuccess()) {
                    this.decompressAndCopyOutputResource(result.getResource().getOutputPath(), protectedResource);
                }
            }
        }
        
        // Update draft status if data ingestion is not required
        final BpmInstanceVariablesBuilder variables = BpmInstanceVariablesBuilder.builder();
        final var                         ingested  = this.isIngestRequired(draft);
        
        variables.variableAsBoolean("ingested", ingested);
        if (!ingested) {
            final AssetDraftSetStatusCommandDto command   = new AssetDraftSetStatusCommandDto();
            final EnumProviderAssetDraftStatus  newStatus = EnumProviderAssetDraftStatus.PENDING_HELPDESK_REVIEW;

            command.setAssetKey(draftKey);
            command.setPublisherKey(publisherKey);
            command.setStatus(newStatus);

            variables.variableAsString("status", newStatus.toString());

            this.providerAssetService.updateStatus(command);
        }

        logger.info("Completed task. [taskId={}]", taskId);
        
        externalTaskService.complete(externalTask, variables.buildValues());
    }

    /**
     * Returns true is Ingest task service must be executed
     * 
     * <p>
     * Data ingestion is required for service ASSETS and assets that support the
     * {@code FIXED_PER_ROWS} pricing model.
     * 
     * @param draft
     * @return
     */
    private boolean isIngestRequired(AssetDraftDto draft) {
        final var type          = draft.getCommand().getType();
        final var fixedRowModel = draft.getCommand().getPricingModels().stream()
            .filter(p -> p.getType() == EnumPricingModel.FIXED_PER_ROWS)
            .findAny().orElse(null);

        return (type == EnumAssetType.SERVICE || fixedRowModel != null);
    }

    private final void protectUserService(
        ExternalTask externalTask, ExternalTaskService externalTaskService
    ) throws InterruptedException {
        throw new ServiceException(BasicMessageCode.NotImplemented, "IPR protection for user services is not supported");
    }

    private ServerIprJobStatusResponseDto protect(
        ExternalTask externalTask, ExternalTaskService externalTaskService,
        String idempotentKey, Path sourcePath, FileResourceDto resource
    ) throws InterruptedException {

        String                        ticket    = null;
        ServerIprJobStatusResponseDto jobStatus = null;
        int                           counter   = 0;

        jobStatus = this.iprService.getJobStatus(ticket, idempotentKey);

        if (jobStatus == null) {
            final var serviceResponse = this.iprService.embedFictitious(
                idempotentKey, 
                sourcePath.toString(), 
                resource.getCrs(),
                resource.getDelimiter(), 
                resource.getEncoding(), 
                resource.getGeometry(), 
                resource.getLatitude(), 
                resource.getLongitude()
            );
            ticket = serviceResponse.getTicket();

            jobStatus = this.iprService.getJobStatus(ticket, idempotentKey);
        }

        if (!jobStatus.isCompleted()) {
            // TODO: Add a parameter for preventing infinite loops
            while (counter++ < 100) {
                // NOTE: Polling interval must be less than lock duration
                Thread.sleep(15000);

                // Extend lock duration
                externalTaskService.extendLock(externalTask, this.getLockDuration());

                try {
                    jobStatus = this.iprService.getJobStatus(ticket, idempotentKey);
                    if (jobStatus.isCompleted()) {
                        break;
                    }
                } catch (Exception ex) {
                    // Ignore exception since the remote server may have not
                    // initialized the job
                    logger.info(String.format("IPR service get job status operation has failed [idempotentKey=%s]", idempotentKey), ex);
                }
            }
        }

        if (!jobStatus.isSuccess() || !jobStatus.isCompleted()) {
            if (counter == 100) {
                logger.warn(String.format("IPR service operation has timed out [idempotentKey=%s]", idempotentKey));
            }

            throw BpmnWorkerException.builder()
                .code(IprServiceMessageCode.SERVICE_ERROR)
                .message("[IPR Service] Operation has failed")
                .errorDetails(String.format(
                    "Idempotent Key: [%s]. Message: [%s]. Status: [%s]", 
                    idempotentKey, jobStatus.getErrorMessage(), jobStatus.getStatus()
                ))
                .build();
        }
        
        return jobStatus;
    }

    private String getResource(
        ExternalTask externalTask, ExternalTaskService externalTaskService, 
        UUID publisherKey, UUID draftKey, String resourceFileName, boolean withIprProtection, boolean throwIfNotExists
    ) throws BpmnWorkerException {
        try {
            final Path path = this.draftFileManager.resolveResourcePath(
                publisherKey, draftKey, resourceFileName, withIprProtection, throwIfNotExists
            );

            return path.toString();
        } catch(final FileSystemException ex) {
            throw BpmnWorkerException.builder()
                .code(DataProfilerServiceMessageCode.SOURCE_NOT_FOUND)
                .message(String.format("Failed to resolve resource file [%s]", resourceFileName))
                .errorDetails(ex.getRootCause().getMessage())
                .build();
        }
    }

    private String getErrorCode(EnumPublishRequestType type) {
        switch (type) {
            case CATALOGUE_ASSET :
                return ErrorCodes.PublishAsset;
            case USER_SERVICE :
                return ErrorCodes.PublishUserService;
        }

        return ErrorCodes.None;
    }
    
    private Path copyInputResource(String idempotentKey, String absoluteSource) throws IOException {
        final String fileName           = FilenameUtils.getName(absoluteSource);
        final Path   absoluteTargetPath = Paths.get(this.inputDir, idempotentKey, fileName);

        Files.createDirectories(Paths.get(this.inputDir, idempotentKey));
        Files.copy(Paths.get(absoluteSource), absoluteTargetPath, StandardCopyOption.REPLACE_EXISTING);

        return Paths.get(idempotentKey, fileName);
    }
    
    private void decompressAndCopyOutputResource(String relativeSource, String absoluteTarget) throws IOException {
        final Path absoluteSourcePath = Paths.get(this.outputDir, relativeSource);
        final Path targetParentPath   = Paths.get(absoluteTarget).getParent();
        
        Files.createDirectories(targetParentPath);

        try (
            FileInputStream           in     = new FileInputStream(absoluteSourcePath.toString());
            GzipCompressorInputStream gzipIn = new GzipCompressorInputStream(in);
            TarArchiveInputStream     tarIn  = new TarArchiveInputStream(gzipIn);
        ){
            TarArchiveEntry entry;
            
            while ((entry = (TarArchiveEntry) tarIn.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    // Skip directories
                } else {
                    int        length;
                    byte[]     buffer    = new byte[BUFFER_SIZE];
                    final Path entryPath = targetParentPath.resolve(Paths.get(entry.getName()));

                    Files.deleteIfExists(entryPath);

                    try (
                        FileOutputStream     out       = new FileOutputStream(absoluteTarget);
                        BufferedOutputStream bufferOut = new BufferedOutputStream(out, BUFFER_SIZE);
                    ) {
                        while ((length = tarIn.read(buffer, 0, BUFFER_SIZE)) != -1) {
                            bufferOut.write(buffer, 0, length);
                        }
                    }
                }
            }
        } catch (IOException ex) {
            throw BpmnWorkerException.builder()
                .code(IprServiceMessageCode.SERVICE_ERROR)
                .message("[IPR Service] Failed to decompress output file")
                .errorDetails(String.format("Source: [%s], Target: [%s]", absoluteSourcePath, absoluteTarget))
                .build();
        }
        
    }
}