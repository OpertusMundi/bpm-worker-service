package eu.opertusmundi.bpm.worker.subscriptions.asset;

import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import eu.opertusmundi.bpm.worker.model.ErrorCodes;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.model.ServiceException;
import eu.opertusmundi.common.model.asset.AssetDraftDto;
import eu.opertusmundi.common.model.asset.EnumResourceType;
import eu.opertusmundi.common.model.asset.ExternalUrlFileResourceCommandDto;
import eu.opertusmundi.common.model.asset.ExternalUrlResourceDto;
import eu.opertusmundi.common.service.ProviderAssetService;
import eu.opertusmundi.common.util.BpmInstanceVariablesBuilder;

@Service
public class DownloadExternalResourcesTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(ProfileTaskService.class);

    @Value("${opertusmundi.bpm.worker.tasks.download-external-resources.lock-duration:120000}")
    private Long lockDurationMillis;

    @Value("${opertusmundi.bpm.worker.tasks.download-external-resources.buffer-size:65536}")
    private int bufferSize;

    private final ProviderAssetService providerAssetService;

    @Autowired
    public DownloadExternalResourcesTaskService(ProviderAssetService providerAssetService) {
        this.providerAssetService = providerAssetService;
    }

    @Override
    public String getTopicName() {
        return "downloadExternalResources";
    }

    @Override
    protected long getLockDuration() {
        return this.lockDurationMillis;
    }

    @Override
    public void execute(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        final String taskId = externalTask.getId();

        logger.info("Received task. [taskId={}]", taskId);
        
        final UUID          draftKey     = this.getVariableAsUUID(externalTask, externalTaskService, "draftKey");
        final UUID          publisherKey = this.getVariableAsUUID(externalTask, externalTaskService, "publisherKey");
        final AssetDraftDto draft        = providerAssetService.findOneDraft(draftKey);
               
        final List<ExternalUrlResourceDto> externalResources = draft.getCommand().getResources().stream()
            .filter(r -> r.getType() == EnumResourceType.EXTERNAL_URL)
            .map(r -> (ExternalUrlResourceDto) r)
            .toList();
        
        logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);
        
        Path tempPath = null;
        
        try {
            tempPath = this.setupTempFolder(draftKey);
            
            for(var r : externalResources) {
                var targetPath = this.downloadResource(externalTask, externalTaskService, tempPath, r);
                var command = ExternalUrlFileResourceCommandDto.builder()
                    .crs(r.getCrs())
                    .draftKey(draftKey)
                    .encoding(r.getEncoding())
                    .fileName(r.getFileName())
                    .format(r.getFormat())
                    .ownerKey(draft.getOwner())
                    .path(targetPath.toString())
                    .publisherKey(publisherKey)
                    .size(targetPath.toFile().length())
                    .url(r.getUrl())
                    .build();
                
                this.providerAssetService.addFileResourceFromExternalUrl(command);
            }
                       
            // Complete task
            final Map<String, Object> variables = BpmInstanceVariablesBuilder.builder().buildValues();
            externalTaskService.complete(externalTask, variables);

            logger.info("Completed task. [taskId={}]", taskId);
        } catch (final ServiceException ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleBpmnError(externalTaskService, externalTask, ErrorCodes.PublishAsset, ex);
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleFailure(externalTaskService, externalTask, ex);
        } finally {
            if (tempPath != null) {
                this.cleanupTempFolder(tempPath);
            }
        }
    }
    
    private Path downloadResource(
        ExternalTask externalTask, ExternalTaskService externalTaskService, Path tempPath, ExternalUrlResourceDto urlResource
    ) {
        final var targetPath = tempPath.resolve(urlResource.getId().toString());
        final var targetFile = targetPath.toFile();

        if (targetFile.exists()) {
            this.deleteExistingFile(targetFile.toPath());
        }

        final var watch = new StopWatch();
        watch.start();
        
        try (
            BufferedInputStream in = new BufferedInputStream(new URL(urlResource.getUrl()).openStream());
            FileOutputStream fileOutputStream = new FileOutputStream(targetFile);
        ) {
            byte dataBuffer[] = new byte[bufferSize];
            int  bytesRead;
            while ((bytesRead = in.read(dataBuffer, 0, bufferSize)) != -1) {
                fileOutputStream.write(dataBuffer, 0, bytesRead);

                // Extend lock duration
                if (watch.getTime() > lockDurationMillis / 2) {
                    externalTaskService.extendLock(externalTask, this.getLockDuration());
                    watch.reset();
                    watch.start();
                }
            }
        } catch (IOException ex) {
            throw new ServiceException(String.format("Failed to download file [url=%s]", urlResource.getUrl()), ex);
        }
        
        return targetFile.toPath();
    }
    
    private void deleteExistingFile(Path targetPath) throws ServiceException {
        try {
            Files.delete(targetPath);
        } catch (IOException ex) {
            throw new ServiceException(String.format("Failed to delete temporary file [file=%s]", targetPath), ex);
        }
    }

    private Path setupTempFolder(UUID draftKey) throws IOException {
        var tempDir     = System.getProperty("java.io.tmpdir");
        var taskTempDir = Path.of(tempDir, draftKey.toString());

        if (!taskTempDir.toFile().exists()) {
            Files.createDirectory(taskTempDir);
        }
        return taskTempDir;
    }
    
    private void cleanupTempFolder(Path path) {
        FileUtils.deleteQuietly(path.toFile());
    }    
}