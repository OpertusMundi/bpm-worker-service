package eu.opertusmundi.bpm.worker.subscriptions.asset;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.StandardCopyOption;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import eu.opertusmundi.bpm.worker.model.ErrorCodes;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.model.BasicMessageCode;
import eu.opertusmundi.common.model.ServiceException;
import eu.opertusmundi.common.model.file.FileCopyResourceDto;
import eu.opertusmundi.common.repository.FileCopyResourceRepository;

@Service
public class CopyResourceToDriveTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(CopyResourceToDriveTaskService.class);

    private static final String TEMP_SUFFIX = "copying";

    @Value("${opertusmundi.bpm.worker.tasks.copy-resource-to-drive.lock-duration:300000}")
    private Long lockDurationMillis;

    @Autowired
    private FileCopyResourceRepository fileCopyResourceRepository;

    @Override
    public String getTopicName() {
        return "copyResourceToDrive";
    }

    @Override
    protected long getLockDuration() {
        return this.lockDurationMillis;
    }

    @Override
    public void execute(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        final String taskId      = externalTask.getId();
        final UUID   businessKey = UUID.fromString(externalTask.getBusinessKey());
        File         temp        = null;

        try {
            logger.info("Received task. [taskId={}]", taskId);

            final FileCopyResourceDto copyOperation = this.fileCopyResourceRepository
                .findOneObjectByIdempotentKey(businessKey)
                .orElse(null);

            logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

            if (copyOperation.getCompletedOn() == null) {
                final String source = copyOperation.getSourcePath();
                final String target = copyOperation.getTargetPath();
                temp = new File(target + "." + businessKey.toString() + "." + TEMP_SUFFIX);

                try (
                    InputStream inputStream   = new FileInputStream(source);
                    OutputStream outputStream = new FileOutputStream(temp);
                ) {
                    IOUtils.copyLarge(inputStream, outputStream);

                    // After copying, rename file
                    FileUtils.moveFile(temp, new File(target), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                } catch (Exception ex) {
                    throw new ServiceException(BasicMessageCode.IOError, "Failed to copy files", ex);
                }
            }

            this.fileCopyResourceRepository.complete(businessKey);

            // Complete task
            externalTaskService.complete(externalTask);

            logger.info("Completed task. [taskId={}]", taskId);
        } catch (final ServiceException ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.fileCopyResourceRepository.fail(businessKey, ex.getMessage());

            this.handleBpmnError(externalTaskService, externalTask, ErrorCodes.CopyResourceToDrive, ex);
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);
            this.handleFailure(externalTaskService, externalTask, ex);
        } finally {
            if (temp != null) {
                FileUtils.deleteQuietly(temp);
            }
        }
    }

}