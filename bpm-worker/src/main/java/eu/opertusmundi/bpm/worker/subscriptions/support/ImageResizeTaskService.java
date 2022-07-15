package eu.opertusmundi.bpm.worker.subscriptions.support;

import org.apache.commons.lang3.StringUtils;
import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.stereotype.Service;

import eu.opertusmundi.bpm.worker.model.BpmnWorkerException;
import eu.opertusmundi.bpm.worker.subscriptions.AbstractTaskService;
import eu.opertusmundi.common.domain.AccountEntity;
import eu.opertusmundi.common.domain.AccountProfileEntity;
import eu.opertusmundi.common.domain.CustomerEntity;
import eu.opertusmundi.common.domain.CustomerProfessionalEntity;
import eu.opertusmundi.common.repository.AccountRepository;
import eu.opertusmundi.common.util.ImageUtils;

@Service
public class ImageResizeTaskService extends AbstractTaskService {

    private static final Logger logger = LoggerFactory.getLogger(ImageResizeTaskService.class);

    private static final String IMAGE_SIZE_VARIABLE = "imageSize";
    
    @Value("${opertusmundi.bpm.worker.tasks.ingest.lock-duration:120000}")
    private Long lockDurationMillis;

    @Override
    protected long getLockDuration() {
        return this.lockDurationMillis;
    }

    @Autowired
    private ImageUtils imageUtils;

    @Autowired
    private AccountRepository accountRepository;

    @Override
    public String getTopicName() {
        return "resizeImages";
    }

    @Override
    public final void execute(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        try {
            final String taskId    = externalTask.getId();
            final int    imageSize = this.getImageSize(externalTask, externalTaskService);

            logger.info("Received task. [taskId={}]", taskId);

            logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

            final Sort  sort        = Sort.by(Direction.ASC, "id");
            PageRequest pageRequest = PageRequest.of(0, 10, sort);

            Page<AccountEntity> accounts = this.accountRepository.findAll(pageRequest);

            while (!accounts.isEmpty()) {
                for (AccountEntity account : accounts) {
                    final AccountProfileEntity       profile  = account.getProfile();
                    final CustomerProfessionalEntity provider = profile.getProvider();
                    final CustomerEntity             consumer = profile.getConsumer();

                    // Resize account image
                    imageUtils.resizeImage(profile.getImage(), profile.getImageMimeType(), imageSize);
                    // Resize provider image
                    if (provider != null) {
                        imageUtils.resizeImage(provider.getLogoImage(), provider.getLogoImageMimeType(), imageSize);
                    }
                    // Resize consumer image if the customer type is
                    // professional
                    if (consumer != null && consumer instanceof CustomerProfessionalEntity) {
                        final CustomerProfessionalEntity typedConsumer = (CustomerProfessionalEntity) consumer;
                        imageUtils.resizeImage(typedConsumer.getLogoImage(), typedConsumer.getLogoImageMimeType(), imageSize);
                    }
                }

                // Extend lock duration
                externalTaskService.extendLock(externalTask, this.getLockDuration());

                pageRequest = pageRequest.next();
                accounts    = this.accountRepository.findAll(pageRequest);
            }

            // Complete task
            this.postExecution(externalTask, externalTaskService);

            externalTaskService.complete(externalTask);

            logger.info("Completed task. [taskId={}]", taskId);
        } catch (final BpmnWorkerException ex) {
            logger.error(String.format("Operation has failed. [details=%s]", ex.getErrorDetails()), ex);

            externalTaskService.handleFailure(
                externalTask, ex.getMessage(), ex.getErrorDetails(), ex.getRetries(), ex.getRetryTimeout()
            );
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleFailure(externalTaskService, externalTask, ex);
        }
    }

    protected void preExecution(ExternalTask externalTask, ExternalTaskService externalTaskService) {

    }

    protected void postExecution(ExternalTask externalTask, ExternalTaskService externalTaskService) {

    }
    
    private int getImageSize(ExternalTask externalTask, ExternalTaskService externalTaskService) throws BpmnWorkerException {
        final String value = (String) externalTask.getVariable(IMAGE_SIZE_VARIABLE);

        if (StringUtils.isBlank(value)) {
            logger.warn("Variable [{}] was not found. Using default size [{}]", IMAGE_SIZE_VARIABLE, ImageUtils.DEFAULT_SIZE);

            return ImageUtils.DEFAULT_SIZE;
        }

        final int result = Integer.parseInt(value);

        if (result <= 0) {
            logger.warn("Image size [{}] is invalid. Using default size [{}]", result, ImageUtils.DEFAULT_SIZE);

            return ImageUtils.DEFAULT_SIZE;
        }

        return result;
    }

}