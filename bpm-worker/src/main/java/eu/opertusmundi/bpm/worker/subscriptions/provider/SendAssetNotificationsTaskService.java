package eu.opertusmundi.bpm.worker.subscriptions.provider;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;

import eu.opertusmundi.bpm.worker.subscriptions.user.AbstractCustomerTaskService;
import eu.opertusmundi.common.domain.FavoriteAssetEntity;
import eu.opertusmundi.common.feign.client.MessageServiceFeignClient;
import eu.opertusmundi.common.model.account.AccountDto;
import eu.opertusmundi.common.model.catalogue.client.CatalogueItemDto;
import eu.opertusmundi.common.model.favorite.EnumAssetFavoriteAction;
import eu.opertusmundi.common.model.message.EnumNotificationType;
import eu.opertusmundi.common.model.message.server.ServerNotificationCommandDto;
import eu.opertusmundi.common.repository.AccountRepository;
import eu.opertusmundi.common.repository.FavoriteRepository;
import eu.opertusmundi.common.service.CatalogueService;
import eu.opertusmundi.common.service.messaging.NotificationMessageHelper;

@Service
public class SendAssetNotificationsTaskService extends AbstractCustomerTaskService {

    private static final Logger logger = LoggerFactory.getLogger(SendAssetNotificationsTaskService.class);

    private static final String IDEMPOTENT_KEY_PREFIX = "FAVORITE_ASSET_";
    
    @Value("${opertusmundi.bpm.worker.tasks.send-asset-notifications.lock-duration:120000}")
    private Long lockDurationMillis;

    final private AccountRepository                         accountRepository;
    final private CatalogueService                          catalogueService;
    final private FavoriteRepository                        favoriteRepository;
    final private NotificationMessageHelper                 notificationMessageBuilder;
    final private ObjectProvider<MessageServiceFeignClient> messageClient;
    
    @Autowired
    public SendAssetNotificationsTaskService(
        AccountRepository accountRepository,
        CatalogueService catalogueService,
        FavoriteRepository favoriteRepository,
        NotificationMessageHelper notificationMessageBuilder,
        ObjectProvider<MessageServiceFeignClient> messageClient
    ) {
        super();

        this.accountRepository          = accountRepository;
        this.catalogueService           = catalogueService;
        this.favoriteRepository         = favoriteRepository;
        this.notificationMessageBuilder = notificationMessageBuilder;
        this.messageClient              = messageClient;
    }

    @Override
    public String getTopicName() {
        return "sendAssetNotifications";
    }

    @Override
    protected long getLockDuration() {
        return this.lockDurationMillis;
    }

    @Override
    public void execute(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        try {
            final String taskId = externalTask.getId();

            logger.info("Received task. [taskId={}]", taskId);
            
            final UUID       accountKey     = this.getVariableAsUUID(externalTask, externalTaskService, "accountKey");
            final String     providerUserId = this.getVariableAsString(externalTask, externalTaskService, "providerUserId");
            final AccountDto account        = this.accountRepository.findOneByKeyObject(accountKey).orElse(null);

            logger.debug("Processing task. [taskId={}, externalTask={}]", taskId, externalTask);

            if (this.shouldSendNotifications(account, providerUserId)) {
                this.sendNotifications(externalTask, externalTaskService, account);
            }

            externalTaskService.complete(externalTask);

            logger.info("Completed task. [taskId={}]", taskId);
        } catch (final Exception ex) {
            logger.error(DEFAULT_ERROR_MESSAGE, ex);

            this.handleFailure(externalTaskService, externalTask, ex);
        }
    }
    
    private boolean shouldSendNotifications(AccountDto account, String providerUserId) {
        if (account == null) {
            return false;
        }
        var provider = account.getProfile().getProvider().getCurrent();
        if (provider == null) {
            return false;
        }
        if(!provider.getPaymentProviderUser().equals(providerUserId)) {
            logger.warn(String.format(
                "Provider user id mismatch found [accountKey=%s, expected=%, found=%]", 
                account.getKey(), providerUserId, provider.getPaymentProviderUser()
            ));
            return false;
        }
        
        return true;
    }

    private void sendNotifications(ExternalTask externalTask, ExternalTaskService externalTaskService, AccountDto account) {
        final var direction   = Direction.ASC;
        var       pageRequest = PageRequest.of(0, 10, Sort.by(direction, "account.id"));
        var       itemCache   = new HashMap<String, CatalogueItemDto>();
        var       favorites   = this.favoriteRepository.findAllAssetByAssetProvider(account.getId(), pageRequest);

        while (!favorites.isEmpty()) {
            for (final FavoriteAssetEntity f : favorites.getContent()) {
                if (f.getAction() != EnumAssetFavoriteAction.PURCHASE) {
                    continue;
                }
                // Find item
                CatalogueItemDto item = null;
                if (!itemCache.containsKey(f.getAssetId())) {
                    var publisher = this.accountRepository.findById(f.getAssetProvider()).get();
                    item = this.catalogueService.findOne(null, f.getAssetId(), publisher.getKey(), false);
                    itemCache.put(f.getAssetId(), item);
                }               
                // Send notification
                final String               idempotentKey = IDEMPOTENT_KEY_PREFIX + f.getId().toString();
                final EnumNotificationType type          = EnumNotificationType.ASSET_AVAILABLE_TO_PURCHASE;
                final Map<String, Object>  variables     = externalTask.getAllVariables();
                variables.put("assetName", item.getTitle());

                final JsonNode data = this.notificationMessageBuilder.collectNotificationData(type, variables);

                final ServerNotificationCommandDto notification = ServerNotificationCommandDto.builder()
                    .data(data)
                    .eventType(type.toString())
                    .idempotentKey(idempotentKey)
                    .recipient(f.getAccount().getKey())
                    .text(this.notificationMessageBuilder.composeNotificationText(type, data))
                    .build();

                messageClient.getObject().sendNotification(notification);
                
                // Mark as sent
                f.setNotificationSentAt(ZonedDateTime.now());
                f.setNotificationSent(true);
                this.favoriteRepository.saveAndFlush(f);
            }

            // Extend lock duration
            externalTaskService.extendLock(externalTask, this.getLockDuration());

            pageRequest = pageRequest.next();
            favorites   = this.favoriteRepository.findAllAssetByAssetProvider(account.getId(), pageRequest);
        }
    }
}
