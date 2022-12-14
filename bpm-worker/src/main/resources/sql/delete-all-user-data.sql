DO $$
DECLARE
  p_account_id              integer                 := ${accountId};
  p_account_key             UUID                    := '${accountKey}';
  p_pid                     character varying ARRAY := ARRAY[${pid}]::character varying[];
  p_delete_account          boolean                 := ${accountDeleted};
  p_delete_contracts        boolean                 := ${contractsDeleted};
  p_customer_consumer       integer;
  p_customer_provider       integer;
  p_customer_consumer_draft integer;
  p_customer_provider_draft integer;
BEGIN
  -- Delete asset statistics (cascades delete operation to asset_statistics_country)
  DELETE from "analytics".asset_statistics s where s.pid = ANY(p_pid);

  -- Delete payment history specific to this user
  DELETE from "analytics".payin_item_hist h where h.consumer = p_account_id or h.provider = p_account_id;

  -- Delete billing records (deletes records from all payin related tables)
  DELETE from "billing".service_billing b  where b."billed_account" = p_account_id;
  
  DELETE from "billing".payin_recurring_registration r where r."subscription" in (
    select s."id" from web.account_subscription s where s.consumer = p_account_id or s.provider = p_account_id
  );
  DELETE from "billing".payin where consumer = p_account_id;
  DELETE from "billing".payout p where p.provider = p_account_id;

  -- Delete contract records
  IF p_delete_account OR p_delete_contracts THEN
    DELETE from "contract".provider_section_draft s where s.contract in (
      select id from "contract".provider_contract_draft c where c.owner  = p_account_id
    );
    DELETE from "contract".provider_contract_draft c where c.owner  = p_account_id;

    DELETE from "contract".provider_section s where s.contract in (
      select id from "contract".provider_contract c where c.owner  = p_account_id
    );
    DELETE from "contract".provider_contract c where c.owner  = p_account_id;

    DELETE from "contract".provider_section_history s where s.contract in (
      select id from "contract".provider_contract_history c where c.owner  = p_account_id
    );
    DELETE from "contract".provider_contract_history c where c.owner  = p_account_id;
  END IF;

  -- Delete files
  DELETE from "file".asset_additional_resource f where
    f.pid = ANY(p_pid) OR
    f.draft_key in (select "key" from "provider".asset_draft a where a.account = p_account_id);
  DELETE from "file".asset_contract_annex f where
    f.pid = ANY(p_pid) OR
    f.draft_key in (select "key" from "provider".asset_draft a where a.account = p_account_id);
  DELETE from "file".asset_resource f where
    f.pid = ANY(p_pid) OR
    f.draft_key in (select "key" from "provider".asset_draft a where a.account = p_account_id);
  DELETE from "file".file_copy_resource where account_key = p_account_key;

  -- Delete all messages and notifications
  DELETE from messaging.message where sender = p_account_key or recipient = p_account_key;
  DELETE from messaging.message_thread where id in (
    select distinct m.thread from messaging.message m where m.sender = p_account_key or m.recipient = p_account_key
  );
  DELETE from messaging.notification where recipient = p_account_key;

  -- Delete orders (provider)
  DELETE from "order"."order" o where o.id in (select distinct i."order" from "order".order_item i where i.provider = p_account_id);
  -- Delete orders (consumer)
  DELETE from "order"."order" o where o.consumer = p_account_id;
  -- Delete cart history
  DELETE from "order".cart c where c.account = p_account_id;

  -- Delete provider records
  DELETE from "provider".asset_draft where account = p_account_id;

  -- Delete ratings
  DELETE from "rating".asset r where r.asset = ANY(p_pid);
  DELETE from "rating".provider r where r.provider = p_account_key or account = p_account_key;

  -- Delete user records
  SELECT
    consumer,            provider,            consumer_registration,     provider_registration INTO
    p_customer_consumer, p_customer_provider, p_customer_consumer_draft, p_customer_provider_draft
  FROM web.account_profile p WHERE p.id = p_account_id;

  IF p_delete_account THEN
    UPDATE "web".account_profile p set consumer = null, provider = null, consumer_registration = null, provider_registration = null
    where p.id = p_account_id;
  END IF;

  DELETE from "web".account_asset a           where a.consumer = p_account_id;
  DELETE from "web".account_client c          where c.account  = p_account_id;
  DELETE from "web".account_recent_search s   where s.account  = p_account_id;
  DELETE from "web".account_subscription s    where s.consumer = p_account_id or provider = p_account_id;
  DELETE from "web".account_user_service s    where s.account  = p_account_id;

  DELETE from "web".favorite f where f.account = p_account_id;
  DELETE from "web".record_lock l where l.account = p_account_id;

  IF p_delete_account THEN
    DELETE from "web".activation_token t where t.account = p_account_id;
    DELETE from "web".customer c where c.id in (p_customer_consumer, p_customer_provider);
    DELETE From "web".customer_draft d where d.id in (p_customer_consumer_draft, p_customer_provider_draft);

    DELETE from "web".account_profile p where p.account = p_account_id;
    DELETE from "web".account_role r where r.account = p_account_id;
    DELETE from "web".account      a where a.id      = p_account_id;
  ELSE
    UPDATE "web".account set active_task = 'NONE' where id = p_account_id;
  END IF;
END $$;


