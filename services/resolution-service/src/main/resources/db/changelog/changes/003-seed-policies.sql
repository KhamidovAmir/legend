--liquibase formatted sql

--changeset demp:003-seed-resolution-policies
insert into resolution_policy (id, code, version, exception_type, execution_mode, action_type, target_system,
                               expected_state, resolution_result, bot_id, max_business_attempts, sla_minutes) values
    (gen_random_uuid(), 'AUTO_REDELIVERY', 4, 'DELIVERY_FAILURE', 'EXTERNAL_AUTOMATED', 'RETRY_DELIVERY', 'DELIVERY',
     'DELIVERY_SCHEDULED', 'DELIVERY_RESCHEDULED', 'AUTO_REDELIVERY_BOT', 2, 240),
    (gen_random_uuid(), 'MANUAL_DATA_FIX', 1, 'SHIPMENT_DATA_INVALID', 'MANUAL', null, null, null, null, null, 1, 480);
--rollback delete from resolution_policy;

--changeset demp:003-seed-reconciliation-policies
insert into reconciliation_policy (id, target_system, action_type, settle_delay_seconds, base_interval_seconds,
                                   max_interval_seconds, query_timeout_seconds, not_applied_confirmations,
                                   deadline_minutes, auto_retry_enabled, processing_states) values
    (gen_random_uuid(), 'DELIVERY', 'RETRY_DELIVERY', 60, 30, 600, 120, 3, 60, true, 'RETRY_PROCESSING');
--rollback delete from reconciliation_policy;
