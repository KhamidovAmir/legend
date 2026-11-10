--liquibase formatted sql

--changeset demp:003-seed-registration-rules
insert into registration_rule (id, source_system, event_type, decision, exception_type) values
    (gen_random_uuid(), 'DELIVERY', 'DeliveryFailed',            'CREATE', 'DELIVERY_FAILURE'),
    (gen_random_uuid(), 'DELIVERY', 'ShipmentDataInvalid',       'CREATE', 'SHIPMENT_DATA_INVALID'),
    (gen_random_uuid(), 'DELIVERY', 'CourierTaskExpired',        'CREATE', 'COURIER_TASK_EXPIRED'),
    (gen_random_uuid(), 'WMS',      'WarehouseOperationTimeout', 'CREATE', 'WAREHOUSE_TIMEOUT'),
    (gen_random_uuid(), 'DELIVERY', 'DeliveryAttemptStarted',    'IGNORE', null);
--rollback delete from registration_rule;

--changeset demp:003-seed-classification-policies
-- COURIER_TASK_EXPIRED intentionally has no policy: such exceptions wait for manual classification
insert into classification_policy (id, exception_type, category, severity, priority) values
    (gen_random_uuid(), 'DELIVERY_FAILURE',      'DELIVERY',  'HIGH',   'P1'),
    (gen_random_uuid(), 'SHIPMENT_DATA_INVALID', 'DATA',      'MEDIUM', 'P2'),
    (gen_random_uuid(), 'WAREHOUSE_TIMEOUT',     'WAREHOUSE', 'MEDIUM', 'P2');
--rollback delete from classification_policy;
