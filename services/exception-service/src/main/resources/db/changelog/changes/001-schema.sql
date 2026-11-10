--liquibase formatted sql

--changeset demp:001-inbound-event
create table inbound_event (
    id            uuid primary key,
    source_system text        not null,
    event_id      text        not null,
    event_type    text        not null,
    entity_type   text,
    entity_id     text,
    payload       jsonb       not null,
    decision      text        not null,           -- RECEIVED | CREATED | LINKED | IGNORED
    exception_id  uuid,
    received_at   timestamptz not null default now(),
    -- technical idempotency of external events
    constraint ux_inbound_event_source unique (source_system, event_id)
);
--rollback drop table inbound_event;

--changeset demp:001-exception-record
create table exception_record (
    id                       uuid primary key,
    source_system            text        not null,
    business_correlation_key text        not null,
    exception_type           text        not null,
    category                 text,
    severity                 text,
    priority                 text,
    region                   text,
    status                   text        not null,
    close_reason             text,
    resolution               text,
    classified_by            text,
    revision                 bigint      not null,
    version                  bigint      not null,
    detected_at              timestamptz not null,
    created_at               timestamptz not null,
    updated_at               timestamptz not null,
    classified_at            timestamptz,
    resolved_at              timestamptz,
    closed_at                timestamptz
);
-- business deduplication: at most one ACTIVE exception per correlation key, enforced by the database,
-- so two events about the same problem processed in parallel cannot create two exceptions
create unique index ux_exception_active_key on exception_record (business_correlation_key) where status <> 'CLOSED';
-- operator list of active exceptions
create index ix_exception_active_status on exception_record (status, created_at) where status <> 'CLOSED';
--rollback drop table exception_record;

--changeset demp:001-exception-reference
create table exception_reference (
    id           uuid primary key,
    exception_id uuid not null references exception_record (id),
    system       text not null,
    entity_type  text not null,
    entity_id    text not null
);
create index ix_exception_reference_exception on exception_reference (exception_id);
create index ix_exception_reference_entity on exception_reference (system, entity_type, entity_id);
--rollback drop table exception_reference;

--changeset demp:001-exception-relation
create table exception_relation (
    id                uuid primary key,
    from_exception_id uuid        not null references exception_record (id),
    to_exception_id   uuid        not null references exception_record (id),
    relation_type     text        not null,       -- CAUSED | RELATED_TO | DUPLICATE_OF
    created_at        timestamptz not null default now(),
    constraint ux_exception_relation unique (from_exception_id, to_exception_id, relation_type),
    constraint ck_exception_relation_self check (from_exception_id <> to_exception_id)
);
--rollback drop table exception_relation;

--changeset demp:001-exception-history
create table exception_history (
    id           uuid primary key,
    exception_id uuid        not null references exception_record (id),
    event        text        not null,
    from_status  text,
    to_status    text,
    actor        text        not null,
    details      text,
    created_at   timestamptz not null
);
create index ix_exception_history_exception on exception_history (exception_id, created_at);
--rollback drop table exception_history;

--changeset demp:001-rules
create table registration_rule (
    id             uuid primary key,
    source_system  text    not null,
    event_type     text    not null,
    decision       text    not null,              -- CREATE | IGNORE
    exception_type text,
    enabled        boolean not null default true,
    constraint ux_registration_rule unique (source_system, event_type)
);
create table classification_policy (
    id             uuid primary key,
    exception_type text not null unique,
    category       text not null,
    severity       text not null,
    priority       text not null
);
--rollback drop table classification_policy; drop table registration_rule;
