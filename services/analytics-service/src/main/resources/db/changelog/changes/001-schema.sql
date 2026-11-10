--liquibase formatted sql

--changeset demp:001-raw-event
-- Every consumed domain event, as is. The primary key doubles as the inbox (idempotent consumption).
create table raw_event (
    event_id          uuid primary key,
    topic             text        not null,
    event_type        text        not null,
    aggregate_id      text,
    aggregate_version bigint      not null,
    occurred_at       timestamptz not null,
    payload           jsonb       not null,
    received_at       timestamptz not null default now()
);
create index ix_raw_event_type_time on raw_event (event_type, occurred_at);

-- Invalid events are parked here instead of polluting analytical tables or blocking the partition.
create table rejected_event (
    id          bigint generated always as identity primary key,
    topic       text        not null,
    partition_no int        not null,
    offset_no   bigint      not null,
    reason      text        not null,
    raw         text,
    received_at timestamptz not null default now()
);
--rollback drop table rejected_event; drop table raw_event;

--changeset demp:001-exception-fact
-- One row per exception, built from events of three services (the analytical model, not a copy of their tables).
create table exception_fact (
    exception_id        uuid primary key,
    source_system       text,
    exception_type      text,
    category            text,
    severity            text,
    priority            text,
    region              text,
    status              text,
    status_version      bigint      not null default 0,   -- guards against out-of-order status events
    close_reason        text,
    policy_code         text,
    automated           boolean,
    detected_at         timestamptz,
    classified_at       timestamptz,
    first_action_at     timestamptz,
    resolved_at         timestamptz,
    closed_at           timestamptz,
    sla_breached        boolean     not null default false,
    verification_failed boolean     not null default false,
    business_attempts   int         not null default 0,
    unknown_count       int         not null default 0,
    work_items          int         not null default 0,
    updated_at          timestamptz not null default now()
);
create index ix_exception_fact_detected on exception_fact (detected_at);
create index ix_exception_fact_type on exception_fact (exception_type);

create table external_call_stat (
    target_system text   not null,
    outcome       text   not null,             -- ACCEPTED | COMPLETED | FAILED | UNKNOWN
    day           date   not null,
    calls         bigint not null,
    primary key (target_system, outcome, day)
);
--rollback drop table external_call_stat; drop table exception_fact;
