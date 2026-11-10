--liquibase formatted sql

--changeset demp:002-inbox
create table inbox (
    consumer_id  text        not null,
    event_id     uuid        not null,
    processed_at timestamptz not null default now(),
    primary key (consumer_id, event_id)
);
--rollback drop table inbox;

--changeset demp:002-outbox
-- Delivered by the polling relay (low volume), hence the published_at marker.
create table outbox (
    id                uuid primary key,
    aggregate_type    text        not null,
    aggregate_id      text        not null,
    aggregate_version bigint      not null,
    destination       text        not null,
    event_type        text        not null,
    event_version     int         not null,
    payload           jsonb       not null,
    correlation_id    text,
    causation_id      text,
    trace_parent      text,
    created_at        timestamptz not null,
    published_at      timestamptz
);
create index ix_outbox_pending on outbox (created_at, id) where published_at is null;
--rollback drop table outbox;
