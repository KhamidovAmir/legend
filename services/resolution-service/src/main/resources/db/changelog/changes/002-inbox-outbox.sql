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
-- APPEND-ONLY. No status column, nobody selects or updates rows here: Debezium reads the inserts from the WAL
-- (Outbox Event Router) and publishes them to the topic named in "destination".
-- This table is a CONTRACT with the connector config (deploy/debezium): changing columns requires a senior/lead approval.
create table outbox (
    id                uuid primary key,           -- event id -> Kafka header "id"
    aggregate_type    text        not null,
    aggregate_id      text        not null,       -- Kafka record key
    aggregate_version bigint      not null,
    destination       text        not null,       -- Kafka topic
    event_type        text        not null,
    event_version     int         not null,
    payload           jsonb       not null,
    correlation_id    text,
    causation_id      text,
    trace_parent      text,
    created_at        timestamptz not null
);
-- only for the retention job and for incremental re-publication "where created_at >= ..."
create index ix_outbox_created_at on outbox (created_at);
--rollback drop table outbox;
