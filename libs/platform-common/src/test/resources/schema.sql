create table if not exists inbox (
    consumer_id  text        not null,
    event_id     uuid        not null,
    processed_at timestamptz not null default now(),
    primary key (consumer_id, event_id)
);
create table if not exists outbox (
    id uuid primary key, aggregate_type text not null, aggregate_id text not null, aggregate_version bigint not null,
    destination text not null, event_type text not null, event_version int not null, payload jsonb not null,
    correlation_id text, causation_id text, trace_parent text, created_at timestamptz not null,
    published_at timestamptz
);
create table if not exists business (id text primary key);
