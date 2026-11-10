--liquibase formatted sql

--changeset demp:001-policies
create table resolution_policy (
    id                    uuid primary key,
    code                  text    not null,
    version               int     not null,
    exception_type        text    not null,
    execution_mode        text    not null,      -- EXTERNAL_AUTOMATED | MANUAL
    action_type           text,
    target_system         text,
    expected_state        text,                  -- external state that proves the problem is gone
    resolution_result     text,
    bot_id                text,
    max_business_attempts int     not null default 1,
    sla_minutes           int     not null,
    enabled               boolean not null default true,
    constraint ux_resolution_policy unique (code, version)
);
-- Settings per "system + action type": how to check, how long to wait, whether auto-retry is allowed.
create table reconciliation_policy (
    id                        uuid primary key,
    target_system             text    not null,
    action_type               text    not null,
    settle_delay_seconds      int     not null,  -- first check is postponed: async systems show the change with a lag
    base_interval_seconds     int     not null,
    max_interval_seconds      int     not null,
    query_timeout_seconds     int     not null,  -- no answer to QueryExternalState for this long -> ask again
    not_applied_confirmations int     not null,  -- NOT_APPLIED must be seen this many times in a row
    deadline_minutes          int     not null,
    auto_retry_enabled        boolean not null,  -- feature flag: false = confirmed NOT_APPLIED goes to an operator
    processing_states         text    not null default '',
    constraint ux_reconciliation_policy unique (target_system, action_type)
);
--rollback drop table reconciliation_policy; drop table resolution_policy;

--changeset demp:001-workflow
create table workflow_instance (
    id                            uuid primary key,
    exception_id                  uuid        not null,
    policy_code                   text,
    policy_version                int,
    status                        text        not null,
    entity_type                   text        not null,
    entity_id                     text        not null,
    resolution_result             text,
    pending_verification_query_id uuid,
    verification_query_failures   int         not null default 0,
    correlation_id                text,
    revision                      bigint      not null,
    version                       bigint      not null,
    created_at                    timestamptz not null,
    updated_at                    timestamptz not null,
    completed_at                  timestamptz,
    -- one workflow per exception: the database, not a "does it exist?" check, protects from duplicates
    constraint ux_workflow_exception unique (exception_id)
);
create index ix_workflow_verification_query on workflow_instance (pending_verification_query_id)
    where pending_verification_query_id is not null;

create table assignment (
    id            uuid primary key,
    exception_id  uuid        not null,
    assignee_type text        not null,           -- BOT | TEAM | OPERATOR
    assignee_id   text        not null,
    active        boolean     not null,
    created_at    timestamptz not null
);
create index ix_assignment_exception on assignment (exception_id);

create table action (
    id             uuid primary key,
    workflow_id    uuid        not null references workflow_instance (id),
    exception_id   uuid        not null,
    action_type    text        not null,
    target_system  text        not null,
    execution_mode text        not null,
    status         text        not null,
    entity_type    text        not null,
    entity_id      text        not null,
    expected_state text,
    max_attempts   int         not null,
    version        bigint      not null,
    created_at     timestamptz not null,
    updated_at     timestamptz not null
);
create index ix_action_workflow on action (workflow_id);

-- One row = one BUSINESS attempt. Transport retries live in integration-service.
create table action_attempt (
    id              uuid primary key,
    action_id       uuid        not null references action (id),
    exception_id    uuid        not null,
    attempt_no      int         not null,
    status          text        not null,
    operation_id    uuid        not null,
    idempotency_key text        not null,
    outcome_result  text,
    failure_reason  text,
    version         bigint      not null,
    created_at      timestamptz not null,
    finished_at     timestamptz,
    constraint ux_action_attempt_no unique (action_id, attempt_no),
    constraint ux_action_attempt_operation unique (operation_id)
);
--rollback drop table action_attempt; drop table action; drop table assignment; drop table workflow_instance;

--changeset demp:001-reconciliation
-- Technical state of checking an UNKNOWN attempt lives in its own small table,
-- so the reconciliation worker never competes with the workflow for the optimistic lock of Action.
create table reconciliation_task (
    id                 uuid primary key,
    attempt_id         uuid        not null references action_attempt (id),
    action_id          uuid        not null,
    exception_id       uuid        not null,
    target_system      text        not null,
    action_type        text        not null,
    entity_type        text        not null,
    entity_id          text        not null,
    status             text        not null,      -- READY | QUERYING | RESOLVED | ESCALATED | CANCELLED
    result             text,                      -- APPLIED | NOT_APPLIED | STILL_PROCESSING | INCONCLUSIVE | CONFLICT
    reason             text        not null,
    checks_count       int         not null default 0,
    not_applied_streak int         not null default 0,
    current_query_id   uuid,
    next_check_at      timestamptz not null,
    deadline_at        timestamptz not null,
    version            bigint      not null,
    created_at         timestamptz not null,
    updated_at         timestamptz not null
);
-- at most one active task per attempt
create unique index ux_reconciliation_active_attempt on reconciliation_task (attempt_id) where status in ('READY', 'QUERYING');
-- the worker polls only this small partial index
create index ix_reconciliation_due on reconciliation_task (next_check_at) where status in ('READY', 'QUERYING');
create index ix_reconciliation_query on reconciliation_task (current_query_id) where current_query_id is not null;
--rollback drop table reconciliation_task;

--changeset demp:001-outcomes
create table resolution_verification (
    id             uuid primary key,
    exception_id   uuid        not null,
    action_id      uuid,
    query_id       uuid,
    expected_state text,
    actual_state   text,
    status         text        not null,          -- PASSED | FAILED
    created_at     timestamptz not null
);
create table resolution (
    id                  uuid primary key,
    exception_id        uuid        not null unique,
    result              text        not null,
    verification_status text        not null,     -- VERIFIED | MANUAL
    resolved_by         text        not null,
    created_at          timestamptz not null
);
create table sla_instance (
    id           uuid primary key,
    exception_id uuid        not null unique,
    status       text        not null,            -- ACTIVE | MET | BREACHED | CANCELLED
    deadline_at  timestamptz not null,
    breached_at  timestamptz,
    version      bigint      not null,
    created_at   timestamptz not null
);
create index ix_sla_active_deadline on sla_instance (deadline_at) where status = 'ACTIVE';
-- escalation is a separate fact; the exception status never becomes "ESCALATED"
create table escalation (
    id           uuid primary key,
    exception_id uuid        not null,
    reason       text        not null,
    created_at   timestamptz not null
);
create table work_item (
    id           uuid primary key,
    exception_id uuid        not null,
    type         text        not null,
    status       text        not null,            -- OPEN | DONE | CANCELLED
    description  text,
    completed_by text,
    version      bigint      not null,
    created_at   timestamptz not null,
    completed_at timestamptz
);
create index ix_work_item_open on work_item (created_at) where status = 'OPEN';
create index ix_work_item_exception on work_item (exception_id);
--rollback drop table work_item; drop table escalation; drop table sla_instance; drop table resolution; drop table resolution_verification;
