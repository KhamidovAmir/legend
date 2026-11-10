-- Local only. Database-per-service: no service ever reads another service's database.
create role exception   login password 'exception';
create role integration login password 'integration';
create role analytics   login password 'analytics';
-- REPLICATION: Debezium reads the WAL of resolution_db through a logical replication slot.
-- (On real stands CDC uses a separate role: REPLICATION + SELECT on the outbox table only.)
create role resolution  login replication password 'resolution';

create database exception_db   owner exception;
create database resolution_db  owner resolution;
create database integration_db owner integration;
create database analytics_db   owner analytics;
