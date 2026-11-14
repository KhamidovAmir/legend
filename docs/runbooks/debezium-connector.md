# Runbook: ReplicationSlotLagHigh / ReplicationSlotInactive

**Что значит:** Debezium не успевает читать или не читает WAL базы resolution-service. Сообщения Resolution
(команды в Exception и Integration, события) не доходят до Kafka.

**Влияние:** новые исключения застревают в `CLASSIFIED`; сверки и верификации не отправляются; SLA нарушаются.
Данные не теряются: они в таблице `outbox` и в WAL. WAL при этом растёт — риск для диска БД.

**Проверить**

1. Состояние коннектора: `GET <connect>/connectors/resolution-outbox/status` (или `kubectl get kafkaconnector resolution-outbox -o yaml`).
2. Слот:
   ```sql
   select slot_name, active, wal_status,
          pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) as lag
     from pg_replication_slots;
   ```
3. Логи Kafka Connect: ошибки задачи, `UNKNOWN_TOPIC_OR_PARTITION`.

**Действия**

| Наблюдение | Причина | Действие |
|---|---|---|
| task `FAILED` | ошибка в конфиге, недоступна БД/Kafka, изменилась таблица `outbox` | исправить причину, `POST /connectors/resolution-outbox/tasks/0/restart` |
| task `RUNNING`, слот активен, lag растёт, в логах `UNKNOWN_TOPIC_OR_PARTITION` | нет топика назначения или heartbeat-топика (auto-create выключен) | создать топик (`deploy/platform/kafka-topics.yaml`) |
| слот `active = false` | коннектор остановлен или удалён | запустить; если коннектор больше не нужен — удалить слот, иначе WAL будет расти |
| `wal_status = lost` | сработал `max_slot_wal_keep_size`, WAL удалён | новый слот + повторная публикация строк outbox за окно потери (incremental snapshot по `created_at`); дубли погасит inbox |
| после failover PostgreSQL слота нет | слоты не переезжают на реплику без специальной настройки | то же, что при `lost` |

**Не делать:** не удалять слот «чтобы освободить диск», не выяснив, что с коннектором — это гарантированная потеря
окна событий.

**После:** слот `active = t`, lag перестаёт расти и колеблется около обычного для стенда уровня,
`event_delivery_latency_seconds` возвращается к норме, застрявшие исключения двигаются дальше.

Lag слота не бывает нулевым и на здоровой системе (на локальном стенде наблюдалось 100–200 кБ): журнал общий для
всего экземпляра PostgreSQL и пополняется постоянно, а позицию `confirmed_flush_lsn` Debezium подтверждает
периодически, а не на каждое событие. Признак проблемы — не величина сама по себе, а **неограниченный рост**.
