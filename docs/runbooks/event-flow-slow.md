# Runbook: OutboxBacklogGrowing / ConsumerLagHigh / EventDeliveryLatencyHigh / InboxDuplicateRateHigh

**Что значит:** события доходят медленно. Задача — найти слой.

| Сигнал | Слой | Дальше |
|---|---|---|
| `outbox_pending_messages` растёт (exception, integration) | polling relay | раздел 1 |
| lag слота растёт (resolution) | CDC | [debezium-connector.md](debezium-connector.md) |
| consumer lag растёт, outbox и слот в норме | потребитель | раздел 2 |
| lag нулевой, latency высокая | медленный обработчик или внешняя система | раздел 3 |

## 1. Polling relay

- В логах `Outbox batch was not acknowledged by Kafka` → недоступна Kafka (проверить брокеры, `min.insync.replicas`).
- Kafka в порядке, backlog растёт → входящий поток выше пропускной способности: поднять
  `platform.outbox.polling.batch-size`, уменьшить `interval`. Если это стало нормой — аргумент за перевод на CDC ([ADR-0001](../adr/0001-transactional-outbox-cdc.md)).

## 2. Потребитель

- Поды потребителя живы? Идёт ли бесконечный rebalance (в логах `Request joining group`)?
- Растёт `kafka_dead_letter_total` → потребитель тратит время на ретраи: [dead-letters.md](dead-letters.md).
- Lag в одной партиции → «горячий» ключ или зависшее сообщение.
- Масштабирование помогает только до числа партиций топика.

## 3. Обработчик

- `hikaricp_connections_pending` > 0 → не хватает соединений с БД.
- Медленные запросы → `pg_stat_statements`.
- Для integration-service: latency внешней системы, открытый circuit breaker — [unknown-results.md](unknown-results.md).

## InboxDuplicateRateHigh

Дубли безвредны (их гасит inbox), но показывают, что кто-то публикует повторно: рестарты коннектора, откаты
транзакций в polling relay, rebalance потребителей до коммита offset. Искать совпадение по времени.
