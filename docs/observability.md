# Наблюдаемость

Цель из исходного описания: проблема должна быть видна **мониторингом, а не по жалобам**, и должно быть понятно,
**на каком слое** замедлился поток событий.

## Логи

- Формат: JSON (ECS) в stdout — `LOGGING_STRUCTURED_FORMAT_CONSOLE=ecs` в контейнере; локально из IDE — обычный текст.
- В каждой строке через MDC: `correlationId`, `causationId`, `eventId`, `exceptionId`, а также `traceId` / `spanId`.
  Их выставляют `MessageContext` и `IdempotentConsumer` из `platform-common`.
- `correlationId` рождается во внешнем событии и копируется во все сообщения цепочки; `causationId` = id сообщения,
  обработка которого породила текущее.
- Сбор (Fluent Bit → OpenSearch) в репозитории не настроен.

Поиск цепочки: по `correlationId` — все строки всех сервисов по одной проблеме; по `causationId` — что чем вызвано.

## Трейсы

Micrometer Tracing + OpenTelemetry, экспорт OTLP (`OTLP_TRACING_ENDPOINT`). Контекст передаётся в заголовке
`traceparent`. Особенность outbox: сообщение публикуется позже и другим потоком (или Debezium), поэтому
`traceparent` сохраняется в строке outbox и восстанавливается в заголовке — трейс не рвётся на границе сервисов.
Доля сэмплирования задаётся на окружение (`TRACING_SAMPLING_PROBABILITY`: 1.0 на dev, 0.05 на prod).

## Метрики

Actuator + Micrometer → `/actuator/prometheus`. Собственные метрики:

| Метрика | Сервис | Вопрос, на который отвечает |
|---|---|---|
| `exceptions_registration_total{outcome}` | exception | сколько создано / привязано / проигнорировано / дублей |
| `outbox_pending_messages` | exception, integration | сколько событий ждёт публикации (polling) |
| `outbox_messages_written_total` | все с outbox | поток исходящих по топикам и типам |
| `inbox_messages_total{result}` | все с inbox | доля дублей (Inbox duplicate rate) |
| `event_delivery_latency_seconds` | все потребители | время от `occurredAt` до конца обработки |
| `kafka_dead_letter_total{topic}` | все | сообщения, ушедшие в DLT |
| `resolution_actions_unknown_total{system}` | resolution | количество UNKNOWN |
| `reconciliation_oldest_unknown_age_seconds{system}` | resolution | возраст самого старого UNKNOWN |
| `reconciliation_checks_total{system,result}` | resolution | исходы проверок |
| `reconciliation_false_not_applied_total{system}` | resolution | ложные NOT_APPLIED |
| `resolution_work_items_created_total{type}` | resolution | объём ручной работы и её причины |
| `resolution_sla_breaches_total` | resolution | нарушения SLA |
| `integration_external_requests_total{system,kind,outcome}` | integration | исходы вызовов; доля запросов в трафике |
| `integration_external_request_duration_seconds` | integration | latency внешних систем |
| `resilience4j_circuitbreaker_state` | integration | открыт ли breaker |

Для resolution-service «сколько событий ждёт» по таблице посчитать нельзя (она append-only, статуса нет):
отставание — это lag слота репликации (postgres_exporter) и `event_delivery_latency_seconds` у потребителей.

## Где замедлилось

| Слой | Сигнал | Алерт |
|---|---|---|
| Сервис не пишет в outbox | падает `outbox_messages_written_total` | — (видно на дашборде) |
| Polling relay | растёт `outbox_pending_messages` | `OutboxBacklogGrowing` |
| Debezium / слот | растёт lag слота, слот неактивен | `ReplicationSlotLagHigh`, `ReplicationSlotInactive` |
| Kafka → потребитель | растёт consumer lag | `ConsumerLagHigh` |
| Обработчик | DLT, рост latency при нулевом lag | `DeadLettersAppeared`, `EventDeliveryLatencyHigh` |
| Внешняя система | UNKNOWN, открытый breaker | `UnknownRateHigh`, `ExternalSystemCircuitOpen` |

## Алерты

`deploy/observability/alerts.yml` — единый источник: монтируется в локальный Prometheus и превращается
в `PrometheusRule`. Маршрутизация — Alertmanager → входящий webhook Mattermost
(`deploy/local/alertmanager/alertmanager.yml`, URL — заглушка). У каждого алерта аннотация `runbook`.

Пороги в правилах — отправные значения, а не измеренные: их подбирают по фактическому профилю нагрузки.
