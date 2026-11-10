# Kubernetes и Helm

Один общий чарт `deploy/helm/demp-service` и values на сервис и окружение (`services/<name>/deploy/values*.yaml`).
Альтернатива — отдельный чарт у каждого сервиса: больше свободы, но четыре копии одних и тех же шаблонов.

```bash
make helm-lint
```

```bash
make helm-template ENV=prod
```

## Что рендерится и зачем

| Ресурс | Ключевое решение | Какую проблему закрывает |
|---|---|---|
| Deployment | `maxUnavailable: 0`, `maxSurge: 1` | при выкладке мощность не падает ниже текущей |
| | startup / liveness / readiness на `/actuator/health/*` | startup даёт время миграциям; liveness не зависит от БД и Kafka — иначе сбой БД перезапускал бы все поды разом |
| | `preStop: sleep 10` + `server.shutdown: graceful` + `terminationGracePeriodSeconds: 45` | под убирается из endpoints не мгновенно; запросы и обработчики Kafka успевают завершиться |
| | `checksum/config` в аннотациях | изменение ConfigMap перезапускает поды (env читается только при старте) |
| | `runAsNonRoot`, `readOnlyRootFilesystem`, `drop: ALL` | базовое усиление безопасности; `/tmp` — `emptyDir` |
| | requests без CPU limit | CPU-throttling бьёт по latency JVM; спорное решение, в части команд лимит обязателен политикой кластера |
| | `topologySpreadConstraints` | реплики на разных узлах |
| ConfigMap | несекретные настройки → env | адреса БД, Kafka, внешних систем на окружение |
| ExternalSecret | Vault → Secret → env | секретов нет ни в репозитории, ни в переменных CI |
| HPA | только integration-service на preprod/prod | см. ниже |
| PodDisruptionBudget | `minAvailable: 1` | обслуживание узлов не уносит все реплики |
| ServiceMonitor | `/actuator/prometheus` | Prometheus Operator находит сервис сам |
| NetworkPolicy | вход только из ingress и monitoring | сервисы не ходят друг к другу по HTTP |
| Ingress | только `/api` | actuator наружу не публикуется |

## Масштабирование

Параллелизм потребителя Kafka ограничен числом партиций, поэтому «добавить реплик» помогает только до этого предела.
HPA по CPU включён лишь у integration-service (он ждёт внешние вызовы и держит потоки). Для остальных CPU — слабый
сигнал; распространённый вариант — KEDA со скейлингом по consumer lag (здесь не реализован).

Фоновые воркеры (reconciliation, SLA, polling outbox, закрытие) работают на **всех** репликах: задачи распределяет
`FOR UPDATE SKIP LOCKED`, лидер и ShedLock не нужны.

## Миграции при выкладке

Liquibase запускается при старте приложения; от параллельного старта реплик защищает его блокировка
(`databasechangeloglock`). Последствия: долгая миграция задерживает старт (её покрывает startup probe —
до 5 минут), а под, убитый посреди миграции, может оставить блокировку (снимается вручную). Альтернатива —
отдельный Job / pre-upgrade hook: миграция видна как отдельный шаг, приложению не нужны права DDL; цена — ещё
один объект и порядок в пайплайне.

## Платформенные ресурсы (`deploy/platform`)

- `kafka-topics.yaml` — топики как код (Strimzi `KafkaTopic`), `min.insync.replicas: 2`, DLT, heartbeat-топик.
- `debezium-connector.yaml` — `KafkaConnector`; генерируется из JSON в репозитории сервиса, CI проверяет расхождение.
- `prometheus-rule.yaml` — генерируется из `deploy/observability/alerts.yml`.

Имена кластеров (`demp-kafka`, `demp-connect`), путь к секрету CDC и хосты — заглушки.
