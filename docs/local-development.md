# Локальная разработка

Локальный стенд — это docker-compose на ноутбуке, а не кластер: PostgreSQL (4 базы), Kafka (KRaft, один брокер),
Kafka Connect с Debezium, WireMock вместо Delivery, Prometheus, Alertmanager, Grafana, Jaeger, экспортёры, Kafka UI.
Сервисы обычно запускаются из IDE: значения по умолчанию в `application.yml` смотрят на `localhost`.

## Windows

Стенд не требует bash на хосте: разовые шаги (создание топиков, регистрация коннектора) выполняются в контейнерах,
поэтому `make up`, `make up-apps`, `make connector`, `make down` работают из PowerShell.
`scripts/demo.sh` — bash-скрипт: запускайте его из Git Bash (ставится вместе с Git for Windows) или из WSL.

Скрипты хранятся с окончаниями строк LF (`.gitattributes`). Если репозиторий был склонирован до появления этого
файла, рабочие копии могут остаться с CRLF; стенд это переживает, а для запуска скриптов из Git Bash обновите их:

```bash
git rm -r -q --cached .
```

```bash
git reset --hard
```

## Запуск

```bash
make up
```

```bash
make run-exception-service
```

```bash
make run-resolution-service
```

```bash
make run-integration-service
```

```bash
make run-analytics-service
```

```bash
make connector
```

`make connector` выполняется **после** первого старта resolution-service: коннектор создаёт публикацию для таблицы
`outbox`, а её создаёт миграция Liquibase сервиса.

Вариант «всё в контейнерах» (образы собираются из `Dockerfile`):

```bash
make up-apps
```

## Сценарии

Заглушка Delivery (`deploy/local/wiremock/mappings/delivery.json`) меняет поведение по префиксу id отправления.

### 1. Успешное асинхронное разрешение

```bash
scripts/demo.sh failed SHP-1
```

```bash
scripts/demo.sh show SHP-1
```

Ожидается: exception `IN_PROGRESS`, попытка `ACCEPTED`, операция integration `ACCEPTED` с `external_request_id`.
Теперь сыграйте роль Delivery и пришлите асинхронный результат (id возьмите из вывода `show`):

```bash
scripts/demo.sh result DELIVERY-REQ-xxxxxxxx
```

Ожидается: `resolution_verification` = `PASSED`, `resolution` = `VERIFIED`, exception `RESOLVED`,
а через `exceptions.closing.quiet-period` (30 с) — `CLOSED`.

### 2. Timeout → UNKNOWN → reconciliation

```bash
scripts/demo.sh failed TIMEOUT-SHP-2
```

Заглушка не отвечает на команду 10 с при read-timeout 3 с. Ожидается: операция `UNKNOWN / REQUEST_TIMEOUT`,
`reconciliation_task` в `READY`, первая проверка через `settle_delay_seconds` (60 с). `GET /shipments/...`
возвращает `DELIVERY_SCHEDULED` → задача `RESOLVED / APPLIED`, exception решён **без повторной команды**.

### 3. Ложный NOT_APPLIED (сценарий инцидента)

```bash
scripts/demo.sh failed TIMEOUT-STALE-SHP-3
```

Чтение возвращает старое состояние `DELIVERY_FAILED`. Ожидается: три проверки подряд
(`not_applied_confirmations = 3`) с растущим интервалом, затем вторая попытка (`attempt_no = 2`) с новым ключом
идемпотентности. Чтобы воспроизвести hotfix из инцидента, отключите автоповтор и повторите сценарий —
вместо второй попытки появится work item `NOT_APPLIED_REVIEW`:

```bash
docker exec demp-postgres-1 psql -U postgres -d resolution_db -c "update reconciliation_policy set auto_retry_enabled = false"
```

### 4. Отказ внешней системы и ручная обработка

```bash
scripts/demo.sh failed REJECT-SHP-4
```

Ожидается: `IntegrationFailed(HTTP_422, retryable=false)` → work item `ACTION_FAILED`. Закройте его через API:

```bash
curl -s localhost:8082/api/v1/work-items
```

```bash
curl -s -X POST localhost:8082/api/v1/work-items/<id>/complete -H 'Content-Type: application/json' -d '{"outcome":"CLOSED","operator":"me","closeReason":"UNRESOLVABLE"}'
```

### 5. Дубликаты

Отправьте `scripts/demo.sh failed SHP-1` ещё раз, пока исключение активно: новое исключение не создаётся,
в `exception_history` появляется `REPEATED_SIGNAL`, в `inbound_event` — решение `LINKED`.

## Что посмотреть после сценариев

- Kafka UI → топик `integration.commands` → заголовки сообщения: их выставил Debezium Event Router.
- `curl -s localhost:8088/connectors/resolution-outbox/status` — состояние коннектора.
- `select slot_name, active, pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) from pg_replication_slots;`
  в `resolution_db` — отставание слота.
- Grafana → «DEMP overview»; Jaeger → сервис `exception-service` → трейс проходит все четыре сервиса.
- `curl -s localhost:8084/api/v1/analytics/summary` — агрегаты.

## Частые проблемы

| Симптом | Причина | Что делать |
|---|---|---|
| Exception застрял в `CLASSIFIED`/`IN_PROGRESS`, в `outbox` resolution есть строки, в топиках пусто | коннектор не зарегистрирован, упал или нет heartbeat-топика | `make connector`; статус коннектора; [runbook](runbooks/debezium-connector.md) |
| `make connector` падает с ошибкой публикации | resolution-service ещё ни разу не стартовал, таблицы `outbox` нет | запустить сервис, повторить |
| Сервис не стартует: `Schema-validation: missing column` | сущность изменена без миграции | добавить changeset; `ddl-auto: validate` это и ловит |
| `kafka-init … exit 2`, сервисы в статусе `Created` | скрипт создания топиков получен с окончаниями строк CRLF (Windows) | обновить репозиторий: скрипт теперь очищается от `\r` перед запуском |
| Интеграционные тесты не находят Docker | не запущен демон / нет доступа к сокету | `docker info`; unit-тесты идут без Docker: `make unit` |
