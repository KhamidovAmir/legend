# План изучения

Порядок — от одного сообщения к системе целиком. У каждого шага: что читать, что запустить, что сломать.

## 1. Одно сообщение от начала до конца

- Читать: [architecture.md](architecture.md), затем `OutboxWriter`, `PollingOutboxRelay`, `IdempotentConsumer`, `EnvelopeReader`.
- Запустить: `InboxOutboxIT`; сценарий 1 из [local-development.md](local-development.md).
- Сломать: уберите `Propagation.MANDATORY` у `OutboxWriter.write` и вызовите его вне транзакции — что изменится в гарантиях?
  Остановите Kafka (`docker stop demp-kafka-1`) посреди сценария — где лежат сообщения и что происходит после старта?

## 2. Идемпотентность и дубли

- Читать: `RegistrationService` (два вида дедупликации), partial unique index в `001-schema.sql` exception-service.
- Запустить: `ExceptionFlowIT.sameExternalEventTwice...`, `ResolutionFlowIT.redeliveredClassifiedEvent...`.
- Сломать: удалите вставку в inbox и отправьте одно событие дважды. Сколько workflow создалось? Что спасает — `UNIQUE(exception_id)`?
- Вопрос: чем inbox отличается от проверки «а есть ли уже такой workflow»?

## 3. CDC

- Читать: [ADR-0001](adr/0001-transactional-outbox-cdc.md), конфиг коннектора, `002-inbox-outbox.sql` resolution-service.
- Запустить: стенд; посмотреть заголовки сообщения в Kafka UI; запрос к `pg_replication_slots`.
- Сломать: остановите Connect (`docker stop demp-connect-1`), пошлите 5 событий, посмотрите lag слота, запустите обратно.
  Удалите heartbeat-топик и перезапустите коннектор — как выглядит «тихая» остановка?

## 4. UNKNOWN и reconciliation

- Читать: [ADR-0002](adr/0002-reconciliation-of-unknown-results.md), `TransportClassifier`, `OperationExecutor`,
  `ReconciliationService`, `ObservationInterpreter`.
- Запустить: сценарии 2 и 3; `ResolutionFlowIT.notApplied_mustBeConfirmed...`.
- Сломать: поставьте `not_applied_confirmations = 1`, `settle_delay_seconds = 0` и повторите сценарий 3 — это и есть инцидент.
- Вопрос: почему решение «применено ли» нельзя отдать в Integration?

## 5. Тесты и покрытие

- Читать: [testing.md](testing.md), корневой `pom.xml` (surefire, failsafe, jacoco).
- Запустить: `make verify`, открыть `services/resolution-service/target/site/jacoco/index.html`.
- Сломать: поднимите `jacoco.minimum.line` до 0.99 — на каком шаге и с каким сообщением упадёт сборка?

## 6. Пайплайн

- Читать: [ci-cd.md](ci-cd.md), `.gitlab-ci.yml` сверху вниз.
- Сделать: включить Actions в репозитории и добиться зелёного `CI`; затем импортировать в GitLab и пройти таблицу «Перенос на GitLab».
- Вопросы: почему test выкатывается кнопкой, а dev автоматически? Почему образ для prod не пересобирается?

## 7. Kubernetes

- Читать: [kubernetes.md](kubernetes.md), шаблоны чарта.
- Запустить: `make helm-template ENV=prod`, прочитать Deployment целиком; развернуть на kind / minikube
  (понадобятся CRD Prometheus Operator и External Secrets либо `--set serviceMonitor.enabled=false --set vault.enabled=false`).
- Сломать: уберите `preStop` и во время `kubectl rollout restart` шлите запросы — будут ли ошибки?

## 8. Наблюдаемость и эксплуатация

- Читать: [observability.md](observability.md), `alerts.yml`, runbook-и.
- Запустить: стенд, дашборд, трейс в Jaeger; найти все строки логов одного исключения по `correlationId`.
- Сломать: остановите коннектор и дождитесь `ReplicationSlotInactive` на http://localhost:9090/alerts; пройдите runbook.

## 9. Доработки для самостоятельной практики

Взяты из раздела «Фоновые задачи» исходного описания и из списка нереализованного:

1. Подключить новое событие WMS: `RegistrationRule` + `ClassificationPolicy` (миграция) + тест.
2. Новая `ResolutionPolicy` с автоматическим сценарием.
3. `SlaPauseInterval`: паузы SLA при статусе `WAITING`.
4. Адаптер второй внешней системы (`ExternalSystemAdapter`) со своим `reconciliation_policy`.
5. `ProblemCase`: группировка исключений.
6. Debezium в Testcontainers: проверка конфига коннектора в пайплайне.
7. Партиционирование `outbox` по времени вместо DELETE.
8. Аутентификация REST (OAuth2 resource server) и аудит действий оператора.
9. Скейлинг потребителей по lag (KEDA).
10. Реакция Resolution на `ExceptionCommandRejected`.
