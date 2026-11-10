# Runbook: ServiceDown / HighHttp5xxRate

**Что значит:** Prometheus не может собрать метрики сервиса дольше 2 минут либо больше 5% запросов завершаются 5xx.

**Влияние:** сообщения не теряются — они ждут в Kafka (и в outbox отправителей). Растёт задержка обработки;
для resolution-service не проверяются SLA и не идут сверки UNKNOWN.

**Проверить**

1. `kubectl -n demp-<env> get pods -l app.kubernetes.io/name=<service>` — статус, рестарты.
2. `kubectl -n demp-<env> describe pod <pod>` — OOMKilled, ошибки probe, ImagePull.
3. `kubectl -n demp-<env> logs <pod> --previous` — причина падения.
4. Если поды `Running`, но не `Ready`: `/actuator/health/readiness` — что именно недоступно (БД, Kafka).
5. Совпадает ли по времени с выкладкой: `helm -n demp-<env> history <service>`.

**Действия**

- Началось после выкладки → `helm -n demp-<env> rollback <service> --wait`. Схему БД откат не возвращает.
- Не стартует на миграции → `select * from databasechangeloglock;` (блокировка от убитого пода снимается вручную, убедившись, что миграция не идёт).
- OOM → поднять `resources.limits.memory` в values окружения, выяснить причину.
- Недоступна БД или Kafka → эскалация платформенной команде.

**После:** убедиться, что consumer lag и `outbox_pending_messages` убывают.
