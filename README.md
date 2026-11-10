# DEMP — платформа управления проблемными отправлениями (учебная реализация)

Четыре микросервиса на Java 21 / Spring Boot 3.5, PostgreSQL, Kafka, Debezium CDC, с пайплайнами (GitHub Actions
и GitLab CI), Helm-чартом для Kubernetes, метриками, трейсами, алертами и JaCoCo.

Это **учебный каркас**, а не копия боевой системы: реализован сквозной вертикальный срез и общие механизмы,
часть предметной модели сознательно опущена. Что именно сделано, что изменено относительно исходного описания
и что не проверялось — в [docs/deviations.md](docs/deviations.md).

```
внешняя система ──Kafka──▶ exception-service ──events──▶ resolution-service ──commands──▶ integration-service ──REST──▶ внешняя система
   (Delivery)              (что случилось,               (как решать: workflow,          (как вызвать: retry,
                            статус)        ◀──commands──  SLA, reconciliation) ◀─events── timeout, UNKNOWN)
                                  │                              │                               │
                                  └──────────────events──────────┴───────────────────────────────┴──▶ analytics-service
```

## Быстрый старт

Нужны: JDK 21, Maven 3.9+, Docker с Compose.

```bash
make verify
```

```bash
make up
```

```bash
make run-exception-service
```

(в отдельных терминалах так же `run-resolution-service`, `run-integration-service`, `run-analytics-service`;
либо всё в контейнерах: `make up-apps`)

```bash
make connector
```

```bash
scripts/demo.sh failed SHP-1
```

```bash
scripts/demo.sh show SHP-1
```

Подробный сценарий с ожидаемым результатом каждого шага — [docs/local-development.md](docs/local-development.md).

| Что | Адрес |
|---|---|
| Swagger сервисов | http://localhost:8081/swagger-ui.html (…8082, 8083, 8084) |
| Kafka UI | http://localhost:8090 |
| Kafka Connect REST | http://localhost:8088/connectors |
| Grafana (дашборд «DEMP overview») | http://localhost:3000 |
| Prometheus / алерты | http://localhost:9090/alerts |
| Jaeger (трейсы) | http://localhost:16686 |
| Заглушка Delivery (WireMock) | http://localhost:8089/__admin/mappings |

## Карта репозитория

| Путь | Что там | Кто владеет в реальной команде |
|---|---|---|
| `libs/contracts` | топики, типы событий, payload-ы (Java records) | seniors + аналитики |
| `libs/platform-common` | outbox, inbox, конверт сообщения, обработка ошибок Kafka | seniors |
| `services/exception-service` | регистрация, дедупликация, классификация, lifecycle | backend |
| `services/resolution-service` | workflow, reconciliation, SLA, work items; outbox через Debezium | backend |
| `services/integration-service` | адаптеры внешних систем, UNKNOWN, circuit breaker, rate limit | backend |
| `services/analytics-service` | модель фактов из событий, отчёты | backend |
| `services/*/deploy` | values Helm на каждое окружение, конфиг коннектора | backend + devops |
| `deploy/helm/demp-service` | общий Helm-чарт | devops |
| `deploy/platform` | топики Kafka, коннектор, PrometheusRule (как код) | платформенная команда |
| `deploy/observability` | алерты и дашборд Grafana | backend |
| `deploy/local` | docker-compose локального стенда | backend |
| `.gitlab-ci.yml`, `.github/workflows` | пайплайны | devops + seniors |
| `contracts/asyncapi.yaml` | каталог каналов для людей | аналитики |
| `docs` | архитектура, ADR, runbook-и, план изучения | все |

## Документация

1. [docs/architecture.md](docs/architecture.md) — сервисы, потоки, гарантии доставки.
2. [docs/local-development.md](docs/local-development.md) — локальный стенд и сценарии.
3. [docs/testing.md](docs/testing.md) — пирамида тестов, JaCoCo, что чем проверяется.
4. [docs/ci-cd.md](docs/ci-cd.md) — пайплайны, окружения, релизы, откат, хотфикс; перенос на GitLab.
5. [docs/kubernetes.md](docs/kubernetes.md) — что рендерит чарт и почему.
6. [docs/observability.md](docs/observability.md) — логи, метрики, трейсы, алерты.
7. [docs/adr](docs/adr) — принятые решения с альтернативами.
8. [docs/runbooks](docs/runbooks) — что делать по каждому алерту.
9. [docs/deviations.md](docs/deviations.md) — отличия от исходного описания, границы реализации и проверки.
10. [docs/learning-path.md](docs/learning-path.md) — в каком порядке это изучать и что ломать руками.
