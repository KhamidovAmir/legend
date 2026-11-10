# Runbook: UnknownResultTooOld / FalseNotAppliedDetected / UnknownRateHigh / ExternalSystemCircuitOpen

## FalseNotAppliedDetected (critical)

**Что значит:** мы объявили команду неприменённой, а позже пришёл её успешный результат. Если после этого была
вторая попытка — возможно **двойное исполнение** (например, две повторные доставки).

**Действия — в этом порядке**

1. Отключить автоповтор для системы (hotfix-флаг, без выкладки):
   ```sql
   update reconciliation_policy set auto_retry_enabled = false where target_system = '<SYSTEM>';
   ```
   С этого момента подтверждённый `NOT_APPLIED` уходит оператору.
2. Найти затронутые исключения: work item-ы типа `CONFLICT_REVIEW`; у действия больше одной попытки и первая — `COMPLETED`.
3. Проверить и отменить лишние действия во внешней системе вручную.
4. Выяснить причину: изменилась ли внешняя система (асинхронная запись, отстающее чтение)? Достаточны ли
   `settle_delay_seconds` и `not_applied_confirmations`?
5. Флаг возвращать только после периода наблюдения без ложных `NOT_APPLIED`.

## UnknownResultTooOld / UnknownRateHigh

**Что значит:** внешняя система не отвечает на команды вовремя, сверки не завершаются.

1. Дашборд, блок «External systems»: latency, доля UNKNOWN, состояние breaker.
2. `GET /api/v1/exceptions/{id}/resolution` — история проверок конкретного исключения.
3. Система деградирует → сообщить её команде. Наша нагрузка при этом не растёт: при открытом breaker запросы не
   отправляются, интервал проверок увеличивается.
4. Доля запросов в трафике высокая (`QueryShareOfExternalTrafficHigh`) → увеличить `base_interval_seconds` для системы.
5. По истечении `deadline_minutes` задачи сами уходят операторам (`RECONCILIATION_EXPIRED`) — следить за очередью work item-ов.

## ExternalSystemCircuitOpen

Breaker открыт: команды завершаются `IntegrationFailed(CIRCUIT_OPEN, retryable)` — они точно не отправлены, и
Resolution расходует на них бизнес-попытки. При долгой недоступности исключения уйдут в ручную обработку
(`ACTION_FAILED`). Это известное ограничение текущей реализации: откладывание команд до закрытия breaker-а не сделано.
