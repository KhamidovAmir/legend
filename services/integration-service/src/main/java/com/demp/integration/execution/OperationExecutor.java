package com.demp.integration.execution;

import com.demp.contracts.IntegrationMessages;
import com.demp.contracts.IntegrationMessages.ExecuteAction;
import com.demp.contracts.IntegrationMessages.ExternalCommandResult;
import com.demp.contracts.IntegrationMessages.QueryExternalState;
import com.demp.integration.adapter.ExternalSystemAdapter;
import com.demp.integration.adapter.ExternalSystemAdapter.CommandResponse;
import com.demp.integration.adapter.ExternalSystemAdapter.StateResponse;
import com.demp.integration.config.IntegrationProperties;
import com.demp.integration.config.IntegrationProperties.SystemPolicy;
import com.demp.integration.domain.IntegrationOperation;
import com.demp.integration.domain.IntegrationOperation.Kind;
import com.demp.integration.domain.Repositories;
import com.demp.integration.execution.ExternalCallGuard.CallResult;
import com.demp.integration.execution.OperationStore.Begin;
import com.demp.integration.execution.OperationStore.Published;
import com.demp.integration.execution.OperationStore.Started;
import com.demp.integration.execution.TransportClassifier.Outcome;
import com.demp.platform.messaging.MessageContext;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

/** Executes commands and queries against external systems and normalizes the outcome for Resolution Service. */
@Slf4j
@Service
public class OperationExecutor {

    private final Map<String, ExternalSystemAdapter> adapters;
    private final IntegrationProperties properties;
    private final ExternalCallGuard guard;
    private final OperationStore store;
    private final Repositories.Operations operations;
    private final Clock clock;

    public OperationExecutor(List<ExternalSystemAdapter> adapters, IntegrationProperties properties,
                             ExternalCallGuard guard, OperationStore store, Repositories.Operations operations,
                             Clock clock) {
        this.adapters = adapters.stream().collect(Collectors.toMap(ExternalSystemAdapter::system, Function.identity()));
        this.properties = properties;
        this.guard = guard;
        this.store = store;
        this.operations = operations;
        this.clock = clock;
    }

    // ------------------------------------------------------------------ commands

    public void execute(ExecuteAction command) {
        Started started;
        try {
            started = store.begin(command.operationId(), () -> IntegrationOperation.received(command.operationId(),
                    Kind.COMMAND, command.exceptionId(), command.targetSystem(), command.actionType(),
                    command.entityType(), command.entityId(), command.idempotencyKey(), MessageContext.correlationId(),
                    clock.instant()));
        } catch (DataIntegrityViolationException e) {
            log.info("Operation {} is being processed by a parallel consumer, skipping", command.operationId());
            return;
        }
        IntegrationOperation op = started.operation();
        if (started.begin() == Begin.DUPLICATE) {
            log.info("Duplicate command for operation {} ({}) skipped", op.getId(), op.getStatus());
            return;
        }
        if (started.begin() == Begin.INTERRUPTED) {
            // We crashed after registering the operation: the request may or may not have left. Not retried blindly.
            store.finish(op.getId(), (o, now) -> {
                o.unknown("INTERRUPTED", now);
                return new Published(IntegrationMessages.UNKNOWN, new IntegrationMessages.IntegrationUnknown(
                        o.getId(), o.getExceptionId(), o.getTargetSystem(), "INTERRUPTED"));
            });
            return;
        }
        ExternalSystemAdapter adapter = adapters.get(op.getTargetSystem());
        if (adapter == null) {
            fail(op, "UNSUPPORTED_SYSTEM", false);
            return;
        }
        SystemPolicy policy = properties.getSystems().get(op.getTargetSystem());

        for (int attemptNo = 1; ; attemptNo++) {
            Instant startedAt = clock.instant();
            CallResult<CommandResponse> result = guard.call(op.getTargetSystem(), Kind.COMMAND, () -> adapter.execute(op));
            store.recordAttempt(op.getId(), attemptNo, result.classified(),
                    result.success() ? result.value().httpStatus() : null, result.durationMs(), startedAt);
            switch (result.classified().outcome()) {
                case SUCCESS -> {
                    succeed(op, result.value());
                    return;
                }
                case REJECTED -> {
                    fail(op, result.classified().reason(), false);
                    return;
                }
                case UNKNOWN -> {
                    String reason = result.classified().reason();
                    log.warn("Operation {}: result is UNKNOWN ({}), no blind retry", op.getId(), reason);
                    store.finish(op.getId(), (o, now) -> {
                        o.unknown(reason, now);
                        return new Published(IntegrationMessages.UNKNOWN, new IntegrationMessages.IntegrationUnknown(
                                o.getId(), o.getExceptionId(), o.getTargetSystem(), reason));
                    });
                    return;
                }
                case NOT_SENT -> {
                    if (attemptNo >= policy.getMaxCommandAttempts() || guard.isOpen(op.getTargetSystem())) {
                        // certainly not applied -> Resolution may start a new business attempt
                        fail(op, result.classified().reason(), true);
                        return;
                    }
                    pause(policy, attemptNo);
                }
            }
        }
    }

    private void succeed(IntegrationOperation op, CommandResponse response) {
        store.finish(op.getId(), (o, now) -> {
            if (response.completed()) {
                o.completed(response.result(), now);
                return new Published(IntegrationMessages.COMPLETED, new IntegrationMessages.IntegrationCompleted(
                        o.getId(), o.getExceptionId(), o.getTargetSystem(), response.result()));
            }
            // accepted for async processing: business success is NOT declared yet
            o.accepted(response.externalRequestId(), now);
            return new Published(IntegrationMessages.ACCEPTED, new IntegrationMessages.IntegrationAccepted(
                    o.getId(), o.getExceptionId(), o.getTargetSystem(), response.externalRequestId()));
        });
    }

    private void fail(IntegrationOperation op, String reason, boolean retryable) {
        store.finish(op.getId(), (o, now) -> {
            o.failed(reason, now);
            return new Published(IntegrationMessages.FAILED, new IntegrationMessages.IntegrationFailed(
                    o.getId(), o.getExceptionId(), o.getTargetSystem(), reason, retryable));
        });
    }

    // ------------------------------------------------------------------ async results of commands

    /**
     * Result sent by the external system later (Kafka / webhook). Matched by its request id or, for commands whose
     * HTTP response we never saw (UNKNOWN), by our idempotency key echoed back.
     * @return false if the operation is not known (yet)
     */
    public boolean onExternalResult(String system, ExternalCommandResult result) {
        Optional<IntegrationOperation> found = Optional.ofNullable(result.externalRequestId())
                .flatMap(id -> operations.findByTargetSystemAndExternalRequestId(system, id))
                .or(() -> Optional.ofNullable(result.idempotencyKey())
                        .flatMap(key -> operations.findByTargetSystemAndIdempotencyKey(system, key)));
        if (found.isEmpty()) {
            return false;
        }
        store.finish(found.get().getId(), (o, now) -> {
            if (!o.awaitsExternalResult()) {
                return null; // duplicate result
            }
            if ("COMPLETED".equals(result.status())) {
                o.completed(result.result(), now);
                return new Published(IntegrationMessages.COMPLETED, new IntegrationMessages.IntegrationCompleted(
                        o.getId(), o.getExceptionId(), o.getTargetSystem(), result.result()));
            }
            o.failed(result.result(), now);
            return new Published(IntegrationMessages.FAILED, new IntegrationMessages.IntegrationFailed(
                    o.getId(), o.getExceptionId(), o.getTargetSystem(), "EXTERNAL_" + result.status(), false));
        });
        return true;
    }

    // ------------------------------------------------------------------ queries

    /** Returns an OBSERVATION. Whether the action "was applied" is decided by Resolution Service. */
    public void query(QueryExternalState query) {
        Started started;
        try {
            started = store.begin(query.queryId(), () -> IntegrationOperation.received(query.queryId(), Kind.QUERY,
                    query.exceptionId(), query.targetSystem(), query.purpose(), query.entityType(), query.entityId(),
                    null, MessageContext.correlationId(), clock.instant()));
        } catch (DataIntegrityViolationException e) {
            return;
        }
        IntegrationOperation op = started.operation();
        if (started.begin() == Begin.DUPLICATE) {
            return;
        }
        // Begin.INTERRUPTED: a query has no side effect, simply run it again
        ExternalSystemAdapter adapter = adapters.get(op.getTargetSystem());
        if (adapter == null) {
            queryFailed(op, "UNSUPPORTED_SYSTEM");
            return;
        }
        SystemPolicy policy = properties.getSystems().get(op.getTargetSystem());
        for (int attemptNo = 1; ; attemptNo++) {
            Instant startedAt = clock.instant();
            CallResult<StateResponse> result = guard.call(op.getTargetSystem(), Kind.QUERY,
                    () -> adapter.queryState(op.getEntityType(), op.getEntityId()));
            store.recordAttempt(op.getId(), attemptNo, result.classified(), result.success() ? 200 : null,
                    result.durationMs(), startedAt);
            if (result.success()) {
                StateResponse state = result.value();
                store.finish(op.getId(), (o, now) -> {
                    o.completed(state.state(), now);
                    return new Published(IntegrationMessages.STATE_RECEIVED, new IntegrationMessages.ExternalStateReceived(
                            o.getId(), o.getExceptionId(), o.getTargetSystem(), state.found(), state.state(), now));
                });
                return;
            }
            boolean breakerOpen = guard.isOpen(op.getTargetSystem());
            if (result.classified().outcome() == Outcome.REJECTED || attemptNo >= policy.getMaxQueryAttempts() || breakerOpen) {
                // an open breaker is reported at once: Resolution backs off instead of hammering a degraded system
                queryFailed(op, breakerOpen ? "CIRCUIT_OPEN" : result.classified().reason());
                return;
            }
            pause(policy, attemptNo);
        }
    }

    private void queryFailed(IntegrationOperation op, String reason) {
        store.finish(op.getId(), (o, now) -> {
            o.failed(reason, now);
            return new Published(IntegrationMessages.STATE_QUERY_FAILED, new IntegrationMessages.ExternalStateQueryFailed(
                    o.getId(), o.getExceptionId(), o.getTargetSystem(), reason));
        });
    }

    private static void pause(SystemPolicy policy, int attemptNo) {
        try {
            Thread.sleep(policy.getRetryBackoff().toMillis() * attemptNo);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for a transport retry", e);
        }
    }
}
