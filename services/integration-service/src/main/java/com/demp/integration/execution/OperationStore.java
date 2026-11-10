package com.demp.integration.execution;

import com.demp.integration.domain.IntegrationAttempt;
import com.demp.integration.domain.IntegrationOperation;
import com.demp.integration.domain.Repositories;
import com.demp.integration.messaging.IntegrationEvents;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Short transactions around the external call. The HTTP call itself is NEVER made inside a DB transaction:
 * a slow external system must not hold connections and row locks.
 */
@Component
@RequiredArgsConstructor
public class OperationStore {

    public enum Begin {
        /** first delivery: go on with the external call */
        STARTED,
        /** redelivery of an already finished operation: nothing to do, the result event is already in the outbox */
        DUPLICATE,
        /** redelivery of an operation that crashed between "registered" and "result stored" */
        INTERRUPTED
    }

    public record Started(Begin begin, IntegrationOperation operation) {
    }

    private final Repositories.Operations operations;
    private final Repositories.Attempts attempts;
    private final IntegrationEvents events;
    private final Clock clock;

    @Transactional
    public Started begin(UUID id, Supplier<IntegrationOperation> factory) {
        Optional<IntegrationOperation> existing = operations.findById(id);
        if (existing.isPresent()) {
            return new Started(existing.get().getStatus() == IntegrationOperation.Status.RECEIVED
                    ? Begin.INTERRUPTED : Begin.DUPLICATE, existing.get());
        }
        return new Started(Begin.STARTED, operations.saveAndFlush(factory.get()));
    }

    @Transactional
    public void recordAttempt(UUID operationId, int attemptNo, TransportClassifier.Classified classified,
                              Integer successStatus, long durationMs, Instant startedAt) {
        attempts.save(new IntegrationAttempt(operationId, attemptNo, classified.outcome().name(),
                classified.httpStatus() != null ? classified.httpStatus() : successStatus, classified.reason(),
                durationMs, startedAt));
    }

    /** State change of the operation and its result event are committed atomically (outbox). */
    @Transactional
    public void finish(UUID operationId, BiFunction<IntegrationOperation, Instant, Published> change) {
        IntegrationOperation op = operations.findById(operationId).orElseThrow();
        Published published = change.apply(op, clock.instant());
        if (published != null) {
            events.publish(op, published.type(), published.payload());
        }
    }

    public record Published(String type, Object payload) {
    }
}
