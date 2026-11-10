package com.demp.platform.inbox;

import com.demp.platform.messaging.EventEnvelope;
import com.demp.platform.messaging.MessageContext;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Inbox pattern: {@code INSERT ... ON CONFLICT DO NOTHING} into inbox and the business change share one transaction.
 * <ul>
 *   <li>row already exists -> duplicate, the handler is not invoked;</li>
 *   <li>handler fails -> the inbox row is rolled back too, so redelivery processes the message again.</li>
 * </ul>
 * The insert also serialises two concurrent deliveries of the same message: the second one blocks on the unique
 * index until the first transaction finishes (that is what the old "does a Workflow already exist?" check lacked).
 */
@Slf4j
@RequiredArgsConstructor
public class IdempotentConsumer {

    private static final String INSERT =
            "insert into inbox (consumer_id, event_id) values (?, ?) on conflict (consumer_id, event_id) do nothing";

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final MeterRegistry meters;

    /** @return true if the handler ran, false if the message was a duplicate */
    public boolean handle(String consumerId, EventEnvelope envelope, Consumer<EventEnvelope> handler) {
        try (MessageContext.Scope ignored = MessageContext.open(envelope)) {
            Boolean processed = tx.execute(status -> {
                if (jdbc.update(INSERT, consumerId, envelope.id()) == 0) {
                    return false;
                }
                handler.accept(envelope);
                return true;
            });
            boolean ok = Boolean.TRUE.equals(processed);
            meters.counter("inbox_messages_total", "consumer", consumerId, "type", envelope.type(),
                    "result", ok ? "processed" : "duplicate").increment();
            if (ok) {
                // end-to-end delivery latency: from the moment the producer wrote the event to the outbox
                Timer.builder("event_delivery_latency")
                        .description("Time from occurredAt to the end of processing by the consumer")
                        .tag("consumer", consumerId)
                        .publishPercentileHistogram()
                        .register(meters)
                        .record(Duration.between(envelope.occurredAt(), Instant.now()).abs());
            } else {
                log.info("Duplicate message skipped: consumer={} type={} id={}", consumerId, envelope.type(), envelope.id());
            }
            return ok;
        }
    }
}
