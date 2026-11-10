package com.demp.platform.outbox;

import com.demp.platform.messaging.MessageContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Transactional outbox: the message row is written in the SAME database transaction as the business change.
 * How the row reaches Kafka is not this class's concern: Debezium CDC (resolution-service) or
 * {@link PollingOutboxRelay} (other services).
 */
@RequiredArgsConstructor
public class OutboxWriter {

    private static final String INSERT = """
            insert into outbox (id, aggregate_type, aggregate_id, aggregate_version, destination, event_type,
                                event_version, payload, correlation_id, causation_id, trace_parent, created_at)
            values (?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?)
            """;

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final Clock clock;
    private final MeterRegistry meters;
    /** null when tracing is not on the classpath */
    private final Tracer tracer;

    /** MANDATORY: writing an outbox row outside of a business transaction is always a bug. */
    @Transactional(propagation = Propagation.MANDATORY)
    public UUID write(OutboxMessage message) {
        UUID id = message.id() != null ? message.id() : UUID.randomUUID();
        String correlationId = message.correlationId() != null ? message.correlationId() : MessageContext.correlationId();
        String causationId = message.causationId() != null ? message.causationId() : MessageContext.currentMessageId();
        jdbc.update(INSERT,
                id,
                message.aggregateType(),
                message.aggregateId(),
                message.aggregateVersion(),
                message.destination(),
                message.eventType(),
                message.eventVersion() == null ? 1 : message.eventVersion(),
                toJson(message.payload()),
                correlationId != null ? correlationId : id.toString(),
                causationId,
                currentTraceParent(),
                OffsetDateTime.now(clock));
        meters.counter("outbox_messages_written_total",
                "destination", message.destination(), "type", message.eventType()).increment();
        return id;
    }

    private String toJson(Object payload) {
        try {
            return mapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Outbox payload is not serializable", e);
        }
    }

    private String currentTraceParent() {
        if (tracer == null) {
            return null;
        }
        Span span = tracer.currentSpan();
        if (span == null) {
            return null;
        }
        boolean sampled = Boolean.TRUE.equals(span.context().sampled());
        return "00-%s-%s-%s".formatted(span.context().traceId(), span.context().spanId(), sampled ? "01" : "00");
    }
}
