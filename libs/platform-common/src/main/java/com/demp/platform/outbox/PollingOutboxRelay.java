package com.demp.platform.outbox;

import com.demp.platform.messaging.MessageHeaders;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Polling publisher for low-volume services: SELECT ... FOR UPDATE SKIP LOCKED -> Kafka -> mark as published.
 * <p>
 * Guarantees: at-least-once (a crash between send and commit re-publishes the batch; consumers dedupe via inbox).
 * SKIP LOCKED lets every replica run the relay without a leader election (no ShedLock needed).
 * Trade-off: with several replicas two batches may interleave, so strict per-aggregate order is not guaranteed;
 * consumers that need it compare aggregateVersion.
 */
@Slf4j
public class PollingOutboxRelay {

    private static final String SELECT_BATCH = """
            select id, aggregate_type, aggregate_id, aggregate_version, destination, event_type, event_version,
                   payload::text as payload, correlation_id, causation_id, trace_parent, created_at
              from outbox
             where published_at is null
             order by created_at, id
             limit ?
               for update skip locked
            """;

    private final JdbcTemplate jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final TransactionTemplate tx;
    private final OutboxProperties.Polling props;
    private final MeterRegistry meters;

    public PollingOutboxRelay(JdbcTemplate jdbc, KafkaTemplate<String, String> kafka, TransactionTemplate tx,
                              OutboxProperties.Polling props, MeterRegistry meters) {
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.tx = tx;
        this.props = props;
        this.meters = meters;
        Gauge.builder("outbox_pending_messages", this, PollingOutboxRelay::pending)
                .description("Outbox rows not yet published to Kafka")
                .register(meters);
    }

    @Scheduled(fixedDelayString = "${platform.outbox.polling.interval:500ms}")
    public void publishPending() {
        int published;
        do {
            published = publishBatch();
        } while (published == props.getBatchSize());
    }

    int publishBatch() {
        Integer count = tx.execute(status -> {
            List<Row> rows = jdbc.query(SELECT_BATCH, (rs, i) -> new Row(
                    rs.getObject("id", UUID.class), rs.getString("aggregate_type"), rs.getString("aggregate_id"),
                    rs.getLong("aggregate_version"), rs.getString("destination"), rs.getString("event_type"),
                    rs.getInt("event_version"), rs.getString("payload"), rs.getString("correlation_id"),
                    rs.getString("causation_id"), rs.getString("trace_parent"),
                    rs.getTimestamp("created_at").toInstant()), props.getBatchSize());
            if (rows.isEmpty()) {
                return 0;
            }
            List<CompletableFuture<?>> sends = new ArrayList<>(rows.size());
            for (Row row : rows) {
                sends.add(kafka.send(toRecord(row)));
            }
            try {
                CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new))
                        .get(props.getSendTimeout().toMillis(), TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                // rollback releases the row locks; the batch is retried on the next tick
                throw new IllegalStateException("Outbox batch was not acknowledged by Kafka", e);
            }
            jdbc.batchUpdate("update outbox set published_at = ? where id = ?", rows, rows.size(), (ps, row) -> {
                ps.setTimestamp(1, Timestamp.from(Instant.now()));
                ps.setObject(2, row.id());
            });
            return rows.size();
        });
        int n = count == null ? 0 : count;
        if (n > 0) {
            meters.counter("outbox_messages_published_total").increment(n);
            log.debug("Published {} outbox messages", n);
        }
        return n;
    }

    @Scheduled(fixedDelayString = "PT1H", initialDelayString = "PT5M")
    public void purgePublished() {
        int deleted = jdbc.update("delete from outbox where published_at < ?",
                Timestamp.from(Instant.now().minus(props.getRetention())));
        if (deleted > 0) {
            log.info("Purged {} published outbox rows", deleted);
        }
    }

    private double pending() {
        try {
            Long n = jdbc.queryForObject("select count(*) from outbox where published_at is null", Long.class);
            return n == null ? 0 : n;
        } catch (Exception e) {
            return Double.NaN;
        }
    }

    private static ProducerRecord<String, String> toRecord(Row row) {
        ProducerRecord<String, String> record = new ProducerRecord<>(row.destination(), row.aggregateId(), row.payload());
        header(record, MessageHeaders.ID, row.id().toString());
        header(record, MessageHeaders.EVENT_TYPE, row.eventType());
        header(record, MessageHeaders.EVENT_VERSION, String.valueOf(row.eventVersion()));
        header(record, MessageHeaders.AGGREGATE_TYPE, row.aggregateType());
        header(record, MessageHeaders.AGGREGATE_ID, row.aggregateId());
        header(record, MessageHeaders.AGGREGATE_VERSION, String.valueOf(row.aggregateVersion()));
        header(record, MessageHeaders.CORRELATION_ID, row.correlationId());
        header(record, MessageHeaders.CAUSATION_ID, row.causationId());
        header(record, MessageHeaders.OCCURRED_AT, row.createdAt().toString());
        header(record, MessageHeaders.TRACEPARENT, row.traceParent());
        return record;
    }

    private static void header(ProducerRecord<String, String> record, String name, String value) {
        if (value != null) {
            record.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
        }
    }

    private record Row(UUID id, String aggregateType, String aggregateId, long aggregateVersion, String destination,
                       String eventType, int eventVersion, String payload, String correlationId, String causationId,
                       String traceParent, Instant createdAt) {
    }
}
