package com.demp.platform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.demp.platform.inbox.IdempotentConsumer;
import com.demp.platform.messaging.EnvelopeReader;
import com.demp.platform.messaging.EventEnvelope;
import com.demp.platform.outbox.OutboxMessage;
import com.demp.platform.outbox.OutboxWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

@SpringBootTest(properties = {
        "platform.outbox.polling.enabled=true",
        "platform.outbox.polling.interval=100ms",
        "spring.sql.init.mode=always",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.consumer.group-id=platform-test"})
class InboxOutboxIT {

    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1");

    static {
        POSTGRES.start();
        KAFKA.start();
    }

    @TestConfiguration
    static class Config {
        @Bean
        @ServiceConnection
        PostgreSQLContainer<?> postgres() {
            return POSTGRES;
        }

        @Bean
        @ServiceConnection
        KafkaContainer kafka() {
            return KAFKA;
        }

        @Bean
        Sink sink() {
            return new Sink();
        }
    }

    static class Sink {
        final List<ConsumerRecord<String, String>> records = new CopyOnWriteArrayList<>();

        @KafkaListener(topics = "test.events")
        void on(ConsumerRecord<String, String> record) {
            records.add(record);
        }
    }

    @Autowired OutboxWriter outbox;
    @Autowired IdempotentConsumer consumer;
    @Autowired TransactionTemplate tx;
    @Autowired JdbcTemplate jdbc;
    @Autowired Sink sink;
    @Autowired EnvelopeReader reader;
    @Autowired ObjectMapper mapper;

    @Test
    void outboxRowIsPublishedWithEnvelopeHeadersAndMarkedPublished() {
        UUID id = tx.execute(s -> outbox.write(OutboxMessage.builder()
                .destination("test.events").aggregateType("Thing").aggregateId("thing-1").aggregateVersion(3)
                .eventType("ThingHappened").payload(Map.of("value", 42)).correlationId("corr-9").build()));

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(sink.records).anyMatch(r -> reader.read(r).id().equals(id)));

        EventEnvelope envelope = sink.records.stream().map(reader::read).filter(e -> e.id().equals(id)).findFirst().orElseThrow();
        assertThat(envelope.type()).isEqualTo("ThingHappened");
        assertThat(envelope.aggregateId()).isEqualTo("thing-1");
        assertThat(envelope.aggregateVersion()).isEqualTo(3);
        assertThat(envelope.correlationId()).isEqualTo("corr-9");
        assertThat(envelope.payload().get("value").asInt()).isEqualTo(42);
        await().untilAsserted(() -> assertThat(jdbc.queryForObject(
                "select published_at is not null from outbox where id = ?", Boolean.class, id)).isTrue());
    }

    @Test
    void outboxRowIsRolledBackTogetherWithBusinessChange() {
        assertThatThrownBy(() -> tx.executeWithoutResult(s -> {
            jdbc.update("insert into business (id) values ('rolled-back')");
            outbox.write(OutboxMessage.builder().destination("test.events").aggregateType("Thing")
                    .aggregateId("rolled-back").eventType("X").payload(Map.of()).build());
            throw new IllegalStateException("boom");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(count("select count(*) from outbox where aggregate_id = 'rolled-back'")).isZero();
        assertThat(count("select count(*) from business where id = 'rolled-back'")).isZero();
    }

    @Test
    void outboxWriteOutsideTransactionIsRejected() {
        assertThatThrownBy(() -> outbox.write(OutboxMessage.builder().destination("t").aggregateType("a")
                .aggregateId("b").eventType("c").payload(Map.of()).build()))
                .isInstanceOf(IllegalTransactionStateException.class);
    }

    @Test
    void duplicateMessageIsProcessedOnce() {
        EventEnvelope envelope = envelope();

        boolean first = consumer.handle("c1", envelope, e -> jdbc.update("insert into business (id) values (?)", e.id().toString()));
        boolean second = consumer.handle("c1", envelope, e -> jdbc.update("insert into business (id) values (?)", e.id().toString()));
        boolean otherConsumer = consumer.handle("c2", envelope, e -> { });

        assertThat(first).isTrue();
        assertThat(second).isFalse();
        assertThat(otherConsumer).as("inbox key is (consumer, event)").isTrue();
    }

    @Test
    void failedHandlerRollsBackInboxSoRedeliveryIsProcessed() {
        EventEnvelope envelope = envelope();

        assertThatThrownBy(() -> consumer.handle("c1", envelope, e -> {
            throw new IllegalStateException("handler failed");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(consumer.handle("c1", envelope, e -> { })).isTrue();
    }

    private EventEnvelope envelope() {
        return new EventEnvelope(UUID.randomUUID(), "T", 1, "A", "1", 1, "corr", null, Instant.now(), "topic",
                mapper.createObjectNode());
    }

    private long count(String sql) {
        return jdbc.queryForObject(sql, Long.class);
    }
}
