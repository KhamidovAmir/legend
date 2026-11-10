package com.demp.exceptions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.demp.contracts.ExceptionCommands;
import com.demp.contracts.ExceptionCommands.ExceptionCommand;
import com.demp.contracts.ExceptionEvents;
import com.demp.contracts.Topics;
import com.demp.exceptions.api.ExceptionController.ExceptionView;
import com.demp.exceptions.domain.ExceptionRecord;
import com.demp.exceptions.domain.ExceptionStatus;
import com.demp.exceptions.domain.Repositories;
import com.demp.platform.messaging.EnvelopeReader;
import com.demp.platform.messaging.EventEnvelope;
import com.demp.platform.messaging.MessageHeaders;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Black-box tests of the service against real PostgreSQL and Kafka:
 * external event in -> exception + outbox -> domain events out; commands in -> state transitions.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
class ExceptionFlowIT {

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
        PublishedEvents publishedEvents(EnvelopeReader reader) {
            return new PublishedEvents(reader);
        }
    }

    /** Plays the role of downstream consumers of exception.events. */
    static class PublishedEvents {
        final List<EventEnvelope> events = new CopyOnWriteArrayList<>();
        private final EnvelopeReader reader;

        PublishedEvents(EnvelopeReader reader) {
            this.reader = reader;
        }

        @KafkaListener(topics = Topics.EXCEPTION_EVENTS, groupId = "test-downstream")
        void on(ConsumerRecord<String, String> record) {
            events.add(reader.read(record));
        }

        List<String> typesFor(UUID exceptionId) {
            return events.stream().filter(e -> e.aggregateId().equals(exceptionId.toString())).map(EventEnvelope::type).toList();
        }
    }

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired ObjectMapper mapper;
    @Autowired Repositories.ExceptionRecords exceptions;
    @Autowired JdbcTemplate jdbc;
    @Autowired PublishedEvents published;
    @Autowired TestRestTemplate rest;

    @Test
    void deliveryFailedCreatesClassifiedExceptionAndPublishesEvents() throws Exception {
        String shipment = "SHP-" + UUID.randomUUID();
        sendExternal(UUID.randomUUID().toString(), "DeliveryFailed", shipment);

        ExceptionRecord e = awaitException(shipment);
        assertThat(e.getStatus()).isEqualTo(ExceptionStatus.CLASSIFIED);
        assertThat(e.getExceptionType()).isEqualTo("DELIVERY_FAILURE");
        assertThat(e.getSeverity()).isEqualTo("HIGH");
        assertThat(e.getPriority()).isEqualTo("P1");

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(published.typesFor(e.getId()))
                .containsExactly(ExceptionEvents.CREATED, ExceptionEvents.CLASSIFIED));
        EventEnvelope classified = published.events.stream()
                .filter(x -> x.type().equals(ExceptionEvents.CLASSIFIED) && x.aggregateId().equals(e.getId().toString()))
                .findFirst().orElseThrow();
        assertThat(classified.aggregateVersion()).isEqualTo(2);
        assertThat(classified.correlationId()).as("correlationId of the external event is propagated").isEqualTo("corr-" + shipment);
        assertThat(classified.payloadAs(ExceptionEvents.ExceptionClassified.class, mapper).entityId()).isEqualTo(shipment);
    }

    @Test
    void sameExternalEventTwiceAndRepeatedSignalDoNotCreateSecondException() throws Exception {
        String shipment = "SHP-" + UUID.randomUUID();
        String eventId = UUID.randomUUID().toString();
        sendExternal(eventId, "DeliveryFailed", shipment);
        sendExternal(eventId, "DeliveryFailed", shipment);                       // technical duplicate
        sendExternal(UUID.randomUUID().toString(), "DeliveryFailed", shipment);  // new event, same problem
        String marker = "SHP-" + UUID.randomUUID();
        sendExternal(UUID.randomUUID().toString(), "DeliveryFailed", marker);    // same partition is not guaranteed -> wait for all
        awaitException(marker);

        ExceptionRecord e = awaitException(shipment);
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(jdbc.queryForObject(
                "select count(*) from inbound_event where entity_id = ? and decision = 'LINKED'", Long.class, shipment)).isEqualTo(1));
        assertThat(jdbc.queryForObject("select count(*) from exception_record where business_correlation_key like ?",
                Long.class, "%" + shipment + "%")).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from inbound_event where entity_id = ?", Long.class, shipment))
                .as("technical duplicate is not stored twice").isEqualTo(2);
        assertThat(jdbc.queryForObject("select count(*) from exception_history where exception_id = ? and event = 'REPEATED_SIGNAL'",
                Long.class, e.getId())).isEqualTo(1);
    }

    @Test
    void ignoredAndUnknownEventTypesCreateNothing() throws Exception {
        String shipment = "SHP-" + UUID.randomUUID();
        sendExternal(UUID.randomUUID().toString(), "DeliveryAttemptStarted", shipment);
        sendExternal(UUID.randomUUID().toString(), "SomethingNew", shipment);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(jdbc.queryForObject(
                "select count(*) from inbound_event where entity_id = ? and decision = 'IGNORED'", Long.class, shipment)).isEqualTo(2));
        assertThat(jdbc.queryForObject("select count(*) from exception_record where business_correlation_key like ?",
                Long.class, "%" + shipment + "%")).isZero();
    }

    @Test
    void commandsDriveLifecycleAndRedeliveredCommandIsAppliedOnce() throws Exception {
        String shipment = "SHP-" + UUID.randomUUID();
        sendExternal(UUID.randomUUID().toString(), "DeliveryFailed", shipment);
        UUID id = awaitException(shipment).getId();

        UUID startId = UUID.randomUUID();
        sendCommand(startId, ExceptionCommands.START_PROGRESS, ExceptionCommand.of(id, "resolution-service"));
        sendCommand(startId, ExceptionCommands.START_PROGRESS, ExceptionCommand.of(id, "resolution-service")); // redelivery
        awaitStatus(id, ExceptionStatus.IN_PROGRESS);

        sendCommand(UUID.randomUUID(), ExceptionCommands.RESOLVE,
                new ExceptionCommand(id, "resolution-service", null, "DELIVERY_RESCHEDULED"));
        // quiet-period is 0 in tests: the closing job moves RESOLVED -> CLOSED
        awaitStatus(id, ExceptionStatus.CLOSED);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(published.typesFor(id)).containsExactly(
                ExceptionEvents.CREATED, ExceptionEvents.CLASSIFIED,
                ExceptionEvents.STATUS_CHANGED,                       // IN_PROGRESS (once, despite redelivery)
                ExceptionEvents.STATUS_CHANGED,                       // RESOLVED
                ExceptionEvents.STATUS_CHANGED, ExceptionEvents.CLOSED));
        assertThat(jdbc.queryForObject("select count(*) from inbox where event_id = ?", Long.class, startId)).isEqualTo(1);

        ResponseEntity<ExceptionView> view = rest.getForEntity("/api/v1/exceptions/{id}", ExceptionView.class, id);
        assertThat(view.getBody().status()).isEqualTo(ExceptionStatus.CLOSED);
        assertThat(view.getBody().resolution()).isEqualTo("DELIVERY_RESCHEDULED");
        assertThat(view.getBody().references()).hasSize(1);
        List<?> history = rest.getForObject("/api/v1/exceptions/{id}/history", List.class, id);
        assertThat(history).hasSize(5);
    }

    @Test
    void commandNotFittingCurrentStateIsRejectedWithEventInsteadOfBlockingThePartition() throws Exception {
        String shipment = "SHP-" + UUID.randomUUID();
        sendExternal(UUID.randomUUID().toString(), "DeliveryFailed", shipment);
        UUID id = awaitException(shipment).getId();

        sendCommand(UUID.randomUUID(), ExceptionCommands.RESOLVE, new ExceptionCommand(id, "resolution-service", null, "X"));

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(published.typesFor(id)).contains(ExceptionEvents.COMMAND_REJECTED));
        assertThat(exceptions.findById(id).orElseThrow().getStatus()).isEqualTo(ExceptionStatus.CLASSIFIED);
    }

    @Test
    void operatorApi_manualClassification_close_relations_andErrors() throws Exception {
        String a = "SHP-" + UUID.randomUUID();
        String b = "SHP-" + UUID.randomUUID();
        sendExternal(UUID.randomUUID().toString(), "CourierTaskExpired", a);   // no ClassificationPolicy -> stays OPEN
        sendExternal(UUID.randomUUID().toString(), "DeliveryFailed", b);
        UUID idA = awaitException(a).getId();
        UUID idB = awaitException(b).getId();
        assertThat(exceptions.findById(idA).orElseThrow().getStatus()).isEqualTo(ExceptionStatus.OPEN);

        ExceptionView classified = rest.postForObject("/api/v1/exceptions/{id}/classification",
                Map.of("category", "COURIER", "severity", "LOW", "priority", "P3", "operator", "ivanov"), ExceptionView.class, idA);
        assertThat(classified.status()).isEqualTo(ExceptionStatus.CLASSIFIED);

        rest.postForObject("/api/v1/exceptions/{id}/relations",
                Map.of("toExceptionId", idB, "type", "CAUSED", "operator", "ivanov"), Map.class, idA);
        assertThat(rest.getForObject("/api/v1/exceptions/{id}/relations", List.class, idB)).hasSize(1);

        ExceptionView closed = rest.postForObject("/api/v1/exceptions/{id}/close",
                Map.of("reason", "FALSE_POSITIVE", "operator", "ivanov"), ExceptionView.class, idA);
        assertThat(closed.status()).isEqualTo(ExceptionStatus.CLOSED);

        assertThat(rest.postForEntity("/api/v1/exceptions/{id}/close",
                Map.of("reason", "DUPLICATE", "operator", "ivanov"), Map.class, idA).getStatusCode())
                .as("already closed").isEqualTo(HttpStatus.CONFLICT);
        assertThat(rest.postForEntity("/api/v1/exceptions/{id}/close", Map.of("operator", "ivanov"), Map.class, idB)
                .getStatusCode()).as("reason is mandatory").isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(rest.getForEntity("/api/v1/exceptions/{id}", Map.class, UUID.randomUUID()).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(rest.getForObject("/api/v1/exceptions?status=CLOSED&size=5", Map.class)).containsKey("items");

        // after closing, the same problem may be registered again as a NEW exception (partial unique index)
        sendExternal(UUID.randomUUID().toString(), "CourierTaskExpired", a);
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(jdbc.queryForObject(
                "select count(*) from exception_record where business_correlation_key like ?", Long.class, "%" + a + "%")).isEqualTo(2));
    }

    @Test
    void poisonMessageGoesToDeadLetterTopicAndDoesNotStopConsumption() throws Exception {
        kafka.send(Topics.EXTERNAL_DELIVERY_EVENTS, "k", "{not-json").get();
        String shipment = "SHP-" + UUID.randomUUID();
        sendExternal(UUID.randomUUID().toString(), "DeliveryFailed", shipment);

        assertThat(awaitException(shipment)).isNotNull();
    }

    // --- helpers ---

    private void sendExternal(String eventId, String eventType, String shipment) throws Exception {
        String body = mapper.writeValueAsString(Map.of(
                "eventId", eventId, "eventType", eventType, "sourceSystem", "DELIVERY",
                "entityType", "SHIPMENT", "entityId", shipment, "region", "MSK",
                "occurredAt", "2026-01-10T10:00:00Z", "correlationId", "corr-" + shipment,
                "payload", Map.of("reason", "RECIPIENT_UNAVAILABLE")));
        kafka.send(Topics.EXTERNAL_DELIVERY_EVENTS, shipment, body).get();
    }

    private void sendCommand(UUID messageId, String type, ExceptionCommand command) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(Topics.EXCEPTION_COMMANDS,
                command.exceptionId().toString(), mapper.writeValueAsString(command));
        record.headers().add(MessageHeaders.ID, messageId.toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add(MessageHeaders.EVENT_TYPE, type.getBytes(StandardCharsets.UTF_8));
        kafka.send(record).get();
    }

    private ExceptionRecord awaitException(String shipment) {
        String key = "DELIVERY:SHIPMENT:" + shipment + ":%";
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(jdbc.queryForObject(
                "select count(*) from exception_record where business_correlation_key like ?", Long.class, key)).isPositive());
        UUID id = jdbc.queryForObject("select id from exception_record where business_correlation_key like ? limit 1", UUID.class, key);
        return exceptions.findById(id).orElseThrow();
    }

    private void awaitStatus(UUID id, ExceptionStatus status) {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(exceptions.findById(id).orElseThrow().getStatus()).isEqualTo(status));
    }
}
