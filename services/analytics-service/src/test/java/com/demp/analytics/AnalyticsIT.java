package com.demp.analytics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.demp.contracts.ExceptionEvents;
import com.demp.contracts.IntegrationMessages;
import com.demp.contracts.ResolutionEvents;
import com.demp.contracts.Topics;
import com.demp.platform.messaging.MessageHeaders;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class AnalyticsIT {

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
    }

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired ObjectMapper mapper;
    @Autowired JdbcTemplate jdbc;
    @Autowired TestRestTemplate rest;

    @Test
    void eventsOfThreeServicesAreFoldedIntoOneModel() throws Exception {
        String region = "R-" + UUID.randomUUID();
        Instant t0 = Instant.now().minus(Duration.ofHours(2));

        // exception A: resolved automatically in 10 minutes
        UUID a = UUID.randomUUID();
        // status event arrives BEFORE "created" (different topics/partitions give no global order)
        UUID statusEvent = send(Topics.EXCEPTION_EVENTS, ExceptionEvents.STATUS_CHANGED, a, 3, t0.plusSeconds(60),
                new ExceptionEvents.ExceptionStatusChanged(a, "CLASSIFIED", "IN_PROGRESS", "resolution-service"));
        send(Topics.EXCEPTION_EVENTS, ExceptionEvents.CREATED, a, 1, t0,
                new ExceptionEvents.ExceptionCreated(a, "DELIVERY", "SHIPMENT", "S1", region, t0));
        send(Topics.EXCEPTION_EVENTS, ExceptionEvents.CLASSIFIED, a, 2, t0.plusSeconds(1),
                new ExceptionEvents.ExceptionClassified(a, "DELIVERY_FAILURE", "DELIVERY", "HIGH", "P1", region, "DELIVERY", "SHIPMENT", "S1", "AUTO"));
        send(Topics.RESOLUTION_EVENTS, ResolutionEvents.WORKFLOW_STARTED, a, 1, t0.plusSeconds(2),
                new ResolutionEvents.WorkflowStarted(a, UUID.randomUUID(), "AUTO_REDELIVERY", true));
        send(Topics.RESOLUTION_EVENTS, ResolutionEvents.ACTION_ATTEMPT_STARTED, a, 2, t0.plusSeconds(30),
                new ResolutionEvents.ActionAttemptStarted(a, UUID.randomUUID(), UUID.randomUUID(), 1, "DELIVERY", "RETRY_DELIVERY"));
        send(Topics.INTEGRATION_EVENTS, IntegrationMessages.ACCEPTED, a, 1, t0.plusSeconds(31),
                new IntegrationMessages.IntegrationAccepted(UUID.randomUUID(), a, "DELIVERY", "REQ"));
        send(Topics.EXCEPTION_EVENTS, ExceptionEvents.STATUS_CHANGED, a, 4, t0.plusSeconds(600),
                new ExceptionEvents.ExceptionStatusChanged(a, "IN_PROGRESS", "RESOLVED", "resolution-service"));
        // duplicate delivery of an already processed event (older status!) must change nothing
        resend(Topics.EXCEPTION_EVENTS, statusEvent, ExceptionEvents.STATUS_CHANGED, a, 3, t0.plusSeconds(60),
                new ExceptionEvents.ExceptionStatusChanged(a, "CLASSIFIED", "IN_PROGRESS", "resolution-service"));

        // exception B: UNKNOWN result, second attempt, SLA breach, operator involved, still open
        UUID b = UUID.randomUUID();
        send(Topics.EXCEPTION_EVENTS, ExceptionEvents.CREATED, b, 1, t0,
                new ExceptionEvents.ExceptionCreated(b, "DELIVERY", "SHIPMENT", "S2", region, t0));
        send(Topics.EXCEPTION_EVENTS, ExceptionEvents.CLASSIFIED, b, 2, t0.plusSeconds(1),
                new ExceptionEvents.ExceptionClassified(b, "DELIVERY_FAILURE", "DELIVERY", "HIGH", "P1", region, "DELIVERY", "SHIPMENT", "S2", "AUTO"));
        send(Topics.RESOLUTION_EVENTS, ResolutionEvents.WORKFLOW_STARTED, b, 1, t0.plusSeconds(2),
                new ResolutionEvents.WorkflowStarted(b, UUID.randomUUID(), "AUTO_REDELIVERY", true));
        for (int attempt = 1; attempt <= 2; attempt++) {
            send(Topics.RESOLUTION_EVENTS, ResolutionEvents.ACTION_ATTEMPT_STARTED, b, 1 + attempt, t0.plusSeconds(60L * attempt),
                    new ResolutionEvents.ActionAttemptStarted(b, UUID.randomUUID(), UUID.randomUUID(), attempt, "DELIVERY", "RETRY_DELIVERY"));
        }
        send(Topics.INTEGRATION_EVENTS, IntegrationMessages.UNKNOWN, b, 1, t0.plusSeconds(70),
                new IntegrationMessages.IntegrationUnknown(UUID.randomUUID(), b, "DELIVERY", "REQUEST_TIMEOUT"));
        send(Topics.RESOLUTION_EVENTS, ResolutionEvents.VERIFICATION_COMPLETED, b, 5, t0.plusSeconds(300),
                new ResolutionEvents.VerificationCompleted(b, "FAILED", "DELIVERY_SCHEDULED", "DELIVERY_FAILED"));
        send(Topics.RESOLUTION_EVENTS, ResolutionEvents.WORK_ITEM_CREATED, b, 6, t0.plusSeconds(301),
                new ResolutionEvents.WorkItemCreated(b, UUID.randomUUID(), "VERIFICATION_FAILED"));
        send(Topics.RESOLUTION_EVENTS, ResolutionEvents.SLA_BREACHED, b, 7, t0.plusSeconds(900),
                new ResolutionEvents.SlaBreached(b, t0.plusSeconds(900)));

        // invalid events: no exceptionId / broken envelope. They are parked, consumption goes on.
        send(Topics.RESOLUTION_EVENTS, ResolutionEvents.SLA_BREACHED, UUID.randomUUID(), 1, t0, Map.of("deadlineAt", t0.toString()));
        kafka.send(Topics.EXCEPTION_EVENTS, "k", "{}").get();

        // exception C in the same region closes the list: once it is visible, everything before it was consumed
        UUID c = UUID.randomUUID();
        send(Topics.EXCEPTION_EVENTS, ExceptionEvents.CREATED, c, 1, t0,
                new ExceptionEvents.ExceptionCreated(c, "WMS", "ORDER", "O1", region, t0));
        send(Topics.EXCEPTION_EVENTS, ExceptionEvents.CLOSED, c, 2, t0.plusSeconds(5),
                new ExceptionEvents.ExceptionClosed(c, "FALSE_POSITIVE", false, "ivanov"));

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> {
            Map<?, ?> summary = rest.getForObject("/api/v1/analytics/summary?region={r}", Map.class, region);
            assertThat(number(summary, "exception_count")).isEqualTo(3);
            assertThat(number(summary, "backlog")).as("A and B are not closed").isEqualTo(2);
            assertThat(number(summary, "sla_breaches")).isEqualTo(1);
            assertThat(number(summary, "business_retries")).isEqualTo(1);
            assertThat(number(summary, "unknown_results")).isEqualTo(1);
            assertThat(number(summary, "avg_resolution_seconds")).isEqualTo(600);
            assertThat(number(summary, "avg_response_seconds")).isEqualTo(45);     // (30 + 60) / 2
            assertThat(number(summary, "automation_rate")).isCloseTo(1.0 / 3, org.assertj.core.data.Offset.offset(0.01));
            assertThat(number(summary, "manual_handling_rate")).isCloseTo(1.0 / 3, org.assertj.core.data.Offset.offset(0.01));
            assertThat(number(summary, "verification_failure_rate")).isCloseTo(1.0 / 3, org.assertj.core.data.Offset.offset(0.01));
        });

        assertThat(jdbc.queryForObject("select status from exception_fact where exception_id = ?", String.class, a))
                .as("late/duplicate older status does not overwrite the newer one").isEqualTo("RESOLVED");
        assertThat(jdbc.queryForObject("select close_reason from exception_fact where exception_id = ?", String.class, c)).isEqualTo("FALSE_POSITIVE");
        assertThat(jdbc.queryForObject("select count(*) from rejected_event", Long.class)).isGreaterThanOrEqualTo(2);
        assertThat(jdbc.queryForObject("select count(*) from raw_event where event_id = ?", Long.class, statusEvent)).isEqualTo(1);

        List<?> byType = rest.getForObject("/api/v1/analytics/breakdown?by=type", List.class);
        List<String> keys = byType.stream().map(row -> String.valueOf(((Map<?, ?>) row).get("key"))).toList();
        assertThat(keys).contains("DELIVERY_FAILURE", "UNCLASSIFIED");
        assertThat(rest.getForObject("/api/v1/analytics/breakdown?by=region", List.class)).isNotEmpty();
        List<?> systems = rest.getForObject("/api/v1/analytics/external-systems", List.class);
        Map<?, ?> delivery = (Map<?, ?>) systems.stream().filter(s -> "DELIVERY".equals(((Map<?, ?>) s).get("target_system"))).findFirst().orElseThrow();
        assertThat(number(delivery, "unknown")).isGreaterThanOrEqualTo(1);
    }

    private static double number(Map<?, ?> row, String key) {
        return ((Number) row.get(key)).doubleValue();
    }

    private UUID send(String topic, String type, UUID key, long version, Instant occurredAt, Object payload) throws Exception {
        UUID id = UUID.randomUUID();
        resend(topic, id, type, key, version, occurredAt, payload);
        return id;
    }

    private void resend(String topic, UUID id, String type, UUID key, long version, Instant occurredAt, Object payload) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key.toString(), mapper.writeValueAsString(payload));
        header(record, MessageHeaders.ID, id.toString());
        header(record, MessageHeaders.EVENT_TYPE, type);
        header(record, MessageHeaders.AGGREGATE_VERSION, String.valueOf(version));
        header(record, MessageHeaders.OCCURRED_AT, occurredAt.toString());
        kafka.send(record).get();
    }

    private static void header(ProducerRecord<String, String> record, String name, String value) {
        record.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
    }
}
