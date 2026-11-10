package com.demp.integration;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.demp.contracts.IntegrationMessages;
import com.demp.contracts.Topics;
import com.demp.platform.messaging.MessageHeaders;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

/** The Delivery system is played by WireMock, including a delayed answer that reproduces UNKNOWN. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
class IntegrationFlowIT {

    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1");
    static final WireMockServer DELIVERY = new WireMockServer(wireMockConfig().dynamicPort());

    static {
        POSTGRES.start();
        KAFKA.start();
        DELIVERY.start();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("integration.systems.DELIVERY.base-url", DELIVERY::baseUrl);
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
    void acceptedCommand_thenAsyncResult_completesOperation() throws Exception {
        Cmd cmd = new Cmd();
        DELIVERY.stubFor(post(urlEqualTo(cmd.retryUrl())).willReturn(aResponse().withStatus(202)
                .withHeader("Content-Type", "application/json").withBody("{\"requestId\":\"REQ-" + cmd.shipment + "\"}")));

        execute(cmd);

        awaitEvents(cmd, IntegrationMessages.ACCEPTED);
        DELIVERY.verify(1, postRequestedFor(urlEqualTo(cmd.retryUrl())).withHeader("Idempotency-Key", equalTo(cmd.idempotencyKey)));
        assertThat(status(cmd)).isEqualTo("ACCEPTED");

        deliveryResult(Map.of("externalRequestId", "REQ-" + cmd.shipment, "status", "COMPLETED", "result", "DELIVERY_RETRY_SCHEDULED"));
        deliveryResult(Map.of("externalRequestId", "REQ-" + cmd.shipment, "status", "COMPLETED", "result", "DELIVERY_RETRY_SCHEDULED"));

        awaitEvents(cmd, IntegrationMessages.ACCEPTED, IntegrationMessages.COMPLETED);
        assertThat(status(cmd)).isEqualTo("COMPLETED");

        Map<?, ?> view = rest.getForObject("/api/v1/operations/{id}", Map.class, cmd.operation);
        assertThat(view.get("externalRequestId")).isEqualTo("REQ-" + cmd.shipment);
        assertThat((List<?>) view.get("attempts")).hasSize(1);
        assertThat(rest.getForEntity("/api/v1/operations/{id}", Map.class, UUID.randomUUID()).getStatusCode().value()).isEqualTo(404);
    }

    @Test
    void readTimeout_isUnknown_notRetried_andLateResultIsMatchedByIdempotencyKey() throws Exception {
        Cmd cmd = new Cmd();
        DELIVERY.stubFor(post(urlEqualTo(cmd.retryUrl())).willReturn(aResponse().withStatus(202)
                .withBody("{\"requestId\":\"REQ-lost\"}").withFixedDelay(1500)));   // read-timeout is 500ms

        execute(cmd);

        JsonNode unknown = awaitEvents(cmd, IntegrationMessages.UNKNOWN).get(0);
        assertThat(unknown.get("reason").asText()).isEqualTo("REQUEST_TIMEOUT");
        assertThat(status(cmd)).isEqualTo("UNKNOWN");
        DELIVERY.verify(1, postRequestedFor(urlEqualTo(cmd.retryUrl())));       // the command is NOT repeated blindly

        // the external system did process the command; we never saw its request id
        deliveryResult(Map.of("externalRequestId", "REQ-never-seen", "idempotencyKey", cmd.idempotencyKey,
                "status", "COMPLETED", "result", "DELIVERY_RETRY_SCHEDULED"));
        awaitEvents(cmd, IntegrationMessages.UNKNOWN, IntegrationMessages.COMPLETED);
    }

    @Test
    void notSentResponses_areRetriedOnTransportLevel() throws Exception {
        Cmd cmd = new Cmd();
        DELIVERY.stubFor(post(urlEqualTo(cmd.retryUrl())).inScenario(cmd.shipment).whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withStatus(503)).willSetStateTo("up"));
        DELIVERY.stubFor(post(urlEqualTo(cmd.retryUrl())).inScenario(cmd.shipment).whenScenarioStateIs("up")
                .willReturn(okJson("{\"deliveryStatus\":\"DELIVERY_SCHEDULED\"}")));

        execute(cmd);

        JsonNode completed = awaitEvents(cmd, IntegrationMessages.COMPLETED).get(0);   // synchronous completion
        assertThat(completed.get("result").asText()).isEqualTo("DELIVERY_SCHEDULED");
        DELIVERY.verify(2, postRequestedFor(urlEqualTo(cmd.retryUrl())));
        assertThat(jdbc.queryForList("select outcome from integration_attempt where operation_id = ? order by attempt_no",
                String.class, cmd.operation)).containsExactly("NOT_SENT", "SUCCESS");
    }

    @Test
    void transportRetriesExhausted_isRetryableFailure_andRejectionIsNot() throws Exception {
        Cmd down = new Cmd();
        DELIVERY.stubFor(post(urlEqualTo(down.retryUrl())).willReturn(aResponse().withStatus(503)));
        execute(down);
        JsonNode failed = awaitEvents(down, IntegrationMessages.FAILED).get(0);
        assertThat(failed.get("retryable").asBoolean()).isTrue();
        DELIVERY.verify(3, postRequestedFor(urlEqualTo(down.retryUrl())));

        Cmd rejected = new Cmd();
        DELIVERY.stubFor(post(urlEqualTo(rejected.retryUrl())).willReturn(aResponse().withStatus(422)));
        execute(rejected);
        JsonNode rejection = awaitEvents(rejected, IntegrationMessages.FAILED).get(0);
        assertThat(rejection.get("retryable").asBoolean()).isFalse();
        assertThat(rejection.get("reason").asText()).isEqualTo("HTTP_422");
        DELIVERY.verify(1, postRequestedFor(urlEqualTo(rejected.retryUrl())));
    }

    @Test
    void redeliveredCommand_doesNotCallExternalSystemTwice() throws Exception {
        Cmd cmd = new Cmd();
        DELIVERY.stubFor(post(urlEqualTo(cmd.retryUrl())).willReturn(aResponse().withStatus(202).withBody("{\"requestId\":\"R\"}")));

        execute(cmd);
        execute(cmd);
        Cmd marker = new Cmd();
        DELIVERY.stubFor(post(urlEqualTo(marker.retryUrl())).willReturn(aResponse().withStatus(422)));
        // same Kafka key -> same partition -> processed after both deliveries of cmd
        send(Topics.INTEGRATION_COMMANDS, IntegrationMessages.EXECUTE_ACTION, cmd.exception, marker.payload(marker.exception));
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(jdbc.queryForObject("select count(*) from integration_operation where id = ? and status = 'FAILED'", Long.class, marker.operation)).isEqualTo(1));

        DELIVERY.verify(1, postRequestedFor(urlEqualTo(cmd.retryUrl())));
        assertThat(events(cmd)).hasSize(1);
    }

    @Test
    void operationInterruptedByCrash_isReportedAsUnknownWithoutCallingExternalSystem() throws Exception {
        Cmd cmd = new Cmd();
        // state left by an instance that died between "operation registered" and "result stored"
        jdbc.update("""
                insert into integration_operation (id, kind, exception_id, target_system, action_type, entity_type, entity_id,
                                                   idempotency_key, status, revision, version, created_at, updated_at)
                values (?, 'COMMAND', ?, 'DELIVERY', 'RETRY_DELIVERY', 'SHIPMENT', ?, ?, 'RECEIVED', 0, 0, now(), now())
                """, cmd.operation, cmd.exception, cmd.shipment, cmd.idempotencyKey);

        execute(cmd);

        JsonNode unknown = awaitEvents(cmd, IntegrationMessages.UNKNOWN).get(0);
        assertThat(unknown.get("reason").asText()).isEqualTo("INTERRUPTED");
        DELIVERY.verify(0, postRequestedFor(urlEqualTo(cmd.retryUrl())));
    }

    @Test
    void unsupportedSystemAndAction_failWithoutRetry() throws Exception {
        Cmd cmd = new Cmd();
        send(Topics.INTEGRATION_COMMANDS, IntegrationMessages.EXECUTE_ACTION, cmd.exception,
                new IntegrationMessages.ExecuteAction(cmd.operation, cmd.exception, UUID.randomUUID(), UUID.randomUUID(),
                        "CRM", "UPDATE_CONTACT", "CLIENT", "1", "k", Map.of()));
        assertThat(awaitEvents(cmd, IntegrationMessages.FAILED).get(0).get("reason").asText()).isEqualTo("UNSUPPORTED_SYSTEM");

        Cmd other = new Cmd();
        send(Topics.INTEGRATION_COMMANDS, IntegrationMessages.EXECUTE_ACTION, other.exception,
                new IntegrationMessages.ExecuteAction(other.operation, other.exception, UUID.randomUUID(), UUID.randomUUID(),
                        "DELIVERY", "CANCEL_DELIVERY", "SHIPMENT", other.shipment, "k2", Map.of()));
        assertThat(awaitEvents(other, IntegrationMessages.FAILED).get(0).get("reason").asText()).isEqualTo("UNSUPPORTED_ACTION");
    }

    @Test
    void query_returnsObservation_notFound_orFailure() throws Exception {
        Cmd found = new Cmd();
        DELIVERY.stubFor(get(urlEqualTo("/shipments/" + found.shipment)).willReturn(okJson("{\"deliveryStatus\":\"DELIVERY_SCHEDULED\"}")));
        query(found);
        JsonNode observation = awaitEvents(found, IntegrationMessages.STATE_RECEIVED).get(0);
        assertThat(observation.get("found").asBoolean()).isTrue();
        assertThat(observation.get("state").asText()).isEqualTo("DELIVERY_SCHEDULED");
        assertThat(observation.get("queryId").asText()).isEqualTo(found.operation.toString());

        Cmd missing = new Cmd();
        DELIVERY.stubFor(get(urlEqualTo("/shipments/" + missing.shipment)).willReturn(aResponse().withStatus(404)));
        query(missing);
        assertThat(awaitEvents(missing, IntegrationMessages.STATE_RECEIVED).get(0).get("found").asBoolean()).isFalse();

        Cmd broken = new Cmd();
        DELIVERY.stubFor(get(urlEqualTo("/shipments/" + broken.shipment)).willReturn(aResponse().withStatus(500)));
        query(broken);
        assertThat(awaitEvents(broken, IntegrationMessages.STATE_QUERY_FAILED).get(0).get("reason").asText()).isEqualTo("HTTP_500");
        DELIVERY.verify(3, getRequestedFor(urlEqualTo("/shipments/" + broken.shipment)));   // queries are safe to repeat
    }

    // ------------------------------------------------------------------ helpers

    class Cmd {
        final UUID operation = UUID.randomUUID();
        final UUID exception = UUID.randomUUID();
        final String shipment = "SHP-" + UUID.randomUUID();
        final String idempotencyKey = UUID.randomUUID().toString();

        String retryUrl() {
            return "/deliveries/" + shipment + "/retry";
        }

        IntegrationMessages.ExecuteAction payload(UUID exceptionId) {
            return new IntegrationMessages.ExecuteAction(operation, exceptionId, UUID.randomUUID(), UUID.randomUUID(),
                    "DELIVERY", "RETRY_DELIVERY", "SHIPMENT", shipment, idempotencyKey, Map.of());
        }
    }

    private void execute(Cmd cmd) throws Exception {
        send(Topics.INTEGRATION_COMMANDS, IntegrationMessages.EXECUTE_ACTION, cmd.exception, cmd.payload(cmd.exception));
    }

    private void query(Cmd cmd) throws Exception {
        send(Topics.INTEGRATION_QUERIES, IntegrationMessages.QUERY_EXTERNAL_STATE, cmd.exception,
                new IntegrationMessages.QueryExternalState(cmd.operation, cmd.exception, "DELIVERY", "SHIPMENT", cmd.shipment, "VERIFICATION"));
    }

    private void deliveryResult(Map<String, String> body) throws Exception {
        kafka.send(Topics.EXTERNAL_DELIVERY_RESULTS, "k", mapper.writeValueAsString(body)).get();
    }

    private void send(String topic, String type, UUID key, Object payload) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key.toString(), mapper.writeValueAsString(payload));
        record.headers().add(MessageHeaders.ID, UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add(MessageHeaders.EVENT_TYPE, type.getBytes(StandardCharsets.UTF_8));
        kafka.send(record).get();
    }

    private List<JsonNode> awaitEvents(Cmd cmd, String... types) {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(eventTypes(cmd)).containsExactly(types));
        return events(cmd).subList(types.length - 1, types.length);
    }

    private List<String> eventTypes(Cmd cmd) {
        return jdbc.queryForList("select event_type from outbox where aggregate_id = ? order by aggregate_version", String.class, cmd.exception.toString());
    }

    private List<JsonNode> events(Cmd cmd) {
        return jdbc.query("select payload::text from outbox where aggregate_id = ? order by aggregate_version", (rs, i) -> {
            try {
                return mapper.readTree(rs.getString(1));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }, cmd.exception.toString());
    }

    private String status(Cmd cmd) {
        return jdbc.queryForObject("select status from integration_operation where id = ?", String.class, cmd.operation);
    }
}
