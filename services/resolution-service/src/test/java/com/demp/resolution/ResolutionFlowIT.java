package com.demp.resolution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.demp.contracts.ExceptionCommands;
import com.demp.contracts.ExceptionEvents;
import com.demp.contracts.IntegrationMessages;
import com.demp.contracts.ResolutionEvents;
import com.demp.contracts.Topics;
import com.demp.platform.messaging.MessageHeaders;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Resolution Service as a black box: Kafka messages in -> rows in the append-only outbox out.
 * (Debezium is not part of these tests: the outbox row IS the contract; the CDC hop is checked on a stand.)
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
class ResolutionFlowIT {

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
    @Autowired MeterRegistry meters;

    @BeforeEach
    void fastReconciliation() {
        // production values are minutes; tests need immediate checks and 2 confirmations of NOT_APPLIED
        jdbc.update("""
                update reconciliation_policy set settle_delay_seconds = 0, base_interval_seconds = 0,
                       max_interval_seconds = 0, not_applied_confirmations = 2, auto_retry_enabled = true
                """);
    }

    @Test
    void happyPath_asyncResolutionWithVerification() throws Exception {
        UUID ex = classified("DELIVERY_FAILURE");

        JsonNode execute = awaitOutbox(ex, IntegrationMessages.EXECUTE_ACTION, 1).get(0);
        assertThat(outboxTypes(ex)).containsExactly(ExceptionCommands.START_PROGRESS, ResolutionEvents.WORKFLOW_STARTED,
                IntegrationMessages.EXECUTE_ACTION, ResolutionEvents.ACTION_ATTEMPT_STARTED);
        assertThat(execute.get("actionType").asText()).isEqualTo("RETRY_DELIVERY");
        assertThat(execute.get("idempotencyKey").asText()).isEqualTo(execute.get("attemptId").asText());
        assertThat(single("select assignee_id from assignment where exception_id = ? and active", ex)).isEqualTo("AUTO_REDELIVERY_BOT");
        UUID operation = UUID.fromString(execute.get("operationId").asText());

        integration(IntegrationMessages.ACCEPTED, new IntegrationMessages.IntegrationAccepted(operation, ex, "DELIVERY", "REQ-1"));
        awaitValue("select status from action where exception_id = ?", ex, "WAITING_EXTERNAL_RESULT");
        assertThat(outboxTypes(ex)).as("accepted is not resolved").doesNotContain(ExceptionCommands.RESOLVE);

        integration(IntegrationMessages.COMPLETED, new IntegrationMessages.IntegrationCompleted(operation, ex, "DELIVERY", "DELIVERY_RETRY_SCHEDULED"));
        JsonNode query = awaitOutbox(ex, IntegrationMessages.QUERY_EXTERNAL_STATE, 1).get(0);
        assertThat(query.get("purpose").asText()).isEqualTo("VERIFICATION");

        observation(UUID.fromString(query.get("queryId").asText()), ex, true, "DELIVERY_SCHEDULED");
        JsonNode resolve = awaitOutbox(ex, ExceptionCommands.RESOLVE, 1).get(0);
        assertThat(resolve.get("resolution").asText()).isEqualTo("DELIVERY_RESCHEDULED");
        assertThat(single("select verification_status from resolution where exception_id = ?", ex)).isEqualTo("VERIFIED");
        assertThat(single("select status from resolution_verification where exception_id = ?", ex)).isEqualTo("PASSED");
        assertThat(single("select status from workflow_instance where exception_id = ?", ex)).isEqualTo("COMPLETED");
        assertThat(single("select status from sla_instance where exception_id = ?", ex)).isEqualTo("MET");

        // all messages about one exception are keyed by it and carry a growing aggregate_version
        List<Long> versions = jdbc.queryForList("select aggregate_version from outbox where aggregate_id = ? order by created_at, aggregate_version", Long.class, ex.toString());
        assertThat(versions).isSorted().doesNotHaveDuplicates();
    }

    @Test
    void redeliveredClassifiedEventStartsOneWorkflow() throws Exception {
        UUID ex = UUID.randomUUID();
        UUID messageId = UUID.randomUUID();
        sendClassified(messageId, ex, "DELIVERY_FAILURE");
        sendClassified(messageId, ex, "DELIVERY_FAILURE");
        sendClassified(UUID.randomUUID(), ex, "DELIVERY_FAILURE"); // different message id, same exception
        UUID marker = classified("DELIVERY_FAILURE");
        awaitOutbox(marker, IntegrationMessages.EXECUTE_ACTION, 1);

        assertThat(count("select count(*) from workflow_instance where exception_id = ?", ex)).isEqualTo(1);
        assertThat(outbox(ex, IntegrationMessages.EXECUTE_ACTION)).hasSize(1);
        assertThat(meters.get("inbox_messages_total").tag("result", "duplicate").counter().count()).isPositive();
    }

    @Test
    void unknown_isReconciled_andAppliedStateResolvesWithoutSecondCommand() throws Exception {
        UUID ex = classified("DELIVERY_FAILURE");
        UUID operation = operationOf(ex, 1);

        integration(IntegrationMessages.UNKNOWN, new IntegrationMessages.IntegrationUnknown(operation, ex, "DELIVERY", "REQUEST_TIMEOUT"));

        UUID query = awaitReconciliationQuery(ex, 1);
        assertThat(single("select status from action where exception_id = ?", ex)).isEqualTo("UNKNOWN");
        observation(query, ex, true, "DELIVERY_SCHEDULED");

        awaitOutbox(ex, ExceptionCommands.RESOLVE, 1);
        assertThat(single("select result from reconciliation_task where exception_id = ?", ex)).isEqualTo("APPLIED");
        assertThat(outbox(ex, IntegrationMessages.EXECUTE_ACTION)).as("no blind retry").hasSize(1);
    }

    @Test
    void notApplied_mustBeConfirmedBeforeSecondAttempt_andLateSuccessRaisesConflict() throws Exception {
        UUID ex = classified("DELIVERY_FAILURE");
        UUID firstOperation = operationOf(ex, 1);
        integration(IntegrationMessages.UNKNOWN, new IntegrationMessages.IntegrationUnknown(firstOperation, ex, "DELIVERY", "REQUEST_TIMEOUT"));

        // 1st check sees the old state: only a candidate (read model of the external system may lag)
        observation(awaitReconciliationQuery(ex, 1), ex, true, "DELIVERY_FAILED");
        UUID secondQuery = awaitReconciliationQuery(ex, 2);
        assertThat(outbox(ex, IntegrationMessages.EXECUTE_ACTION)).hasSize(1);

        // an answer to an outdated query is ignored
        observation(UUID.randomUUID(), ex, true, "DELIVERY_FAILED");

        // 2nd consecutive check confirms NOT_APPLIED -> policy allows an automatic business retry
        observation(secondQuery, ex, true, "DELIVERY_FAILED");
        List<JsonNode> commands = awaitOutbox(ex, IntegrationMessages.EXECUTE_ACTION, 2);
        assertThat(commands.get(1).get("operationId")).isNotEqualTo(commands.get(0).get("operationId"));
        assertThat(commands.get(1).get("idempotencyKey")).as("new business attempt = new idempotency key")
                .isNotEqualTo(commands.get(0).get("idempotencyKey"));
        assertThat(single("select status from action_attempt where operation_id = ?", firstOperation)).isEqualTo("NOT_APPLIED");

        // ...and now the first command turns out to have succeeded: possible double delivery -> alert + operator
        integration(IntegrationMessages.COMPLETED, new IntegrationMessages.IntegrationCompleted(firstOperation, ex, "DELIVERY", "DELIVERY_RETRY_SCHEDULED"));
        awaitValue("select type from work_item where exception_id = ?", ex, "CONFLICT_REVIEW");
        assertThat(meters.get("reconciliation_false_not_applied_total").tag("system", "DELIVERY").counter().count()).isPositive();
    }

    @Test
    void autoRetryFlagOff_confirmedNotAppliedGoesToOperator() throws Exception {
        jdbc.update("update reconciliation_policy set auto_retry_enabled = false"); // the incident hotfix
        UUID ex = classified("DELIVERY_FAILURE");
        integration(IntegrationMessages.UNKNOWN, new IntegrationMessages.IntegrationUnknown(operationOf(ex, 1), ex, "DELIVERY", "REQUEST_TIMEOUT"));
        observation(awaitReconciliationQuery(ex, 1), ex, true, "DELIVERY_FAILED");
        observation(awaitReconciliationQuery(ex, 2), ex, true, "DELIVERY_FAILED");

        awaitValue("select type from work_item where exception_id = ?", ex, "NOT_APPLIED_REVIEW");
        assertThat(outbox(ex, IntegrationMessages.EXECUTE_ACTION)).hasSize(1);
        assertThat(single("select assignee_type from assignment where exception_id = ? and active", ex)).isEqualTo("TEAM");
    }

    @Test
    void stillProcessing_thenInconclusive_goesToOperator_whoClosesTheException() throws Exception {
        UUID ex = classified("DELIVERY_FAILURE");
        integration(IntegrationMessages.UNKNOWN, new IntegrationMessages.IntegrationUnknown(operationOf(ex, 1), ex, "DELIVERY", "REQUEST_TIMEOUT"));

        observation(awaitReconciliationQuery(ex, 1), ex, true, "RETRY_PROCESSING");
        UUID second = awaitReconciliationQuery(ex, 2);
        stateQueryFailed(second, ex);                                   // system unavailable: not an observation
        observation(awaitReconciliationQuery(ex, 3), ex, false, null);  // entity not found -> INCONCLUSIVE

        awaitValue("select type from work_item where exception_id = ?", ex, "RECONCILIATION_REVIEW");
        assertThat(single("select status || '/' || result from reconciliation_task where exception_id = ?", ex)).isEqualTo("ESCALATED/INCONCLUSIVE");
        assertThat(outbox(ex, IntegrationMessages.EXECUTE_ACTION)).as("side effect is never repeated automatically").hasSize(1);

        Map<?, ?> state = rest.getForObject("/api/v1/exceptions/{id}/resolution", Map.class, ex);
        assertThat(state.get("status")).isEqualTo("MANUAL");
        assertThat((List<?>) state.get("reconciliationTasks")).hasSize(1);

        UUID workItem = jdbc.queryForObject("select id from work_item where exception_id = ?", UUID.class, ex);
        assertThat(rest.getForObject("/api/v1/work-items", List.class)).isNotEmpty();
        assertThat(rest.postForEntity("/api/v1/work-items/{id}/complete", Map.of("outcome", "CLOSED", "operator", "petrov"), Map.class, workItem)
                .getStatusCode()).as("closeReason is mandatory").isEqualTo(HttpStatus.CONFLICT);
        rest.postForObject("/api/v1/work-items/{id}/complete",
                Map.of("outcome", "CLOSED", "operator", "petrov", "closeReason", "UNRESOLVABLE"), Map.class, workItem);

        JsonNode close = awaitOutbox(ex, ExceptionCommands.CLOSE, 1).get(0);
        assertThat(close.get("reason").asText()).isEqualTo("UNRESOLVABLE");
        assertThat(single("select status from workflow_instance where exception_id = ?", ex)).isEqualTo("CANCELLED");
    }

    @Test
    void reconciliationDeadline_escalatesToOperatorWithHistoryOfChecks() throws Exception {
        UUID ex = classified("DELIVERY_FAILURE");
        integration(IntegrationMessages.UNKNOWN, new IntegrationMessages.IntegrationUnknown(operationOf(ex, 1), ex, "DELIVERY", "REQUEST_TIMEOUT"));
        awaitReconciliationQuery(ex, 1);

        // no answer arrives; both the answer timeout and the task deadline pass
        jdbc.update("update reconciliation_task set deadline_at = now(), next_check_at = now() where exception_id = ?", ex);

        awaitValue("select status from reconciliation_task where exception_id = ?", ex, "ESCALATED");
        awaitValue("select type from work_item where exception_id = ?", ex, "RECONCILIATION_EXPIRED");
    }

    @Test
    void definiteFailure_isRetriedAsNewBusinessAttempt_untilAttemptsAreExhausted() throws Exception {
        UUID ex = classified("DELIVERY_FAILURE");
        integration(IntegrationMessages.FAILED, new IntegrationMessages.IntegrationFailed(operationOf(ex, 1), ex, "DELIVERY", "HTTP_503", true));
        UUID second = operationOf(ex, 2);
        integration(IntegrationMessages.FAILED, new IntegrationMessages.IntegrationFailed(second, ex, "DELIVERY", "HTTP_422", true));

        awaitValue("select type from work_item where exception_id = ?", ex, "ACTION_FAILED");
        assertThat(outbox(ex, IntegrationMessages.EXECUTE_ACTION)).as("max_business_attempts = 2").hasSize(2);
        assertThat(single("select status from action where exception_id = ?", ex)).isEqualTo("FAILED");
    }

    @Test
    void failedVerification_doesNotResolve() throws Exception {
        UUID ex = classified("DELIVERY_FAILURE");
        integration(IntegrationMessages.COMPLETED, new IntegrationMessages.IntegrationCompleted(operationOf(ex, 1), ex, "DELIVERY", "OK"));
        UUID first = UUID.fromString(awaitOutbox(ex, IntegrationMessages.QUERY_EXTERNAL_STATE, 1).get(0).get("queryId").asText());
        stateQueryFailed(first, ex);                                    // verification query is repeated
        UUID second = UUID.fromString(awaitOutbox(ex, IntegrationMessages.QUERY_EXTERNAL_STATE, 2).get(1).get("queryId").asText());
        observation(second, ex, true, "DELIVERY_FAILED");

        awaitValue("select type from work_item where exception_id = ?", ex, "VERIFICATION_FAILED");
        assertThat(outbox(ex, ExceptionCommands.RESOLVE)).isEmpty();
        assertThat(single("select status from resolution_verification where exception_id = ?", ex)).isEqualTo("FAILED");
    }

    @Test
    void slaBreach_createsEscalationButKeepsWorkflowRunning() throws Exception {
        UUID ex = classified("DELIVERY_FAILURE");
        awaitOutbox(ex, IntegrationMessages.EXECUTE_ACTION, 1);

        jdbc.update("update sla_instance set deadline_at = now() - interval '1 minute' where exception_id = ?", ex);

        awaitOutbox(ex, ResolutionEvents.SLA_BREACHED, 1);
        assertThat(single("select reason from escalation where exception_id = ?", ex)).isEqualTo("SLA_BREACHED");
        assertThat(single("select type from work_item where exception_id = ?", ex)).isEqualTo("SLA_ESCALATION");
        assertThat(single("select status from workflow_instance where exception_id = ?", ex)).isEqualTo("ACTION_IN_PROGRESS");
        assertThat(single("select assignee_id from assignment where exception_id = ? and active", ex)).isEqualTo("senior-operators");
    }

    @Test
    void manualPolicy_createsWorkItem_operatorResolves() throws Exception {
        UUID ex = classified("SHIPMENT_DATA_INVALID");
        awaitValue("select type from work_item where exception_id = ?", ex, "MANUAL_RESOLUTION");
        assertThat(outbox(ex, IntegrationMessages.EXECUTE_ACTION)).isEmpty();

        UUID workItem = jdbc.queryForObject("select id from work_item where exception_id = ?", UUID.class, ex);
        rest.postForObject("/api/v1/work-items/{id}/complete",
                Map.of("outcome", "RESOLVED", "operator", "petrov", "resolution", "ADDRESS_FIXED"), Map.class, workItem);

        assertThat(awaitOutbox(ex, ExceptionCommands.RESOLVE, 1).get(0).get("resolution").asText()).isEqualTo("ADDRESS_FIXED");
        assertThat(single("select verification_status from resolution where exception_id = ?", ex)).isEqualTo("MANUAL");
        assertThat(rest.postForEntity("/api/v1/work-items/{id}/complete", Map.of("outcome", "RESOLVED", "operator", "petrov"), Map.class, workItem)
                .getStatusCode()).as("already completed").isEqualTo(HttpStatus.CONFLICT);
        assertThat(rest.getForEntity("/api/v1/exceptions/{id}/resolution", Map.class, UUID.randomUUID()).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void exceptionClosedElsewhere_cancelsWorkflowAndItsChecks() throws Exception {
        UUID ex = classified("DELIVERY_FAILURE");
        integration(IntegrationMessages.UNKNOWN, new IntegrationMessages.IntegrationUnknown(operationOf(ex, 1), ex, "DELIVERY", "REQUEST_TIMEOUT"));
        awaitReconciliationQuery(ex, 1);

        send(Topics.EXCEPTION_EVENTS, UUID.randomUUID(), ExceptionEvents.CLOSED, ex,
                new ExceptionEvents.ExceptionClosed(ex, "CANCELLED_BY_OPERATOR", false, "ivanov"));

        awaitValue("select status from workflow_instance where exception_id = ?", ex, "CANCELLED");
        assertThat(single("select status from reconciliation_task where exception_id = ?", ex)).isEqualTo("CANCELLED");
        assertThat(single("select status from sla_instance where exception_id = ?", ex)).isEqualTo("CANCELLED");
    }

    // ------------------------------------------------------------------ helpers

    private UUID classified(String exceptionType) throws Exception {
        UUID ex = UUID.randomUUID();
        sendClassified(UUID.randomUUID(), ex, exceptionType);
        return ex;
    }

    private void sendClassified(UUID messageId, UUID ex, String exceptionType) throws Exception {
        send(Topics.EXCEPTION_EVENTS, messageId, ExceptionEvents.CLASSIFIED, ex, new ExceptionEvents.ExceptionClassified(
                ex, exceptionType, "DELIVERY", "HIGH", "P1", "MSK", "DELIVERY", "SHIPMENT", "SHP-" + ex, "AUTO"));
    }

    private void integration(String type, Object payload) throws Exception {
        UUID ex = UUID.fromString(mapper.valueToTree(payload).get("exceptionId").asText());
        send(Topics.INTEGRATION_EVENTS, UUID.randomUUID(), type, ex, payload);
    }

    private void observation(UUID queryId, UUID ex, boolean found, String state) throws Exception {
        integration(IntegrationMessages.STATE_RECEIVED,
                new IntegrationMessages.ExternalStateReceived(queryId, ex, "DELIVERY", found, state, Instant.now()));
    }

    private void stateQueryFailed(UUID queryId, UUID ex) throws Exception {
        integration(IntegrationMessages.STATE_QUERY_FAILED,
                new IntegrationMessages.ExternalStateQueryFailed(queryId, ex, "DELIVERY", "CIRCUIT_OPEN"));
    }

    private void send(String topic, UUID messageId, String type, UUID key, Object payload) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key.toString(), mapper.writeValueAsString(payload));
        record.headers().add(MessageHeaders.ID, messageId.toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add(MessageHeaders.EVENT_TYPE, type.getBytes(StandardCharsets.UTF_8));
        record.headers().add(MessageHeaders.CORRELATION_ID, ("corr-" + key).getBytes(StandardCharsets.UTF_8));
        kafka.send(record).get();
    }

    private UUID operationOf(UUID ex, int attemptNo) {
        return UUID.fromString(awaitOutbox(ex, IntegrationMessages.EXECUTE_ACTION, attemptNo).get(attemptNo - 1).get("operationId").asText());
    }

    /** n-th RECONCILIATION query written by the worker. */
    private UUID awaitReconciliationQuery(UUID ex, int n) {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(reconciliationQueries(ex)).hasSizeGreaterThanOrEqualTo(n));
        return UUID.fromString(reconciliationQueries(ex).get(n - 1).get("queryId").asText());
    }

    private List<JsonNode> reconciliationQueries(UUID ex) {
        return outbox(ex, IntegrationMessages.QUERY_EXTERNAL_STATE).stream()
                .filter(q -> q.get("purpose").asText().equals("RECONCILIATION")).toList();
    }

    private List<JsonNode> awaitOutbox(UUID ex, String type, int atLeast) {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(outbox(ex, type)).hasSizeGreaterThanOrEqualTo(atLeast));
        return outbox(ex, type);
    }

    private List<JsonNode> outbox(UUID ex, String type) {
        return jdbc.query("select payload::text from outbox where aggregate_id = ? and event_type = ? order by aggregate_version",
                (rs, i) -> read(rs.getString(1)), ex.toString(), type);
    }

    private List<String> outboxTypes(UUID ex) {
        return jdbc.queryForList("select event_type from outbox where aggregate_id = ? order by aggregate_version", String.class, ex.toString());
    }

    private void awaitValue(String sql, UUID ex, String expected) {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(jdbc.queryForList(sql, String.class, ex)).contains(expected));
    }

    private String single(String sql, UUID arg) {
        return jdbc.queryForObject(sql, String.class, arg);
    }

    private long count(String sql, UUID arg) {
        return jdbc.queryForObject(sql, Long.class, arg);
    }

    private JsonNode read(String json) {
        try {
            return mapper.readTree(json);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
