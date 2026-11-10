package com.demp.analytics;

import com.demp.contracts.ExceptionEvents;
import com.demp.contracts.IntegrationMessages;
import com.demp.contracts.ResolutionEvents;
import com.demp.platform.messaging.EventEnvelope;
import com.demp.platform.messaging.InvalidMessageException;
import com.fasterxml.jackson.databind.JsonNode;
import java.sql.Timestamp;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Normalization: events of three services with different payloads are folded into one analytical model.
 * Works only from events; operational databases of other services are never queried.
 */
@Component
@RequiredArgsConstructor
public class EventProjector {

    private final JdbcClient jdbc;

    /** @return false if the event was already stored (duplicate delivery) */
    @Transactional
    public boolean project(EventEnvelope event) {
        JsonNode p = event.payload();
        UUID exceptionId = exceptionId(p);

        int stored = jdbc.sql("""
                        insert into raw_event (event_id, topic, event_type, aggregate_id, aggregate_version, occurred_at, payload)
                        values (?, ?, ?, ?, ?, ?, ?::jsonb) on conflict (event_id) do nothing
                        """)
                .params(event.id(), event.topic(), event.type(), event.aggregateId(), event.aggregateVersion(),
                        Timestamp.from(event.occurredAt()), p.toString())
                .update();
        if (stored == 0) {
            return false;
        }
        // events may arrive in any order across topics: make sure the row exists, then apply the piece we know
        jdbc.sql("insert into exception_fact (exception_id) values (?) on conflict do nothing").param(exceptionId).update();
        Timestamp at = Timestamp.from(event.occurredAt());

        switch (event.type()) {
            case ExceptionEvents.CREATED -> update("""
                    update exception_fact set source_system = ?, region = coalesce(?, region),
                           detected_at = ?::timestamptz, status = coalesce(status, 'OPEN') where exception_id = ?
                    """, text(p, "sourceSystem"), text(p, "region"), text(p, "detectedAt"), exceptionId);
            case ExceptionEvents.CLASSIFIED -> update("""
                    update exception_fact set exception_type = ?, category = ?, severity = ?, priority = ?,
                           region = coalesce(?, region), source_system = coalesce(source_system, ?), classified_at = ?
                     where exception_id = ?
                    """, text(p, "exceptionType"), text(p, "category"), text(p, "severity"), text(p, "priority"),
                    text(p, "region"), text(p, "sourceSystem"), at, exceptionId);
            case ExceptionEvents.STATUS_CHANGED -> {
                String to = text(p, "toStatus");
                // an older status event delivered late must not overwrite a newer status
                update("update exception_fact set status = ?, status_version = ? where exception_id = ? and status_version < ?",
                        to, event.aggregateVersion(), exceptionId, event.aggregateVersion());
                if ("RESOLVED".equals(to)) {
                    update("update exception_fact set resolved_at = ? where exception_id = ?", at, exceptionId);
                }
            }
            case ExceptionEvents.CLOSED -> update(
                    "update exception_fact set closed_at = ?, close_reason = ? where exception_id = ?",
                    at, text(p, "closeReason"), exceptionId);
            case ResolutionEvents.WORKFLOW_STARTED -> update(
                    "update exception_fact set automated = ?, policy_code = ? where exception_id = ?",
                    p.path("automated").asBoolean(false), text(p, "policyCode"), exceptionId);
            case ResolutionEvents.ACTION_ATTEMPT_STARTED -> update("""
                    update exception_fact set business_attempts = business_attempts + 1,
                           first_action_at = coalesce(first_action_at, ?) where exception_id = ?
                    """, at, exceptionId);
            case ResolutionEvents.SLA_BREACHED -> update(
                    "update exception_fact set sla_breached = true where exception_id = ?", exceptionId);
            case ResolutionEvents.WORK_ITEM_CREATED -> update(
                    "update exception_fact set work_items = work_items + 1 where exception_id = ?", exceptionId);
            case ResolutionEvents.VERIFICATION_COMPLETED -> {
                if ("FAILED".equals(text(p, "status"))) {
                    update("update exception_fact set verification_failed = true where exception_id = ?", exceptionId);
                }
            }
            case IntegrationMessages.UNKNOWN -> {
                update("update exception_fact set unknown_count = unknown_count + 1 where exception_id = ?", exceptionId);
                externalCall(p, "UNKNOWN", at);
            }
            case IntegrationMessages.ACCEPTED -> externalCall(p, "ACCEPTED", at);
            case IntegrationMessages.COMPLETED -> externalCall(p, "COMPLETED", at);
            case IntegrationMessages.FAILED -> externalCall(p, "FAILED", at);
            default -> { /* stored in raw_event; not used by the current model */ }
        }
        jdbc.sql("update exception_fact set updated_at = now() where exception_id = ?").param(exceptionId).update();
        return true;
    }

    private void externalCall(JsonNode p, String outcome, Timestamp at) {
        update("""
                insert into external_call_stat (target_system, outcome, day, calls) values (?, ?, ?::date, 1)
                on conflict (target_system, outcome, day) do update set calls = external_call_stat.calls + 1
                """, text(p, "targetSystem") == null ? "UNKNOWN_SYSTEM" : text(p, "targetSystem"), outcome, at);
    }

    private void update(String sql, Object... params) {
        jdbc.sql(sql).params(params).update();
    }

    /** Validation: an event that cannot be attributed to an exception is useless for this model. */
    private static UUID exceptionId(JsonNode payload) {
        String value = payload == null ? null : text(payload, "exceptionId");
        if (value == null) {
            throw new InvalidMessageException("payload.exceptionId is missing");
        }
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            throw new InvalidMessageException("payload.exceptionId is not a UUID: " + value);
        }
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : value.asText();
    }
}
