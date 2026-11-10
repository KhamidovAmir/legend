package com.demp.platform.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.UUID;

/** Internal message: metadata from Kafka headers + JSON payload from the record value. */
public record EventEnvelope(
        UUID id,
        String type,
        int version,
        String aggregateType,
        String aggregateId,
        long aggregateVersion,
        String correlationId,
        String causationId,
        Instant occurredAt,
        String topic,
        JsonNode payload) {

    public <T> T payloadAs(Class<T> type, ObjectMapper mapper) {
        try {
            return mapper.treeToValue(payload, type);
        } catch (Exception e) {
            throw new InvalidMessageException("Cannot map payload of " + this.type + " to " + type.getSimpleName(), e);
        }
    }
}
