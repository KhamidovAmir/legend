package com.demp.contracts;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

/**
 * Message format of external systems (anti-corruption boundary): external teams do not use our headers,
 * so all metadata travels in the body.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ExternalEvent(
        String eventId,
        String eventType,
        String sourceSystem,
        String entityType,
        String entityId,
        String region,
        Instant occurredAt,
        String correlationId,
        JsonNode payload) {
}
