package com.demp.platform.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

@RequiredArgsConstructor
public class EnvelopeReader {

    private final ObjectMapper mapper;

    public EventEnvelope read(ConsumerRecord<String, String> record) {
        String id = required(record, MessageHeaders.ID);
        String type = required(record, MessageHeaders.EVENT_TYPE);
        try {
            JsonNode payload = mapper.readTree(record.value());
            // Debezium may deliver a jsonb payload as an escaped JSON string if expand.json.payload is off
            if (payload != null && payload.isTextual()) {
                payload = mapper.readTree(payload.asText());
            }
            return new EventEnvelope(
                    UUID.fromString(id),
                    type,
                    Integer.parseInt(optional(record, MessageHeaders.EVENT_VERSION, "1")),
                    optional(record, MessageHeaders.AGGREGATE_TYPE, null),
                    optional(record, MessageHeaders.AGGREGATE_ID, record.key()),
                    Long.parseLong(optional(record, MessageHeaders.AGGREGATE_VERSION, "0")),
                    optional(record, MessageHeaders.CORRELATION_ID, null),
                    optional(record, MessageHeaders.CAUSATION_ID, null),
                    parseInstant(optional(record, MessageHeaders.OCCURRED_AT, null), record.timestamp()),
                    record.topic(),
                    payload);
        } catch (InvalidMessageException e) {
            throw e;
        } catch (Exception e) {
            throw new InvalidMessageException("Malformed message %s-%d@%d: %s"
                    .formatted(record.topic(), record.partition(), record.offset(), e.getMessage()), e);
        }
    }

    private static Instant parseInstant(String value, long fallbackEpochMs) {
        if (value == null || value.isBlank()) {
            return Instant.ofEpochMilli(fallbackEpochMs);
        }
        return OffsetDateTime.parse(value).toInstant();
    }

    private static String required(ConsumerRecord<String, String> record, String name) {
        String value = optional(record, name, null);
        if (value == null || value.isBlank()) {
            throw new InvalidMessageException("Missing header '%s' in %s-%d@%d"
                    .formatted(name, record.topic(), record.partition(), record.offset()));
        }
        return value;
    }

    private static String optional(ConsumerRecord<String, String> record, String name, String defaultValue) {
        Header header = record.headers().lastHeader(name);
        if (header == null || header.value() == null) {
            return defaultValue;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
