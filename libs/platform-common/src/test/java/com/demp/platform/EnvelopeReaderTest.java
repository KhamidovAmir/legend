package com.demp.platform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.demp.platform.messaging.EnvelopeReader;
import com.demp.platform.messaging.EventEnvelope;
import com.demp.platform.messaging.InvalidMessageException;
import com.demp.platform.messaging.MessageHeaders;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class EnvelopeReaderTest {

    private final EnvelopeReader reader = new EnvelopeReader(new ObjectMapper());

    @Test
    void readsMetadataFromHeaders() {
        UUID id = UUID.randomUUID();
        ConsumerRecord<String, String> record = record("{\"a\":1}");
        header(record, MessageHeaders.ID, id.toString());
        header(record, MessageHeaders.EVENT_TYPE, "Something");
        header(record, MessageHeaders.AGGREGATE_VERSION, "7");
        header(record, MessageHeaders.OCCURRED_AT, "2026-01-01T10:00:00Z");
        header(record, MessageHeaders.CORRELATION_ID, "corr-1");

        EventEnvelope envelope = reader.read(record);

        assertThat(envelope.id()).isEqualTo(id);
        assertThat(envelope.type()).isEqualTo("Something");
        assertThat(envelope.aggregateVersion()).isEqualTo(7);
        assertThat(envelope.aggregateId()).isEqualTo("key-1");
        assertThat(envelope.correlationId()).isEqualTo("corr-1");
        assertThat(envelope.occurredAt()).hasToString("2026-01-01T10:00:00Z");
        assertThat(envelope.payload().get("a").asInt()).isEqualTo(1);
    }

    @Test
    void unwrapsPayloadDeliveredAsJsonString() {
        ConsumerRecord<String, String> record = record("\"{\\\"a\\\":2}\"");
        header(record, MessageHeaders.ID, UUID.randomUUID().toString());
        header(record, MessageHeaders.EVENT_TYPE, "Something");

        assertThat(reader.read(record).payload().get("a").asInt()).isEqualTo(2);
    }

    @Test
    void rejectsMessageWithoutIdOrWithBrokenJson() {
        ConsumerRecord<String, String> noId = record("{}");
        assertThatThrownBy(() -> reader.read(noId)).isInstanceOf(InvalidMessageException.class);

        ConsumerRecord<String, String> broken = record("{not json");
        header(broken, MessageHeaders.ID, UUID.randomUUID().toString());
        header(broken, MessageHeaders.EVENT_TYPE, "Something");
        assertThatThrownBy(() -> reader.read(broken)).isInstanceOf(InvalidMessageException.class);
    }

    private static ConsumerRecord<String, String> record(String value) {
        return new ConsumerRecord<>("topic", 0, 0L, "key-1", value);
    }

    private static void header(ConsumerRecord<String, String> record, String name, String value) {
        record.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
    }
}
