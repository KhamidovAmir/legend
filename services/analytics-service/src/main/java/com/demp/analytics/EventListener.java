package com.demp.analytics;

import com.demp.contracts.Topics;
import com.demp.platform.messaging.EnvelopeReader;
import com.demp.platform.messaging.InvalidMessageException;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class EventListener {

    private final EnvelopeReader reader;
    private final EventProjector projector;
    private final JdbcClient jdbc;
    private final MeterRegistry meters;

    @KafkaListener(id = "analytics-events", groupId = "analytics-service.events",
            topics = {Topics.EXCEPTION_EVENTS, Topics.RESOLUTION_EVENTS, Topics.INTEGRATION_EVENTS})
    public void onEvent(ConsumerRecord<String, String> record) {
        try {
            boolean stored = projector.project(reader.read(record));
            meters.counter("analytics_events_total", "topic", record.topic(), "result", stored ? "stored" : "duplicate").increment();
        } catch (InvalidMessageException e) {
            // analytics must never stop because of one bad event: park it and go on
            log.warn("Event rejected {}-{}@{}: {}", record.topic(), record.partition(), record.offset(), e.getMessage());
            jdbc.sql("insert into rejected_event (topic, partition_no, offset_no, reason, raw) values (?, ?, ?, ?, ?)")
                    .params(record.topic(), record.partition(), record.offset(), e.getMessage(), record.value()).update();
            meters.counter("analytics_events_total", "topic", record.topic(), "result", "rejected").increment();
        }
    }
}
