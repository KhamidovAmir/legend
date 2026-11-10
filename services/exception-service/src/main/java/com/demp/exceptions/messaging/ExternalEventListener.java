package com.demp.exceptions.messaging;

import com.demp.contracts.ExternalEvent;
import com.demp.contracts.Topics;
import com.demp.exceptions.registration.RegistrationService;
import com.demp.platform.messaging.InvalidMessageException;
import com.demp.platform.messaging.MessageContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Entry point for events of external systems that belong to our bounded context. */
@Slf4j
@Component
@RequiredArgsConstructor
public class ExternalEventListener {

    private final RegistrationService registration;
    private final ObjectMapper mapper;

    @KafkaListener(topics = Topics.EXTERNAL_DELIVERY_EVENTS, id = "external-events")
    public void onExternalEvent(ConsumerRecord<String, String> record) {
        ExternalEvent event = parse(record);
        try (MessageContext.Scope ignored = MessageContext.open(
                event.correlationId() != null ? event.correlationId() : event.eventId(), event.eventId(), null)) {
            registration.register(event);
        }
    }

    private ExternalEvent parse(ConsumerRecord<String, String> record) {
        ExternalEvent event;
        try {
            event = mapper.readValue(record.value(), ExternalEvent.class);
        } catch (Exception e) {
            throw new InvalidMessageException("Unparseable external event at offset " + record.offset(), e);
        }
        if (isBlank(event.eventId()) || isBlank(event.eventType()) || isBlank(event.sourceSystem())
                || isBlank(event.entityType()) || isBlank(event.entityId())) {
            throw new InvalidMessageException("External event at offset %d lacks mandatory fields".formatted(record.offset()));
        }
        return event;
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
