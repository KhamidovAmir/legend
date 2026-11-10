package com.demp.exceptions.messaging;

import com.demp.contracts.ExceptionCommands.ExceptionCommand;
import com.demp.contracts.Topics;
import com.demp.exceptions.lifecycle.ExceptionLifecycleService;
import com.demp.platform.inbox.IdempotentConsumer;
import com.demp.platform.messaging.EnvelopeReader;
import com.demp.platform.messaging.EventEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Commands from Resolution Service. Inbox makes redelivery harmless. */
@Component
@RequiredArgsConstructor
public class CommandListener {

    static final String CONSUMER_ID = "exception-commands";

    private final EnvelopeReader reader;
    private final IdempotentConsumer idempotent;
    private final ExceptionLifecycleService lifecycle;
    private final ObjectMapper mapper;

    @KafkaListener(topics = Topics.EXCEPTION_COMMANDS, id = "exception-commands")
    public void onCommand(ConsumerRecord<String, String> record) {
        EventEnvelope envelope = reader.read(record);
        idempotent.handle(CONSUMER_ID, envelope,
                e -> lifecycle.handleCommand(e.type(), e.payloadAs(ExceptionCommand.class, mapper)));
    }
}
