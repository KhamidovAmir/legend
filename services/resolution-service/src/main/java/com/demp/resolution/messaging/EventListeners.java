package com.demp.resolution.messaging;

import com.demp.contracts.ExceptionEvents;
import com.demp.contracts.IntegrationMessages;
import com.demp.contracts.Topics;
import com.demp.platform.inbox.IdempotentConsumer;
import com.demp.platform.messaging.EnvelopeReader;
import com.demp.platform.messaging.EventEnvelope;
import com.demp.resolution.workflow.ResolutionWorkflowService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka entry points. Every message goes through the inbox: handler + inbox row + outbox rows commit together,
 * so duplicates (redelivery, connector restart, re-publication) never change the business state twice.
 */
@Component
@RequiredArgsConstructor
public class EventListeners {

    private final EnvelopeReader reader;
    private final IdempotentConsumer idempotent;
    private final ResolutionWorkflowService workflow;
    private final ObjectMapper mapper;

    @KafkaListener(topics = Topics.EXCEPTION_EVENTS, id = "resolution-exception-events", groupId = "resolution-service.exception-events")
    public void onExceptionEvent(ConsumerRecord<String, String> record) {
        EventEnvelope envelope = reader.read(record);
        switch (envelope.type()) {
            case ExceptionEvents.CLASSIFIED -> idempotent.handle("resolution-exception-events", envelope, e ->
                    workflow.onExceptionClassified(e.payloadAs(ExceptionEvents.ExceptionClassified.class, mapper)));
            case ExceptionEvents.CLOSED -> idempotent.handle("resolution-exception-events", envelope, e ->
                    workflow.onExceptionClosed(e.payloadAs(ExceptionEvents.ExceptionClosed.class, mapper).exceptionId()));
            default -> { /* other exception events are not interesting for resolution */ }
        }
    }

    @KafkaListener(topics = Topics.INTEGRATION_EVENTS, id = "resolution-integration-events", groupId = "resolution-service.integration-events")
    public void onIntegrationEvent(ConsumerRecord<String, String> record) {
        EventEnvelope envelope = reader.read(record);
        idempotent.handle("resolution-integration-events", envelope, e -> {
            switch (e.type()) {
                case IntegrationMessages.ACCEPTED ->
                        workflow.onAccepted(e.payloadAs(IntegrationMessages.IntegrationAccepted.class, mapper));
                case IntegrationMessages.COMPLETED ->
                        workflow.onCompleted(e.payloadAs(IntegrationMessages.IntegrationCompleted.class, mapper));
                case IntegrationMessages.FAILED ->
                        workflow.onFailed(e.payloadAs(IntegrationMessages.IntegrationFailed.class, mapper));
                case IntegrationMessages.UNKNOWN ->
                        workflow.onUnknown(e.payloadAs(IntegrationMessages.IntegrationUnknown.class, mapper));
                case IntegrationMessages.STATE_RECEIVED ->
                        workflow.onStateReceived(e.payloadAs(IntegrationMessages.ExternalStateReceived.class, mapper));
                case IntegrationMessages.STATE_QUERY_FAILED ->
                        workflow.onStateQueryFailed(e.payloadAs(IntegrationMessages.ExternalStateQueryFailed.class, mapper));
                default -> { /* unknown event types are ignored: producers may add new ones */ }
            }
        });
    }
}
