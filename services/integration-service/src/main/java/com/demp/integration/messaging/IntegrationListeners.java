package com.demp.integration.messaging;

import com.demp.contracts.IntegrationMessages;
import com.demp.contracts.IntegrationMessages.ExecuteAction;
import com.demp.contracts.IntegrationMessages.ExternalCommandResult;
import com.demp.contracts.IntegrationMessages.QueryExternalState;
import com.demp.contracts.Topics;
import com.demp.integration.adapter.DeliveryAdapter;
import com.demp.integration.execution.OperationExecutor;
import com.demp.platform.messaging.EnvelopeReader;
import com.demp.platform.messaging.EventEnvelope;
import com.demp.platform.messaging.InvalidMessageException;
import com.demp.platform.messaging.MessageContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Commands and queries are consumed from different topics by different listener containers (= different thread
 * pools), so a flood of reconciliation queries cannot delay commands.
 * Idempotency: the operation id from the message is the primary key of integration_operation.
 */
@Component
@RequiredArgsConstructor
public class IntegrationListeners {

    private final EnvelopeReader reader;
    private final OperationExecutor executor;
    private final ObjectMapper mapper;

    @KafkaListener(topics = Topics.INTEGRATION_COMMANDS, id = "integration-commands",
            concurrency = "${integration.commands-concurrency}")
    public void onCommand(ConsumerRecord<String, String> record) {
        EventEnvelope envelope = reader.read(record);
        if (!IntegrationMessages.EXECUTE_ACTION.equals(envelope.type())) {
            throw new InvalidMessageException("Unexpected message type on commands topic: " + envelope.type());
        }
        try (MessageContext.Scope ignored = MessageContext.open(envelope)) {
            executor.execute(envelope.payloadAs(ExecuteAction.class, mapper));
        }
    }

    @KafkaListener(topics = Topics.INTEGRATION_QUERIES, id = "integration-queries",
            concurrency = "${integration.queries-concurrency}")
    public void onQuery(ConsumerRecord<String, String> record) {
        EventEnvelope envelope = reader.read(record);
        try (MessageContext.Scope ignored = MessageContext.open(envelope)) {
            executor.query(envelope.payloadAs(QueryExternalState.class, mapper));
        }
    }

    /** Async results published by the Delivery team in THEIR format (no envelope headers). */
    @KafkaListener(topics = Topics.EXTERNAL_DELIVERY_RESULTS, id = "delivery-results")
    public void onDeliveryResult(ConsumerRecord<String, String> record) {
        ExternalCommandResult result;
        try {
            result = mapper.readValue(record.value(), ExternalCommandResult.class);
        } catch (Exception e) {
            throw new InvalidMessageException("Unparseable delivery result at offset " + record.offset(), e);
        }
        if (!executor.onExternalResult(DeliveryAdapter.SYSTEM, result)) {
            // The result can overtake our own commit of "ACCEPTED": retry with backoff, then DLT (error handler).
            throw new IllegalStateException("No operation for external request " + result.externalRequestId());
        }
    }
}
