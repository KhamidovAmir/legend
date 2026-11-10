package com.demp.integration.messaging;

import com.demp.contracts.Topics;
import com.demp.integration.domain.IntegrationOperation;
import com.demp.platform.outbox.OutboxMessage;
import com.demp.platform.outbox.OutboxWriter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class IntegrationEvents {

    private final OutboxWriter outbox;

    public void publish(IntegrationOperation op, String type, Object payload) {
        outbox.write(OutboxMessage.builder()
                .destination(Topics.INTEGRATION_EVENTS)
                .aggregateType("IntegrationOperation")
                // keyed by exception: Resolution receives results of one exception in order
                .aggregateId(op.getExceptionId() != null ? op.getExceptionId().toString() : op.getId().toString())
                .aggregateVersion(op.nextRevision())
                .eventType(type)
                .payload(payload)
                .correlationId(op.getCorrelationId())
                .build());
    }
}
