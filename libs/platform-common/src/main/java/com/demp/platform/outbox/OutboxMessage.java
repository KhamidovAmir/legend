package com.demp.platform.outbox;

import java.util.UUID;
import lombok.Builder;

/**
 * @param destination      Kafka topic
 * @param aggregateId      becomes the Kafka record key (ordering per aggregate)
 * @param aggregateVersion monotonically growing per aggregate; lets consumers detect out-of-order delivery
 */
@Builder
public record OutboxMessage(
        UUID id,
        String destination,
        String aggregateType,
        String aggregateId,
        long aggregateVersion,
        String eventType,
        Integer eventVersion,
        Object payload,
        String correlationId,
        String causationId) {
}
