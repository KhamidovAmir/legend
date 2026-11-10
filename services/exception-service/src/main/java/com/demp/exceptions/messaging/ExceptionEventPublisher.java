package com.demp.exceptions.messaging;

import com.demp.contracts.ExceptionEvents;
import com.demp.contracts.Topics;
import com.demp.exceptions.domain.ExceptionRecord;
import com.demp.exceptions.domain.ExceptionReference;
import com.demp.exceptions.domain.ExceptionStatus;
import com.demp.platform.outbox.OutboxMessage;
import com.demp.platform.outbox.OutboxWriter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/** Writes domain events of the Exception aggregate to the outbox (same transaction as the state change). */
@Component
@RequiredArgsConstructor
public class ExceptionEventPublisher {

    private final OutboxWriter outbox;

    public void created(ExceptionRecord e, ExceptionReference ref) {
        publish(e, ExceptionEvents.CREATED, new ExceptionEvents.ExceptionCreated(e.getId(), e.getSourceSystem(),
                ref.getEntityType(), ref.getEntityId(), e.getRegion(), e.getDetectedAt()));
    }

    public void classified(ExceptionRecord e, ExceptionReference ref) {
        publish(e, ExceptionEvents.CLASSIFIED, new ExceptionEvents.ExceptionClassified(e.getId(),
                e.getExceptionType(), e.getCategory(), e.getSeverity(), e.getPriority(), e.getRegion(),
                e.getSourceSystem(), ref.getEntityType(), ref.getEntityId(), e.getClassifiedBy()));
    }

    public void statusChanged(ExceptionRecord e, ExceptionStatus from, String actor) {
        publish(e, ExceptionEvents.STATUS_CHANGED, new ExceptionEvents.ExceptionStatusChanged(e.getId(),
                from.name(), e.getStatus().name(), actor));
    }

    public void closed(ExceptionRecord e, String actor) {
        publish(e, ExceptionEvents.CLOSED, new ExceptionEvents.ExceptionClosed(e.getId(),
                e.getCloseReason() == null ? null : e.getCloseReason().name(), e.getCloseReason() == null, actor));
    }

    public void commandRejected(ExceptionRecord e, String commandType, String reason) {
        publish(e, ExceptionEvents.COMMAND_REJECTED, new ExceptionEvents.ExceptionCommandRejected(e.getId(),
                commandType, e.getStatus().name(), reason));
    }

    private void publish(ExceptionRecord e, String type, Object payload) {
        outbox.write(OutboxMessage.builder()
                .destination(Topics.EXCEPTION_EVENTS)
                .aggregateType("Exception")
                .aggregateId(e.getId().toString())
                .aggregateVersion(e.getRevision())
                .eventType(type)
                .payload(payload)
                .build());
    }
}
