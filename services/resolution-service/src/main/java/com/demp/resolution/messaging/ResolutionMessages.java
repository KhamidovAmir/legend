package com.demp.resolution.messaging;

import com.demp.contracts.ExceptionCommands.ExceptionCommand;
import com.demp.contracts.IntegrationMessages;
import com.demp.contracts.IntegrationMessages.ExecuteAction;
import com.demp.contracts.IntegrationMessages.QueryExternalState;
import com.demp.contracts.Topics;
import com.demp.platform.outbox.OutboxMessage;
import com.demp.platform.outbox.OutboxWriter;
import com.demp.resolution.domain.WorkflowInstance;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * Everything Resolution Service sends goes through the outbox table (delivered by Debezium).
 * All messages are keyed by exceptionId, so messages about one exception keep their order within a topic.
 */
@Component
@RequiredArgsConstructor
public class ResolutionMessages {

    private final OutboxWriter outbox;

    public void exceptionCommand(WorkflowInstance wf, String type, ExceptionCommand command) {
        write(wf, Topics.EXCEPTION_COMMANDS, type, null, command);
    }

    public void executeAction(WorkflowInstance wf, ExecuteAction command) {
        // message id == operation id: handy when grepping logs of both services
        write(wf, Topics.INTEGRATION_COMMANDS, IntegrationMessages.EXECUTE_ACTION, command.operationId(), command);
    }

    public void queryExternalState(WorkflowInstance wf, QueryExternalState query) {
        write(wf, Topics.INTEGRATION_QUERIES, IntegrationMessages.QUERY_EXTERNAL_STATE, query.queryId(), query);
    }

    public void event(WorkflowInstance wf, String type, Object payload) {
        write(wf, Topics.RESOLUTION_EVENTS, type, null, payload);
    }

    private void write(WorkflowInstance wf, String topic, String type, UUID id, Object payload) {
        outbox.write(OutboxMessage.builder()
                .id(id)
                .destination(topic)
                .aggregateType("Exception")
                .aggregateId(wf.getExceptionId().toString())
                .aggregateVersion(wf.nextRevision())
                .eventType(type)
                .payload(payload)
                // scheduler threads have no message context, so the correlation id is taken from the workflow
                .correlationId(wf.getCorrelationId())
                .build());
    }
}
