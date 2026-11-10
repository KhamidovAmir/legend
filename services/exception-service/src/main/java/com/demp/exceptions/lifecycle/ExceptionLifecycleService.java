package com.demp.exceptions.lifecycle;

import com.demp.contracts.ExceptionCommands;
import com.demp.contracts.ExceptionCommands.ExceptionCommand;
import com.demp.exceptions.domain.CloseReason;
import com.demp.exceptions.domain.ExceptionHistory;
import com.demp.exceptions.domain.ExceptionRecord;
import com.demp.exceptions.domain.ExceptionReference;
import com.demp.exceptions.domain.ExceptionRelation;
import com.demp.exceptions.domain.ExceptionStatus;
import com.demp.exceptions.domain.IllegalTransitionException;
import com.demp.exceptions.domain.Repositories;
import com.demp.exceptions.messaging.ExceptionEventPublisher;
import com.demp.platform.messaging.InvalidMessageException;
import com.demp.platform.messaging.MessageContext;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Owner of the Exception state. Resolution Service only asks for transitions via commands. */
@Slf4j
@Service
@RequiredArgsConstructor
public class ExceptionLifecycleService {

    private final Repositories.ExceptionRecords exceptions;
    private final Repositories.References references;
    private final Repositories.Relations relations;
    private final Repositories.History history;
    private final ExceptionEventPublisher events;
    private final Clock clock;

    /**
     * Applies a command from Kafka. A command that does not fit the current state is NOT an infrastructure error:
     * retrying would not help, so it is answered with ExceptionCommandRejected instead of going to the DLT.
     */
    @Transactional
    public void handleCommand(String commandType, ExceptionCommand command) {
        ExceptionRecord e = exceptions.findById(command.exceptionId()).orElseThrow(() ->
                new InvalidMessageException("Command %s for unknown exception %s".formatted(commandType, command.exceptionId())));
        MessageContext.exceptionId(e.getId());
        String actor = command.actor() == null ? "resolution-service" : command.actor();
        ExceptionStatus from = e.getStatus();
        Instant now = clock.instant();
        try {
            switch (commandType) {
                case ExceptionCommands.START_PROGRESS, ExceptionCommands.RESUME -> e.startProgress(now);
                case ExceptionCommands.MARK_WAITING -> e.markWaiting(now);
                case ExceptionCommands.RESOLVE -> e.resolve(command.resolution(), now);
                case ExceptionCommands.CLOSE -> e.closeWithoutResolution(parseReason(command.reason()), now);
                default -> throw new InvalidMessageException("Unknown command type " + commandType);
            }
        } catch (IllegalTransitionException | IllegalArgumentException ex) {
            log.warn("Command {} rejected for exception {} in status {}: {}", commandType, e.getId(), from, ex.getMessage());
            history.save(new ExceptionHistory(e.getId(), "COMMAND_REJECTED", from, from, actor,
                    commandType + ": " + ex.getMessage(), now));
            events.commandRejected(e, commandType, ex.getMessage());
            return;
        }
        recordTransition(e, from, actor, commandType, now);
    }

    @Transactional
    public ExceptionRecord closeByOperator(UUID exceptionId, CloseReason reason, String operator) {
        ExceptionRecord e = get(exceptionId);
        ExceptionStatus from = e.getStatus();
        Instant now = clock.instant();
        e.closeWithoutResolution(reason, now);
        recordTransition(e, from, operator, "CLOSED_BY_OPERATOR", now);
        return e;
    }

    @Transactional
    public ExceptionRecord classifyManually(UUID exceptionId, String category, String severity, String priority,
                                            String operator) {
        ExceptionRecord e = get(exceptionId);
        Instant now = clock.instant();
        e.classify(category, severity, priority, "MANUAL:" + operator, now);
        history.save(new ExceptionHistory(e.getId(), "CLASSIFIED", ExceptionStatus.OPEN, ExceptionStatus.CLASSIFIED,
                operator, "severity=%s priority=%s".formatted(severity, priority), now));
        events.classified(e, primaryReference(e.getId()));
        return e;
    }

    @Transactional
    public ExceptionRelation link(UUID from, UUID to, ExceptionRelation.Type type, String operator) {
        get(from);
        get(to);
        Instant now = clock.instant();
        ExceptionRelation relation = relations.save(new ExceptionRelation(from, to, type, now));
        history.save(new ExceptionHistory(from, "RELATION_ADDED", null, null, operator, type + " " + to, now));
        return relation;
    }

    /** Final closing rule: RESOLVED and quiet for the configured period. Returns the number of closed exceptions. */
    @Transactional
    public int closeResolved(Duration quietPeriod, int batchSize) {
        Instant now = clock.instant();
        List<ExceptionRecord> batch = exceptions.lockResolvedBefore(now.minus(quietPeriod), batchSize);
        for (ExceptionRecord e : batch) {
            e.closeResolved(now);
            recordTransition(e, ExceptionStatus.RESOLVED, "system", "FINAL_CLOSING", now);
        }
        return batch.size();
    }

    @Transactional(readOnly = true)
    public ExceptionRecord get(UUID id) {
        return exceptions.findById(id).orElseThrow(() -> new NoSuchElementException("Exception %s not found".formatted(id)));
    }

    private void recordTransition(ExceptionRecord e, ExceptionStatus from, String actor, String cause, Instant now) {
        history.save(new ExceptionHistory(e.getId(), "STATUS_CHANGED", from, e.getStatus(), actor, cause, now));
        events.statusChanged(e, from, actor);
        if (e.getStatus() == ExceptionStatus.CLOSED) {
            events.closed(e, actor);
        }
        log.info("Exception {}: {} -> {} by {} ({})", e.getId(), from, e.getStatus(), actor, cause);
    }

    private ExceptionReference primaryReference(UUID exceptionId) {
        return references.findByExceptionId(exceptionId).stream().findFirst()
                .orElseThrow(() -> new IllegalStateException("Exception %s has no reference".formatted(exceptionId)));
    }

    private static CloseReason parseReason(String reason) {
        if (reason == null) {
            throw new IllegalArgumentException("closeReason is mandatory");
        }
        return CloseReason.valueOf(reason);
    }
}
