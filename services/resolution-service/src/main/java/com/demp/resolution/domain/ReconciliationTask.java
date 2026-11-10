package com.demp.resolution.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.time.Instant;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Finds out the real state of an external system after an UNKNOWN command result.
 * READY -> QUERYING -> (READY ...) -> RESOLVED | ESCALATED | CANCELLED. The result is stored separately from the status.
 */
@Entity
@Table(name = "reconciliation_task")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ReconciliationTask {

    public enum Status {
        READY, QUERYING, RESOLVED, ESCALATED, CANCELLED;

        public boolean isActive() {
            return this == READY || this == QUERYING;
        }
    }

    public enum Result { APPLIED, NOT_APPLIED, STILL_PROCESSING, INCONCLUSIVE, CONFLICT }

    @Id
    private UUID id;
    private UUID attemptId;
    private UUID actionId;
    private UUID exceptionId;
    private String targetSystem;
    private String actionType;
    private String entityType;
    private String entityId;
    @Enumerated(EnumType.STRING)
    private Status status;
    @Enumerated(EnumType.STRING)
    private Result result;
    private String reason;
    private int checksCount;
    private int notAppliedStreak;
    /** answers to any other query id are stale and ignored */
    private UUID currentQueryId;
    private Instant nextCheckAt;
    private Instant deadlineAt;
    @Version
    private Long version;
    private Instant createdAt;
    private Instant updatedAt;

    public static ReconciliationTask create(ActionAttempt attempt, Action action, String reason, Instant firstCheckAt,
                                            Instant deadlineAt, Instant now) {
        ReconciliationTask t = new ReconciliationTask();
        t.id = UUID.randomUUID();
        t.attemptId = attempt.getId();
        t.actionId = action.getId();
        t.exceptionId = action.getExceptionId();
        t.targetSystem = action.getTargetSystem();
        t.actionType = action.getActionType();
        t.entityType = action.getEntityType();
        t.entityId = action.getEntityId();
        t.status = Status.READY;
        t.reason = reason;
        t.nextCheckAt = firstCheckAt;
        t.deadlineAt = deadlineAt;
        t.createdAt = now;
        t.updatedAt = now;
        return t;
    }

    public boolean isExpired(Instant now) {
        return !now.isBefore(deadlineAt);
    }

    /** A check is sent; if no answer arrives until {@code answerDeadline}, the worker picks the task up again. */
    public UUID startQuery(Instant answerDeadline, Instant now) {
        requireActive();
        this.status = Status.QUERYING;
        this.currentQueryId = UUID.randomUUID();
        this.checksCount++;
        this.nextCheckAt = answerDeadline;
        this.updatedAt = now;
        return currentQueryId;
    }

    /** Not conclusive yet: check again later. */
    public void scheduleNext(Result interim, Instant nextCheckAt, Instant now) {
        requireActive();
        this.status = Status.READY;
        this.result = interim;
        this.notAppliedStreak = interim == Result.NOT_APPLIED ? notAppliedStreak + 1 : 0;
        this.currentQueryId = null;
        this.nextCheckAt = nextCheckAt;
        this.updatedAt = now;
    }

    /** The query itself failed (external system unavailable): does not count as an observation. */
    public void queryFailed(Instant nextCheckAt, Instant now) {
        requireActive();
        this.status = Status.READY;
        this.currentQueryId = null;
        this.nextCheckAt = nextCheckAt;
        this.updatedAt = now;
    }

    public void resolve(Result result, Instant now) {
        finish(Status.RESOLVED, result, now);
    }

    public void escalate(Result result, Instant now) {
        finish(Status.ESCALATED, result, now);
    }

    public void cancel(Instant now) {
        finish(Status.CANCELLED, result, now);
    }

    private void finish(Status status, Result result, Instant now) {
        requireActive();
        this.status = status;
        this.result = result;
        this.currentQueryId = null;
        this.updatedAt = now;
    }

    private void requireActive() {
        if (!status.isActive()) {
            throw new IllegalStateException("ReconciliationTask %s is already %s".formatted(id, status));
        }
    }
}
