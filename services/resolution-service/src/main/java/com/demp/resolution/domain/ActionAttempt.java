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
 * One business attempt to perform an Action. A new attempt appears only when the workflow DECIDES to repeat the
 * action; it is not an HTTP retry. Its id is the idempotency key sent to the external system.
 */
@Entity
@Table(name = "action_attempt")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ActionAttempt {

    public enum Status {
        SENT, ACCEPTED, UNKNOWN, COMPLETED, FAILED, NOT_APPLIED;

        public boolean awaitsResult() {
            return this == SENT || this == ACCEPTED;
        }
    }

    @Id
    private UUID id;
    private UUID actionId;
    private UUID exceptionId;
    private int attemptNo;
    @Enumerated(EnumType.STRING)
    private Status status;
    /** id of IntegrationOperation, generated here so that command redelivery cannot create a second operation */
    private UUID operationId;
    private String idempotencyKey;
    private String outcomeResult;
    private String failureReason;
    @Version
    private Long version;
    private Instant createdAt;
    private Instant finishedAt;

    public static ActionAttempt start(Action action, int attemptNo, Instant now) {
        ActionAttempt a = new ActionAttempt();
        a.id = UUID.randomUUID();
        a.actionId = action.getId();
        a.exceptionId = action.getExceptionId();
        a.attemptNo = attemptNo;
        a.status = Status.SENT;
        a.operationId = UUID.randomUUID();
        a.idempotencyKey = a.id.toString();
        a.createdAt = now;
        return a;
    }

    public void accepted() {
        this.status = Status.ACCEPTED;
    }

    public void unknown() {
        this.status = Status.UNKNOWN;
    }

    public void completed(String result, Instant now) {
        this.status = Status.COMPLETED;
        this.outcomeResult = result;
        this.finishedAt = now;
    }

    public void failed(String reason, Instant now) {
        this.status = Status.FAILED;
        this.failureReason = reason;
        this.finishedAt = now;
    }

    public void notApplied(Instant now) {
        this.status = Status.NOT_APPLIED;
        this.finishedAt = now;
    }
}
