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

/** A running resolution process of one Exception. */
@Entity
@Table(name = "workflow_instance")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class WorkflowInstance {

    public enum Status {
        ACTION_IN_PROGRESS, VERIFYING, MANUAL, COMPLETED, CANCELLED;

        public boolean isFinished() {
            return this == COMPLETED || this == CANCELLED;
        }
    }

    @Id
    private UUID id;
    private UUID exceptionId;
    private String policyCode;
    private Integer policyVersion;
    @Enumerated(EnumType.STRING)
    private Status status;
    private String entityType;
    private String entityId;
    private String resolutionResult;
    private UUID pendingVerificationQueryId;
    private int verificationQueryFailures;
    private String correlationId;
    private long revision;
    @Version
    private Long version;
    private Instant createdAt;
    private Instant updatedAt;
    private Instant completedAt;

    public static WorkflowInstance start(UUID exceptionId, ResolutionPolicy policy, String entityType, String entityId,
                                         String correlationId, Instant now) {
        WorkflowInstance w = new WorkflowInstance();
        w.id = UUID.randomUUID();
        w.exceptionId = exceptionId;
        boolean automated = policy != null && policy.getExecutionMode() == ResolutionPolicy.ExecutionMode.EXTERNAL_AUTOMATED;
        if (policy != null) {
            w.policyCode = policy.getCode();
            w.policyVersion = policy.getVersion();
            w.resolutionResult = policy.getResolutionResult();
        }
        w.status = automated ? Status.ACTION_IN_PROGRESS : Status.MANUAL;
        w.entityType = entityType;
        w.entityId = entityId;
        w.correlationId = correlationId;
        w.createdAt = now;
        w.updatedAt = now;
        return w;
    }

    /** Every outgoing message of the workflow gets the next revision as aggregateVersion. */
    public long nextRevision() {
        return ++revision;
    }

    public void actionInProgress(Instant now) {
        change(Status.ACTION_IN_PROGRESS, now);
        this.pendingVerificationQueryId = null;
    }

    public void verifying(UUID queryId, Instant now) {
        change(Status.VERIFYING, now);
        this.pendingVerificationQueryId = queryId;
    }

    public int verificationQueryFailed() {
        return ++verificationQueryFailures;
    }

    public void manual(Instant now) {
        change(Status.MANUAL, now);
        this.pendingVerificationQueryId = null;
    }

    public void complete(Instant now) {
        change(Status.COMPLETED, now);
        this.pendingVerificationQueryId = null;
        this.completedAt = now;
    }

    public void cancel(Instant now) {
        change(Status.CANCELLED, now);
        this.pendingVerificationQueryId = null;
        this.completedAt = now;
    }

    private void change(Status target, Instant now) {
        if (status.isFinished()) {
            throw new IllegalStateException("Workflow %s is already %s".formatted(id, status));
        }
        this.status = target;
        this.updatedAt = now;
    }
}
