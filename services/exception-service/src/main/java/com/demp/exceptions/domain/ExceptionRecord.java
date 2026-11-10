package com.demp.exceptions.domain;

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
 * Canonical Exception aggregate: what happened and what its current state is.
 * (Named ExceptionRecord to avoid clashing with java.lang.Exception.)
 * All state changes go through methods of this class, so the lifecycle invariants live in one place.
 */
@Entity
@Table(name = "exception_record")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ExceptionRecord {

    @Id
    private UUID id;
    private String sourceSystem;
    private String businessCorrelationKey;
    private String exceptionType;
    private String category;
    private String severity;
    private String priority;
    private String region;
    @Enumerated(EnumType.STRING)
    private ExceptionStatus status;
    @Enumerated(EnumType.STRING)
    private CloseReason closeReason;
    private String resolution;
    private String classifiedBy;
    /** business revision: +1 on every published change, travels as aggregateVersion */
    private long revision;
    /** optimistic lock; wrapper type on purpose: null tells Spring Data the entity is new (persist, not merge) */
    @Version
    private Long version;
    private Instant detectedAt;
    private Instant createdAt;
    private Instant updatedAt;
    private Instant classifiedAt;
    private Instant resolvedAt;
    private Instant closedAt;

    public static ExceptionRecord open(String sourceSystem, String correlationKey, String exceptionType,
                                       String region, Instant detectedAt, Instant now) {
        ExceptionRecord e = new ExceptionRecord();
        e.id = UUID.randomUUID();
        e.sourceSystem = sourceSystem;
        e.businessCorrelationKey = correlationKey;
        e.exceptionType = exceptionType;
        e.region = region;
        e.status = ExceptionStatus.OPEN;
        e.detectedAt = detectedAt != null ? detectedAt : now;
        e.createdAt = now;
        e.updatedAt = now;
        e.revision = 1;
        return e;
    }

    public void classify(String category, String severity, String priority, String classifiedBy, Instant now) {
        moveTo(ExceptionStatus.CLASSIFIED, now);
        this.category = category;
        this.severity = severity;
        this.priority = priority;
        this.classifiedBy = classifiedBy;
        this.classifiedAt = now;
    }

    public void startProgress(Instant now) {
        moveTo(ExceptionStatus.IN_PROGRESS, now);
    }

    public void markWaiting(Instant now) {
        moveTo(ExceptionStatus.WAITING, now);
    }

    public void resolve(String resolution, Instant now) {
        moveTo(ExceptionStatus.RESOLVED, now);
        this.resolution = resolution;
        this.resolvedAt = now;
    }

    /** Final closing of a RESOLVED exception. */
    public void closeResolved(Instant now) {
        moveTo(ExceptionStatus.CLOSED, now);
        this.closedAt = now;
    }

    /** Closing without Resolution: only from an unfinished status and only with a reason. */
    public void closeWithoutResolution(CloseReason reason, Instant now) {
        if (reason == null) {
            throw new IllegalArgumentException("closeReason is mandatory when closing without Resolution");
        }
        if (!status.isUnfinished()) {
            throw new IllegalTransitionException(status, ExceptionStatus.CLOSED);
        }
        this.status = ExceptionStatus.CLOSED;
        this.closeReason = reason;
        this.closedAt = now;
        touch(now);
    }

    private void moveTo(ExceptionStatus target, Instant now) {
        if (!status.canMoveTo(target)) {
            throw new IllegalTransitionException(status, target);
        }
        this.status = target;
        touch(now);
    }

    private void touch(Instant now) {
        this.updatedAt = now;
        this.revision++;
    }
}
