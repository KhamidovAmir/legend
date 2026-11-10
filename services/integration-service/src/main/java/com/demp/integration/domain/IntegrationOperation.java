package com.demp.integration.domain;

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

/** A logical external operation: a command (side effect) or a query (read-only). */
@Entity
@Table(name = "integration_operation")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class IntegrationOperation {

    public enum Kind { COMMAND, QUERY }

    public enum Status { RECEIVED, ACCEPTED, COMPLETED, FAILED, UNKNOWN }

    @Id
    private UUID id;
    @Enumerated(EnumType.STRING)
    private Kind kind;
    private UUID exceptionId;
    private String targetSystem;
    private String actionType;
    private String entityType;
    private String entityId;
    private String idempotencyKey;
    @Enumerated(EnumType.STRING)
    private Status status;
    private String externalRequestId;
    private String result;
    private String correlationId;
    private long revision;
    @Version
    private Long version;
    private Instant createdAt;
    private Instant updatedAt;

    public static IntegrationOperation received(UUID id, Kind kind, UUID exceptionId, String system, String actionType,
                                                String entityType, String entityId, String idempotencyKey,
                                                String correlationId, Instant now) {
        IntegrationOperation op = new IntegrationOperation();
        op.id = id;
        op.kind = kind;
        op.exceptionId = exceptionId;
        op.targetSystem = system;
        op.actionType = actionType;
        op.entityType = entityType;
        op.entityId = entityId;
        op.idempotencyKey = idempotencyKey;
        op.correlationId = correlationId;
        op.status = Status.RECEIVED;
        op.createdAt = now;
        op.updatedAt = now;
        return op;
    }

    public long nextRevision() {
        return ++revision;
    }

    public void accepted(String externalRequestId, Instant now) {
        this.status = Status.ACCEPTED;
        this.externalRequestId = externalRequestId;
        this.updatedAt = now;
    }

    public void completed(String result, Instant now) {
        this.status = Status.COMPLETED;
        this.result = result;
        this.updatedAt = now;
    }

    public void failed(String reason, Instant now) {
        this.status = Status.FAILED;
        this.result = reason;
        this.updatedAt = now;
    }

    public void unknown(String reason, Instant now) {
        this.status = Status.UNKNOWN;
        this.result = reason;
        this.updatedAt = now;
    }

    /** An async result may still arrive for an accepted or an unknown (timed out) command. */
    public boolean awaitsExternalResult() {
        return status == Status.ACCEPTED || status == Status.UNKNOWN;
    }
}
