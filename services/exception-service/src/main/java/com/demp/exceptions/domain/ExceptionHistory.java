package com.demp.exceptions.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** Append-only audit trail. Not a source of truth: the current state lives in {@link ExceptionRecord}. */
@Entity
@Table(name = "exception_history")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ExceptionHistory {

    @Id
    private UUID id;
    private UUID exceptionId;
    private String event;
    private String fromStatus;
    private String toStatus;
    private String actor;
    private String details;
    private Instant createdAt;

    public ExceptionHistory(UUID exceptionId, String event, ExceptionStatus from, ExceptionStatus to, String actor,
                            String details, Instant now) {
        this.id = UUID.randomUUID();
        this.exceptionId = exceptionId;
        this.event = event;
        this.fromStatus = from == null ? null : from.name();
        this.toStatus = to == null ? null : to.name();
        this.actor = actor;
        this.details = details;
        this.createdAt = now;
    }
}
