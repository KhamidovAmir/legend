package com.demp.integration.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** One transport attempt (HTTP call) of an operation. */
@Entity
@Table(name = "integration_attempt")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class IntegrationAttempt {

    @Id
    private UUID id;
    private UUID operationId;
    private int attemptNo;
    private String outcome;
    private Integer httpStatus;
    private String error;
    private long durationMs;
    private Instant startedAt;

    public IntegrationAttempt(UUID operationId, int attemptNo, String outcome, Integer httpStatus, String error,
                              long durationMs, Instant startedAt) {
        this.id = UUID.randomUUID();
        this.operationId = operationId;
        this.attemptNo = attemptNo;
        this.outcome = outcome;
        this.httpStatus = httpStatus;
        this.error = error;
        this.durationMs = durationMs;
        this.startedAt = startedAt;
    }
}
