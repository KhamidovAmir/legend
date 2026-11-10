package com.demp.contracts;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Instant;
import java.util.UUID;

/** Events published by exception-service to {@link Topics#EXCEPTION_EVENTS}. */
public final class ExceptionEvents {

    public static final String CREATED = "ExceptionCreated";
    public static final String CLASSIFIED = "ExceptionClassified";
    public static final String STATUS_CHANGED = "ExceptionStatusChanged";
    public static final String CLOSED = "ExceptionClosed";
    public static final String COMMAND_REJECTED = "ExceptionCommandRejected";

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ExceptionCreated(UUID exceptionId, String sourceSystem, String entityType, String entityId,
                                   String region, Instant detectedAt) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ExceptionClassified(UUID exceptionId, String exceptionType, String category, String severity,
                                      String priority, String region, String sourceSystem, String entityType,
                                      String entityId, String classifiedBy) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ExceptionStatusChanged(UUID exceptionId, String fromStatus, String toStatus, String actor) {
    }

    /** closeReason is null when the exception was closed after a confirmed Resolution. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ExceptionClosed(UUID exceptionId, String closeReason, boolean resolved, String actor) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ExceptionCommandRejected(UUID exceptionId, String commandType, String currentStatus, String reason) {
    }

    private ExceptionEvents() {
    }
}
