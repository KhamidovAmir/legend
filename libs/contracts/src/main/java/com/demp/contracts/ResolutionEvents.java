package com.demp.contracts;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Instant;
import java.util.UUID;

/** Events published by resolution-service to {@link Topics#RESOLUTION_EVENTS} (consumed by analytics). */
public final class ResolutionEvents {

    public static final String WORKFLOW_STARTED = "WorkflowStarted";
    public static final String ACTION_ATTEMPT_STARTED = "ActionAttemptStarted";
    public static final String RECONCILIATION_FINISHED = "ReconciliationFinished";
    public static final String VERIFICATION_COMPLETED = "VerificationCompleted";
    public static final String RESOLUTION_RECORDED = "ResolutionRecorded";
    public static final String SLA_BREACHED = "SlaBreached";
    public static final String WORK_ITEM_CREATED = "WorkItemCreated";

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record WorkflowStarted(UUID exceptionId, UUID workflowId, String policyCode, boolean automated) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ActionAttemptStarted(UUID exceptionId, UUID actionId, UUID attemptId, int attemptNo,
                                       String targetSystem, String actionType) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ReconciliationFinished(UUID exceptionId, UUID taskId, String targetSystem, String status,
                                         String result, int checks) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record VerificationCompleted(UUID exceptionId, String status, String expected, String actual) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ResolutionRecorded(UUID exceptionId, UUID resolutionId, String result, String resolvedBy) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SlaBreached(UUID exceptionId, Instant deadlineAt) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record WorkItemCreated(UUID exceptionId, UUID workItemId, String type) {
    }

    private ResolutionEvents() {
    }
}
