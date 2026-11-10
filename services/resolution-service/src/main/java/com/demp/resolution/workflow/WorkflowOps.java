package com.demp.resolution.workflow;

import com.demp.contracts.ExceptionCommands;
import com.demp.contracts.ExceptionCommands.ExceptionCommand;
import com.demp.contracts.IntegrationMessages.ExecuteAction;
import com.demp.contracts.IntegrationMessages.QueryExternalState;
import com.demp.contracts.ResolutionEvents;
import com.demp.resolution.domain.Action;
import com.demp.resolution.domain.ActionAttempt;
import com.demp.resolution.domain.Records.Assignment;
import com.demp.resolution.domain.Records.Resolution;
import com.demp.resolution.domain.Records.ResolutionVerification;
import com.demp.resolution.domain.Records.SlaInstance;
import com.demp.resolution.domain.Records.WorkItem;
import com.demp.resolution.domain.Repositories;
import com.demp.resolution.domain.WorkflowInstance;
import com.demp.resolution.messaging.ResolutionMessages;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Building blocks of the workflow shared by event handlers and the reconciliation worker.
 * Always called inside the caller's transaction (state change + outbox row are atomic).
 */
@Slf4j
@Component
@RequiredArgsConstructor
@Transactional(propagation = Propagation.MANDATORY)
public class WorkflowOps {

    public static final String BOT_ACTOR = "resolution-service";

    private final Repositories.Attempts attempts;
    private final Repositories.Assignments assignments;
    private final Repositories.Verifications verifications;
    private final Repositories.Resolutions resolutions;
    private final Repositories.Slas slas;
    private final Repositories.WorkItems workItems;
    private final ResolutionMessages messages;
    private final Clock clock;
    private final MeterRegistry meters;

    /** A new BUSINESS attempt: new idempotency key, new integration operation. */
    public ActionAttempt startAttempt(WorkflowInstance wf, Action action, int attemptNo) {
        Instant now = clock.instant();
        ActionAttempt attempt = attempts.save(ActionAttempt.start(action, attemptNo, now));
        action.status(Action.Status.CREATED, now);
        if (wf.getStatus() != WorkflowInstance.Status.ACTION_IN_PROGRESS) {
            wf.actionInProgress(now);
        }
        messages.executeAction(wf, new ExecuteAction(attempt.getOperationId(), wf.getExceptionId(), action.getId(),
                attempt.getId(), action.getTargetSystem(), action.getActionType(), action.getEntityType(),
                action.getEntityId(), attempt.getIdempotencyKey(), Map.of()));
        messages.event(wf, ResolutionEvents.ACTION_ATTEMPT_STARTED, new ResolutionEvents.ActionAttemptStarted(
                wf.getExceptionId(), action.getId(), attempt.getId(), attemptNo, action.getTargetSystem(),
                action.getActionType()));
        meters.counter("resolution_action_attempts_total", "system", action.getTargetSystem(),
                "action", action.getActionType(), "retry", String.valueOf(attemptNo > 1)).increment();
        log.info("Exception {}: attempt #{} of {} started, operation {}", wf.getExceptionId(), attemptNo,
                action.getActionType(), attempt.getOperationId());
        return attempt;
    }

    /** "The external system said OK" is not enough: ask for the actual state before resolving. */
    public void startVerification(WorkflowInstance wf, Action action) {
        UUID queryId = UUID.randomUUID();
        wf.verifying(queryId, clock.instant());
        messages.queryExternalState(wf, new QueryExternalState(queryId, wf.getExceptionId(), action.getTargetSystem(),
                action.getEntityType(), action.getEntityId(), "VERIFICATION"));
    }

    /** @return true if the observed state proves the problem is gone (workflow completed) */
    public boolean verify(WorkflowInstance wf, Action action, UUID queryId, String actualState) {
        Instant now = clock.instant();
        ResolutionVerification verification = verifications.save(new ResolutionVerification(wf.getExceptionId(),
                action.getId(), queryId, action.getExpectedState(), actualState, now));
        messages.event(wf, ResolutionEvents.VERIFICATION_COMPLETED, new ResolutionEvents.VerificationCompleted(
                wf.getExceptionId(), verification.getStatus(), action.getExpectedState(), actualState));
        meters.counter("resolution_verifications_total", "status", verification.getStatus()).increment();
        if (!verification.passed()) {
            action.status(Action.Status.FAILED, now);
            handToOperator(wf, "VERIFICATION_FAILED", "Expected %s but external state is %s"
                    .formatted(action.getExpectedState(), actualState));
            return false;
        }
        action.status(Action.Status.COMPLETED, now);
        resolve(wf, wf.getResolutionResult(), "VERIFIED", activeAssignee(wf.getExceptionId()));
        return true;
    }

    /** Resolution is recorded and Exception Service is asked to move the exception to RESOLVED. */
    public void resolve(WorkflowInstance wf, String result, String verificationStatus, String resolvedBy) {
        Instant now = clock.instant();
        Resolution resolution = resolutions.save(new Resolution(wf.getExceptionId(), result, verificationStatus,
                resolvedBy, now));
        wf.complete(now);
        slas.findByExceptionId(wf.getExceptionId()).ifPresent(sla -> sla.finish(SlaInstance.Status.MET));
        messages.exceptionCommand(wf, ExceptionCommands.RESOLVE,
                new ExceptionCommand(wf.getExceptionId(), resolvedBy, null, result));
        messages.event(wf, ResolutionEvents.RESOLUTION_RECORDED, new ResolutionEvents.ResolutionRecorded(
                wf.getExceptionId(), resolution.getId(), result, resolvedBy));
        meters.counter("resolution_resolutions_total", "verification", verificationStatus).increment();
        log.info("Exception {} resolved: {} ({}) by {}", wf.getExceptionId(), result, verificationStatus, resolvedBy);
    }

    /** Automation gives up: reassign to people and create a manual task with the context. */
    public WorkItem handToOperator(WorkflowInstance wf, String type, String description) {
        Instant now = clock.instant();
        if (wf.getStatus() != WorkflowInstance.Status.MANUAL) {
            wf.manual(now);
        }
        assign(wf.getExceptionId(), "TEAM", "operators", now);
        return createWorkItem(wf, type, description);
    }

    public WorkItem createWorkItem(WorkflowInstance wf, String type, String description) {
        WorkItem item = workItems.save(new WorkItem(wf.getExceptionId(), type, description, clock.instant()));
        messages.event(wf, ResolutionEvents.WORK_ITEM_CREATED,
                new ResolutionEvents.WorkItemCreated(wf.getExceptionId(), item.getId(), type));
        meters.counter("resolution_work_items_created_total", "type", type).increment();
        log.info("Exception {}: work item {} created ({})", wf.getExceptionId(), type, description);
        return item;
    }

    public void assign(UUID exceptionId, String assigneeType, String assigneeId, Instant now) {
        assignments.findByExceptionIdAndActiveTrue(exceptionId).forEach(Assignment::deactivate);
        assignments.save(new Assignment(exceptionId, assigneeType, assigneeId, now));
    }

    private String activeAssignee(UUID exceptionId) {
        return assignments.findByExceptionIdAndActiveTrue(exceptionId).stream().findFirst()
                .map(Assignment::getAssigneeId).orElse(BOT_ACTOR);
    }
}
