package com.demp.resolution.workflow;

import com.demp.contracts.ExceptionCommands;
import com.demp.contracts.ExceptionCommands.ExceptionCommand;
import com.demp.contracts.ExceptionEvents.ExceptionClassified;
import com.demp.contracts.IntegrationMessages.ExternalStateQueryFailed;
import com.demp.contracts.IntegrationMessages.ExternalStateReceived;
import com.demp.contracts.IntegrationMessages.IntegrationAccepted;
import com.demp.contracts.IntegrationMessages.IntegrationCompleted;
import com.demp.contracts.IntegrationMessages.IntegrationFailed;
import com.demp.contracts.IntegrationMessages.IntegrationUnknown;
import com.demp.contracts.ResolutionEvents;
import com.demp.platform.messaging.MessageContext;
import com.demp.resolution.domain.Action;
import com.demp.resolution.domain.ActionAttempt;
import com.demp.resolution.domain.ReconciliationTask;
import com.demp.resolution.domain.Records.SlaInstance;
import com.demp.resolution.domain.Records.WorkItem;
import com.demp.resolution.domain.Repositories;
import com.demp.resolution.domain.ResolutionPolicy;
import com.demp.resolution.domain.ResolutionPolicy.ExecutionMode;
import com.demp.resolution.domain.WorkflowInstance;
import com.demp.resolution.messaging.ResolutionMessages;
import com.demp.resolution.reconciliation.ReconciliationService;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Reactions of the resolution workflow to events of Exception Service and Integration Service. */
@Slf4j
@Service
@RequiredArgsConstructor
public class ResolutionWorkflowService {

    private final Repositories.ResolutionPolicies policies;
    private final Repositories.Workflows workflows;
    private final Repositories.Actions actions;
    private final Repositories.Attempts attempts;
    private final Repositories.ReconciliationTasks tasks;
    private final Repositories.Slas slas;
    private final Repositories.WorkItems workItems;
    private final WorkflowOps ops;
    private final ReconciliationService reconciliation;
    private final ResolutionMessages messages;
    private final Clock clock;
    private final MeterRegistry meters;
    @Value("${resolution.verification.max-query-failures}")
    private int maxVerificationQueryFailures;

    // ---------------------------------------------------------------- exception.events

    /** Resolution starts only after classification: the policy is chosen by its result. */
    @Transactional
    public void onExceptionClassified(ExceptionClassified event) {
        MessageContext.exceptionId(event.exceptionId());
        if (workflows.findByExceptionId(event.exceptionId()).isPresent()) {
            return; // inbox already guards against redelivery; UNIQUE(exception_id) is the last line of defence
        }
        Instant now = clock.instant();
        Optional<ResolutionPolicy> policy =
                policies.findFirstByExceptionTypeAndEnabledTrueOrderByVersionDesc(event.exceptionType());
        boolean automated = policy.map(p -> p.getExecutionMode() == ExecutionMode.EXTERNAL_AUTOMATED).orElse(false);

        WorkflowInstance wf = workflows.save(WorkflowInstance.start(event.exceptionId(), policy.orElse(null),
                event.entityType(), event.entityId(), MessageContext.correlationId(), now));
        int slaMinutes = policy.map(ResolutionPolicy::getSlaMinutes).orElse(480);
        slas.save(new SlaInstance(event.exceptionId(), now.plus(Duration.ofMinutes(slaMinutes)), now));

        messages.exceptionCommand(wf, ExceptionCommands.START_PROGRESS,
                ExceptionCommand.of(event.exceptionId(), WorkflowOps.BOT_ACTOR));
        messages.event(wf, ResolutionEvents.WORKFLOW_STARTED, new ResolutionEvents.WorkflowStarted(
                event.exceptionId(), wf.getId(), wf.getPolicyCode(), automated));
        meters.counter("resolution_workflows_started_total", "automated", String.valueOf(automated)).increment();

        if (!automated) {
            ops.assign(event.exceptionId(), "TEAM", "operators", now);
            ops.createWorkItem(wf, "MANUAL_RESOLUTION", policy.isPresent()
                    ? "Policy %s requires manual handling".formatted(wf.getPolicyCode())
                    : "No resolution policy for type " + event.exceptionType());
            return;
        }
        ops.assign(event.exceptionId(), "BOT", policy.get().getBotId(), now);
        Action action = actions.save(Action.create(wf, policy.get(), now));
        ops.startAttempt(wf, action, 1);
    }

    /** The exception was closed outside of the workflow (operator closed it in Exception Service). */
    @Transactional
    public void onExceptionClosed(UUID exceptionId) {
        workflows.findByExceptionId(exceptionId).filter(wf -> !wf.getStatus().isFinished()).ifPresent(wf -> {
            Instant now = clock.instant();
            wf.cancel(now);
            reconciliation.cancelAll(exceptionId);
            slas.findByExceptionId(exceptionId).ifPresent(sla -> sla.finish(SlaInstance.Status.CANCELLED));
            workItems.findByExceptionIdAndStatus(exceptionId, WorkItem.Status.OPEN)
                    .forEach(item -> item.close(WorkItem.Status.CANCELLED, "system", now));
            log.info("Exception {} closed externally: workflow cancelled", exceptionId);
        });
    }

    // ---------------------------------------------------------------- integration.events

    @Transactional
    public void onAccepted(IntegrationAccepted event) {
        attempt(event.operationId()).filter(a -> a.getStatus() == ActionAttempt.Status.SENT).ifPresent(attempt -> {
            attempt.accepted();
            // accepted != done: without a confirmed result the action is not completed
            action(attempt).status(Action.Status.WAITING_EXTERNAL_RESULT, clock.instant());
        });
    }

    @Transactional
    public void onCompleted(IntegrationCompleted event) {
        Optional<ActionAttempt> found = attempt(event.operationId());
        if (found.isEmpty() || found.get().getStatus() == ActionAttempt.Status.COMPLETED) {
            return;
        }
        ActionAttempt attempt = found.get();
        Action action = action(attempt);
        WorkflowInstance wf = workflows.findById(action.getWorkflowId()).orElseThrow();
        Instant now = clock.instant();
        boolean wasDeclaredNotApplied = attempt.getStatus() == ActionAttempt.Status.NOT_APPLIED;
        attempt.completed(event.result(), now);
        reconciliation.finishByLateResult(wf, attempt);

        ActionAttempt latest = attempts.findFirstByActionIdOrderByAttemptNoDesc(action.getId()).orElseThrow();
        if (wasDeclaredNotApplied || !latest.getId().equals(attempt.getId())) {
            // We decided "not applied" (and possibly repeated the action), but the first command did succeed:
            // potential double execution. Never resolved automatically.
            meters.counter("reconciliation_false_not_applied_total", "system", action.getTargetSystem()).increment();
            log.error("Exception {}: late result for attempt #{} after it was declared NOT_APPLIED - possible double execution",
                    wf.getExceptionId(), attempt.getAttemptNo());
            if (!wf.getStatus().isFinished()) {
                ops.createWorkItem(wf, "CONFLICT_REVIEW", "Late success of attempt #%d of %s: check for double execution"
                        .formatted(attempt.getAttemptNo(), action.getActionType()));
            }
            return;
        }
        if (wf.getStatus() == WorkflowInstance.Status.ACTION_IN_PROGRESS) {
            ops.startVerification(wf, action);
        }
    }

    /** FAILED means "definitely not applied", so a new business attempt is safe. */
    @Transactional
    public void onFailed(IntegrationFailed event) {
        attempt(event.operationId()).filter(a -> a.getStatus().awaitsResult()).ifPresent(attempt -> {
            Action action = action(attempt);
            WorkflowInstance wf = workflows.findById(action.getWorkflowId()).orElseThrow();
            Instant now = clock.instant();
            attempt.failed(event.reason(), now);
            if (event.retryable() && attempt.getAttemptNo() < action.getMaxAttempts()) {
                ops.startAttempt(wf, action, attempt.getAttemptNo() + 1);
            } else {
                action.status(Action.Status.FAILED, now);
                meters.counter("resolution_business_retries_exhausted_total", "system", action.getTargetSystem()).increment();
                ops.handToOperator(wf, "ACTION_FAILED", "%s failed: %s".formatted(action.getActionType(), event.reason()));
            }
        });
    }

    /** No blind retry: the command may have been executed. Find out the real state first. */
    @Transactional
    public void onUnknown(IntegrationUnknown event) {
        attempt(event.operationId()).filter(a -> a.getStatus().awaitsResult()).ifPresent(attempt -> {
            Action action = action(attempt);
            attempt.unknown();
            action.status(Action.Status.UNKNOWN, clock.instant());
            reconciliation.open(attempt, action, event.reason());
            meters.counter("resolution_actions_unknown_total", "system", action.getTargetSystem()).increment();
        });
    }

    @Transactional
    public void onStateReceived(ExternalStateReceived event) {
        Optional<ReconciliationTask> task = tasks.findQuerying(event.queryId());
        if (task.isPresent()) {
            reconciliation.onObservation(task.get(), event);
            return;
        }
        Optional<WorkflowInstance> verifying = workflows.findByPendingVerificationQueryId(event.queryId());
        if (verifying.isPresent()) {
            WorkflowInstance wf = verifying.get();
            Action action = actions.findFirstByWorkflowIdOrderByCreatedAtDesc(wf.getId()).orElseThrow();
            ops.verify(wf, action, event.queryId(), event.found() ? event.state() : null);
            return;
        }
        // answer to a query that is no longer current (timeout re-query, late result, cancellation)
        meters.counter("resolution_stale_observations_total").increment();
        log.info("Stale observation for query {} ignored", event.queryId());
    }

    @Transactional
    public void onStateQueryFailed(ExternalStateQueryFailed event) {
        Optional<ReconciliationTask> task = tasks.findQuerying(event.queryId());
        if (task.isPresent()) {
            reconciliation.onQueryFailed(task.get(), event.reason());
            return;
        }
        workflows.findByPendingVerificationQueryId(event.queryId()).ifPresent(wf -> {
            Action action = actions.findFirstByWorkflowIdOrderByCreatedAtDesc(wf.getId()).orElseThrow();
            if (wf.verificationQueryFailed() >= maxVerificationQueryFailures) {
                ops.handToOperator(wf, "VERIFICATION_UNAVAILABLE", "Cannot verify external state: " + event.reason());
            } else {
                ops.startVerification(wf, action);
            }
        });
    }

    // ---------------------------------------------------------------- operator

    public enum WorkItemOutcome { RESOLVED, CLOSED }

    @Transactional
    public WorkItem completeWorkItem(UUID workItemId, WorkItemOutcome outcome, String operator, String resolution,
                                     String closeReason) {
        WorkItem item = workItems.findById(workItemId)
                .orElseThrow(() -> new NoSuchElementException("WorkItem %s not found".formatted(workItemId)));
        WorkflowInstance wf = workflows.findByExceptionId(item.getExceptionId()).orElseThrow();
        Instant now = clock.instant();
        item.close(WorkItem.Status.DONE, operator, now);
        if (wf.getStatus().isFinished()) {
            return item; // e.g. a CONFLICT_REVIEW item acknowledged after the exception was already resolved
        }
        reconciliation.cancelAll(wf.getExceptionId());
        if (outcome == WorkItemOutcome.RESOLVED) {
            ops.resolve(wf, resolution == null ? "RESOLVED_MANUALLY" : resolution, "MANUAL", operator);
        } else {
            if (closeReason == null || closeReason.isBlank()) {
                throw new IllegalArgumentException("closeReason is mandatory when closing without resolution");
            }
            wf.cancel(now);
            slas.findByExceptionId(wf.getExceptionId()).ifPresent(sla -> sla.finish(SlaInstance.Status.CANCELLED));
            messages.exceptionCommand(wf, ExceptionCommands.CLOSE,
                    new ExceptionCommand(wf.getExceptionId(), operator, closeReason, null));
        }
        workItems.findByExceptionIdAndStatus(wf.getExceptionId(), WorkItem.Status.OPEN)
                .forEach(other -> other.close(WorkItem.Status.CANCELLED, operator, now));
        return item;
    }

    private Optional<ActionAttempt> attempt(UUID operationId) {
        Optional<ActionAttempt> attempt = attempts.findByOperationId(operationId);
        attempt.ifPresentOrElse(a -> MessageContext.exceptionId(a.getExceptionId()),
                () -> log.warn("Integration event for unknown operation {} ignored", operationId));
        return attempt;
    }

    private Action action(ActionAttempt attempt) {
        return actions.findById(attempt.getActionId()).orElseThrow();
    }
}
