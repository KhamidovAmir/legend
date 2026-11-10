package com.demp.resolution.reconciliation;

import com.demp.contracts.IntegrationMessages.ExternalStateReceived;
import com.demp.contracts.IntegrationMessages.QueryExternalState;
import com.demp.contracts.ResolutionEvents;
import com.demp.resolution.domain.Action;
import com.demp.resolution.domain.ActionAttempt;
import com.demp.resolution.domain.ReconciliationPolicy;
import com.demp.resolution.domain.ReconciliationTask;
import com.demp.resolution.domain.ReconciliationTask.Result;
import com.demp.resolution.domain.Repositories;
import com.demp.resolution.domain.WorkflowInstance;
import com.demp.resolution.messaging.ResolutionMessages;
import com.demp.resolution.workflow.WorkflowOps;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Reconciliation of UNKNOWN results: one managed process for all external systems, tuned by ReconciliationPolicy. */
@Slf4j
@Service
@RequiredArgsConstructor
public class ReconciliationService {

    private final Repositories.ReconciliationTasks tasks;
    private final Repositories.ReconciliationPolicies policies;
    private final Repositories.Workflows workflows;
    private final Repositories.Actions actions;
    private final Repositories.Attempts attempts;
    private final WorkflowOps ops;
    private final ResolutionMessages messages;
    private final Clock clock;
    private final MeterRegistry meters;

    /** The command result is unknown: schedule the first check after settleDelay. */
    @Transactional
    public ReconciliationTask open(ActionAttempt attempt, Action action, String reason) {
        ReconciliationPolicy policy = policyFor(action.getTargetSystem(), action.getActionType());
        Instant now = clock.instant();
        ReconciliationTask task = tasks.save(ReconciliationTask.create(attempt, action, reason,
                now.plus(policy.settleDelay()), now.plus(policy.deadline()), now));
        log.info("Exception {}: reconciliation task {} opened for attempt {} ({}), first check at {}",
                action.getExceptionId(), task.getId(), attempt.getId(), reason, task.getNextCheckAt());
        return task;
    }

    /**
     * Worker step: claim due tasks and, in the SAME transaction, write QueryExternalState to the outbox.
     * @return number of claimed tasks
     */
    @Transactional
    public int processDue(int batchSize) {
        Instant now = clock.instant();
        List<ReconciliationTask> due = tasks.lockDue(now, batchSize);
        for (ReconciliationTask task : due) {
            WorkflowInstance wf = workflows.findByExceptionId(task.getExceptionId()).orElseThrow();
            if (task.isExpired(now)) {
                task.escalate(task.getResult(), now);
                finished(wf, task);
                ops.handToOperator(wf, "RECONCILIATION_EXPIRED",
                        "Result of %s is still unknown after %d checks (last: %s)"
                                .formatted(task.getActionType(), task.getChecksCount(), task.getResult()));
                continue;
            }
            ReconciliationPolicy policy = policyFor(task.getTargetSystem(), task.getActionType());
            UUID queryId = task.startQuery(now.plus(policy.queryTimeout()), now);
            messages.queryExternalState(wf, new QueryExternalState(queryId, task.getExceptionId(),
                    task.getTargetSystem(), task.getEntityType(), task.getEntityId(), "RECONCILIATION"));
        }
        return due.size();
    }

    /** Integration returned an observation for the CURRENT query of the task. Resolution interprets it. */
    @Transactional
    public void onObservation(ReconciliationTask task, ExternalStateReceived observation) {
        Instant now = clock.instant();
        WorkflowInstance wf = workflows.findByExceptionId(task.getExceptionId()).orElseThrow();
        Action action = actions.findById(task.getActionId()).orElseThrow();
        ActionAttempt attempt = attempts.findById(task.getAttemptId()).orElseThrow();
        ReconciliationPolicy policy = policyFor(task.getTargetSystem(), task.getActionType());

        Result result = ObservationInterpreter.interpret(observation.found(), observation.state(),
                action.getExpectedState(), policy.processingStateSet());
        meters.counter("reconciliation_checks_total", "system", task.getTargetSystem(), "result", result.name()).increment();
        log.info("Exception {}: reconciliation check #{} -> {} (state={})", task.getExceptionId(),
                task.getChecksCount(), result, observation.state());

        switch (result) {
            case APPLIED -> {
                task.resolve(Result.APPLIED, now);
                finished(wf, task);
                attempt.completed(observation.state(), now);
                // the observation that proved APPLIED is at the same time the verification of the resolution
                ops.verify(wf, action, observation.queryId(), observation.state());
            }
            case STILL_PROCESSING -> task.scheduleNext(Result.STILL_PROCESSING, now.plus(backoff(policy, task)), now);
            case NOT_APPLIED -> {
                if (task.getNotAppliedStreak() + 1 < policy.getNotAppliedConfirmations()) {
                    task.scheduleNext(Result.NOT_APPLIED, now.plus(backoff(policy, task)), now);
                    return;
                }
                // confirmed by several consecutive checks
                task.resolve(Result.NOT_APPLIED, now);
                finished(wf, task);
                attempt.notApplied(now);
                if (policy.isAutoRetryEnabled() && attempt.getAttemptNo() < action.getMaxAttempts()) {
                    ops.startAttempt(wf, action, attempt.getAttemptNo() + 1);
                } else {
                    action.status(Action.Status.FAILED, now);
                    ops.handToOperator(wf, "NOT_APPLIED_REVIEW", policy.isAutoRetryEnabled()
                            ? "Action was not applied and business attempts are exhausted"
                            : "Action was not applied; automatic retry is disabled for " + task.getTargetSystem());
                }
            }
            case INCONCLUSIVE, CONFLICT -> {
                // never repeat a side effect when the state cannot be determined
                task.escalate(result, now);
                finished(wf, task);
                ops.handToOperator(wf, "RECONCILIATION_REVIEW",
                        "External state cannot be determined (%s, state=%s)".formatted(result, observation.state()));
            }
        }
    }

    /** The check itself failed (system down / circuit breaker open): back off, do not count it as an observation. */
    @Transactional
    public void onQueryFailed(ReconciliationTask task, String reason) {
        Instant now = clock.instant();
        ReconciliationPolicy policy = policyFor(task.getTargetSystem(), task.getActionType());
        task.queryFailed(now.plus(backoff(policy, task)), now);
        meters.counter("reconciliation_checks_total", "system", task.getTargetSystem(), "result", "QUERY_FAILED").increment();
        log.warn("Exception {}: reconciliation query failed ({}), next check at {}", task.getExceptionId(), reason,
                task.getNextCheckAt());
    }

    /** A late result of the original command ends the reconciliation: we now know what happened. */
    @Transactional
    public void finishByLateResult(WorkflowInstance wf, ActionAttempt attempt) {
        tasks.findActiveByAttempt(attempt.getId()).ifPresent(task -> {
            task.resolve(Result.APPLIED, clock.instant());
            finished(wf, task);
        });
    }

    @Transactional
    public void cancelAll(UUID exceptionId) {
        tasks.findActiveByException(exceptionId).forEach(task -> task.cancel(clock.instant()));
    }

    private void finished(WorkflowInstance wf, ReconciliationTask task) {
        messages.event(wf, ResolutionEvents.RECONCILIATION_FINISHED, new ResolutionEvents.ReconciliationFinished(
                task.getExceptionId(), task.getId(), task.getTargetSystem(), task.getStatus().name(),
                task.getResult() == null ? null : task.getResult().name(), task.getChecksCount()));
        meters.counter("reconciliation_tasks_finished_total", "system", task.getTargetSystem(),
                "status", task.getStatus().name(), "result", String.valueOf(task.getResult())).increment();
        meters.timer("reconciliation_task_duration", "system", task.getTargetSystem())
                .record(Duration.between(task.getCreatedAt(), clock.instant()));
    }

    private Duration backoff(ReconciliationPolicy policy, ReconciliationTask task) {
        return Backoff.next(Duration.ofSeconds(policy.getBaseIntervalSeconds()),
                Duration.ofSeconds(policy.getMaxIntervalSeconds()), task.getChecksCount(),
                () -> ThreadLocalRandom.current().nextDouble());
    }

    private ReconciliationPolicy policyFor(String system, String actionType) {
        return policies.findByTargetSystemAndActionType(system, actionType)
                .orElseGet(() -> ReconciliationPolicy.conservativeDefault(system, actionType));
    }
}
