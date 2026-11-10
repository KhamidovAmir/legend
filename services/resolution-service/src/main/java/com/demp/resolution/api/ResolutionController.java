package com.demp.resolution.api;

import com.demp.resolution.domain.ActionAttempt;
import com.demp.resolution.domain.ReconciliationTask;
import com.demp.resolution.domain.Records.WorkItem;
import com.demp.resolution.domain.Repositories;
import com.demp.resolution.domain.WorkflowInstance;
import com.demp.resolution.workflow.ResolutionWorkflowService;
import com.demp.resolution.workflow.ResolutionWorkflowService.WorkItemOutcome;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Operator API: manual tasks and the resolution state of an exception. */
@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class ResolutionController {

    private final ResolutionWorkflowService workflow;
    private final Repositories.WorkItems workItems;
    private final Repositories.Workflows workflows;
    private final Repositories.Attempts attempts;
    private final Repositories.ReconciliationTasks tasks;

    @GetMapping("/work-items")
    public List<WorkItemView> openWorkItems() {
        return workItems.findByStatusOrderByCreatedAt(WorkItem.Status.OPEN).stream().map(WorkItemView::of).toList();
    }

    @PostMapping("/work-items/{id}/complete")
    public WorkItemView complete(@PathVariable UUID id, @Valid @RequestBody CompleteRequest request) {
        return WorkItemView.of(workflow.completeWorkItem(id, request.outcome(), request.operator(),
                request.resolution(), request.closeReason()));
    }

    /** Everything an operator needs to see why an exception is where it is (including the history of checks). */
    @GetMapping("/exceptions/{exceptionId}/resolution")
    public ResolutionStateView state(@PathVariable UUID exceptionId) {
        WorkflowInstance wf = workflows.findByExceptionId(exceptionId)
                .orElseThrow(() -> new NoSuchElementException("No workflow for exception " + exceptionId));
        return new ResolutionStateView(wf.getId(), wf.getStatus().name(), wf.getPolicyCode(), wf.getPolicyVersion(),
                attempts.findByExceptionIdOrderByCreatedAt(exceptionId).stream().map(AttemptView::of).toList(),
                tasks.findByExceptionIdOrderByCreatedAt(exceptionId).stream().map(TaskView::of).toList());
    }

    @ExceptionHandler(NoSuchElementException.class)
    ProblemDetail notFound(NoSuchElementException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler({IllegalStateException.class, IllegalArgumentException.class})
    ProblemDetail conflict(RuntimeException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
    }

    public record CompleteRequest(@NotNull WorkItemOutcome outcome, @NotBlank String operator, String resolution,
                                  String closeReason) {
    }

    public record WorkItemView(UUID id, UUID exceptionId, String type, String status, String description, Instant createdAt) {
        static WorkItemView of(WorkItem w) {
            return new WorkItemView(w.getId(), w.getExceptionId(), w.getType(), w.getStatus().name(), w.getDescription(), w.getCreatedAt());
        }
    }

    public record AttemptView(UUID id, int attemptNo, String status, UUID operationId, String outcome, String failureReason) {
        static AttemptView of(ActionAttempt a) {
            return new AttemptView(a.getId(), a.getAttemptNo(), a.getStatus().name(), a.getOperationId(), a.getOutcomeResult(), a.getFailureReason());
        }
    }

    public record TaskView(UUID id, String status, String result, String reason, int checks, Instant nextCheckAt, Instant deadlineAt) {
        static TaskView of(ReconciliationTask t) {
            return new TaskView(t.getId(), t.getStatus().name(), t.getResult() == null ? null : t.getResult().name(),
                    t.getReason(), t.getChecksCount(), t.getNextCheckAt(), t.getDeadlineAt());
        }
    }

    public record ResolutionStateView(UUID workflowId, String status, String policyCode, Integer policyVersion,
                                      List<AttemptView> attempts, List<TaskView> reconciliationTasks) {
    }
}
