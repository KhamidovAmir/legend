package com.demp.resolution.domain;

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

/** A business operation the workflow requires (e.g. RETRY_DELIVERY). */
@Entity
@Table(name = "action")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Action {

    public enum Status { CREATED, WAITING_EXTERNAL_RESULT, UNKNOWN, COMPLETED, FAILED }

    @Id
    private UUID id;
    private UUID workflowId;
    private UUID exceptionId;
    private String actionType;
    private String targetSystem;
    private String executionMode;
    @Enumerated(EnumType.STRING)
    private Status status;
    private String entityType;
    private String entityId;
    private String expectedState;
    private int maxAttempts;
    @Version
    private Long version;
    private Instant createdAt;
    private Instant updatedAt;

    public static Action create(WorkflowInstance workflow, ResolutionPolicy policy, Instant now) {
        Action a = new Action();
        a.id = UUID.randomUUID();
        a.workflowId = workflow.getId();
        a.exceptionId = workflow.getExceptionId();
        a.actionType = policy.getActionType();
        a.targetSystem = policy.getTargetSystem();
        a.executionMode = policy.getExecutionMode().name();
        a.status = Status.CREATED;
        a.entityType = workflow.getEntityType();
        a.entityId = workflow.getEntityId();
        a.expectedState = policy.getExpectedState();
        a.maxAttempts = policy.getMaxBusinessAttempts();
        a.createdAt = now;
        a.updatedAt = now;
        return a;
    }

    public void status(Status status, Instant now) {
        this.status = status;
        this.updatedAt = now;
    }
}
