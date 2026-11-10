package com.demp.resolution.domain;

import com.demp.resolution.domain.Records.Assignment;
import com.demp.resolution.domain.Records.Escalation;
import com.demp.resolution.domain.Records.Resolution;
import com.demp.resolution.domain.Records.ResolutionVerification;
import com.demp.resolution.domain.Records.SlaInstance;
import com.demp.resolution.domain.Records.WorkItem;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public final class Repositories {

    public interface ResolutionPolicies extends JpaRepository<ResolutionPolicy, UUID> {
        Optional<ResolutionPolicy> findFirstByExceptionTypeAndEnabledTrueOrderByVersionDesc(String exceptionType);
    }

    public interface ReconciliationPolicies extends JpaRepository<ReconciliationPolicy, UUID> {
        Optional<ReconciliationPolicy> findByTargetSystemAndActionType(String targetSystem, String actionType);
    }

    public interface Workflows extends JpaRepository<WorkflowInstance, UUID> {
        Optional<WorkflowInstance> findByExceptionId(UUID exceptionId);

        Optional<WorkflowInstance> findByPendingVerificationQueryId(UUID queryId);
    }

    public interface Actions extends JpaRepository<Action, UUID> {
        Optional<Action> findFirstByWorkflowIdOrderByCreatedAtDesc(UUID workflowId);

        List<Action> findByExceptionId(UUID exceptionId);
    }

    public interface Attempts extends JpaRepository<ActionAttempt, UUID> {
        Optional<ActionAttempt> findByOperationId(UUID operationId);

        Optional<ActionAttempt> findFirstByActionIdOrderByAttemptNoDesc(UUID actionId);

        List<ActionAttempt> findByExceptionIdOrderByCreatedAt(UUID exceptionId);
    }

    public interface ReconciliationTasks extends JpaRepository<ReconciliationTask, UUID> {

        /**
         * Claim due tasks. FOR UPDATE SKIP LOCKED: all replicas poll concurrently and never take the same task,
         * so no ShedLock / leader is needed. QUERYING tasks whose answer did not arrive in time are picked up as well.
         */
        @Query(value = """
                select * from reconciliation_task
                 where status in ('READY', 'QUERYING') and next_check_at <= :now
                 order by next_check_at
                 limit :limit
                   for update skip locked
                """, nativeQuery = true)
        List<ReconciliationTask> lockDue(Instant now, int limit);

        @Query("select t from ReconciliationTask t where t.currentQueryId = :queryId and t.status = 'QUERYING'")
        Optional<ReconciliationTask> findQuerying(UUID queryId);

        @Query("select t from ReconciliationTask t where t.attemptId = :attemptId and t.status in ('READY', 'QUERYING')")
        Optional<ReconciliationTask> findActiveByAttempt(UUID attemptId);

        @Query("select t from ReconciliationTask t where t.exceptionId = :exceptionId and t.status in ('READY', 'QUERYING')")
        List<ReconciliationTask> findActiveByException(UUID exceptionId);

        List<ReconciliationTask> findByExceptionIdOrderByCreatedAt(UUID exceptionId);
    }

    public interface Assignments extends JpaRepository<Assignment, UUID> {
        List<Assignment> findByExceptionIdAndActiveTrue(UUID exceptionId);
    }

    public interface Verifications extends JpaRepository<ResolutionVerification, UUID> {
        List<ResolutionVerification> findByExceptionId(UUID exceptionId);
    }

    public interface Resolutions extends JpaRepository<Resolution, UUID> {
        Optional<Resolution> findByExceptionId(UUID exceptionId);
    }

    public interface Slas extends JpaRepository<SlaInstance, UUID> {
        Optional<SlaInstance> findByExceptionId(UUID exceptionId);

        @Query(value = """
                select * from sla_instance
                 where status = 'ACTIVE' and deadline_at <= :now
                 order by deadline_at
                 limit :limit
                   for update skip locked
                """, nativeQuery = true)
        List<SlaInstance> lockBreached(Instant now, int limit);
    }

    public interface Escalations extends JpaRepository<Escalation, UUID> {
        List<Escalation> findByExceptionId(UUID exceptionId);
    }

    public interface WorkItems extends JpaRepository<WorkItem, UUID> {
        List<WorkItem> findByStatusOrderByCreatedAt(WorkItem.Status status);

        List<WorkItem> findByExceptionIdAndStatus(UUID exceptionId, WorkItem.Status status);
    }

    private Repositories() {
    }
}
