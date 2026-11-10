package com.demp.exceptions.domain;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public final class Repositories {

    public interface ExceptionRecords extends JpaRepository<ExceptionRecord, UUID> {

        @Query("select e from ExceptionRecord e where e.businessCorrelationKey = :key and e.status <> 'CLOSED'")
        Optional<ExceptionRecord> findActiveByKey(String key);

        Page<ExceptionRecord> findByStatus(ExceptionStatus status, Pageable pageable);

        /** SKIP LOCKED: every replica may run the closing job, rows taken by another replica are skipped. */
        @Query(value = """
                select * from exception_record
                 where status = 'RESOLVED' and resolved_at <= :resolvedBefore
                 order by resolved_at
                 limit :limit
                   for update skip locked
                """, nativeQuery = true)
        List<ExceptionRecord> lockResolvedBefore(Instant resolvedBefore, int limit);
    }

    public interface References extends JpaRepository<ExceptionReference, UUID> {
        List<ExceptionReference> findByExceptionId(UUID exceptionId);
    }

    public interface Relations extends JpaRepository<ExceptionRelation, UUID> {
        @Query("select r from ExceptionRelation r where r.fromExceptionId = :id or r.toExceptionId = :id")
        List<ExceptionRelation> findAllTouching(UUID id);
    }

    public interface History extends JpaRepository<ExceptionHistory, UUID> {
        List<ExceptionHistory> findByExceptionIdOrderByCreatedAtAscIdAsc(UUID exceptionId);
    }

    public interface RegistrationRules extends JpaRepository<RegistrationRule, UUID> {
        Optional<RegistrationRule> findBySourceSystemAndEventTypeAndEnabledTrue(String sourceSystem, String eventType);
    }

    public interface ClassificationPolicies extends JpaRepository<ClassificationPolicy, UUID> {
        Optional<ClassificationPolicy> findByExceptionType(String exceptionType);
    }

    private Repositories() {
    }
}
