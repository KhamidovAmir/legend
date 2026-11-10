package com.demp.exceptions.registration;

import com.demp.contracts.ExternalEvent;
import com.demp.exceptions.domain.ClassificationPolicy;
import com.demp.exceptions.domain.ExceptionHistory;
import com.demp.exceptions.domain.ExceptionRecord;
import com.demp.exceptions.domain.ExceptionReference;
import com.demp.exceptions.domain.ExceptionStatus;
import com.demp.exceptions.domain.RegistrationRule;
import com.demp.exceptions.domain.Repositories;
import com.demp.exceptions.messaging.ExceptionEventPublisher;
import com.demp.exceptions.registration.RegistrationDecision.Outcome;
import com.demp.platform.messaging.MessageContext;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * InboundEvent -> technical dedup -> RegistrationRule -> business dedup -> Exception -> classification.
 * Everything, including outbox rows, happens in one transaction.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RegistrationService {

    static final String SYSTEM_ACTOR = "system";

    private final JdbcTemplate jdbc;
    private final Repositories.ExceptionRecords exceptions;
    private final Repositories.References references;
    private final Repositories.History history;
    private final Repositories.RegistrationRules rules;
    private final Repositories.ClassificationPolicies policies;
    private final ExceptionEventPublisher events;
    private final Clock clock;
    private final MeterRegistry meters;

    @Transactional
    public RegistrationDecision register(ExternalEvent event) {
        RegistrationDecision decision = doRegister(event);
        meters.counter("exceptions_registration_total",
                "source", event.sourceSystem(), "eventType", event.eventType(),
                "outcome", decision.outcome().name()).increment();
        return decision;
    }

    private RegistrationDecision doRegister(ExternalEvent event) {
        UUID inboundId = UUID.randomUUID();
        // 1. technical idempotency: UNIQUE(source_system, event_id)
        int inserted = jdbc.update("""
                insert into inbound_event (id, source_system, event_id, event_type, entity_type, entity_id, payload, decision)
                values (?, ?, ?, ?, ?, ?, ?::jsonb, 'RECEIVED')
                on conflict (source_system, event_id) do nothing
                """, inboundId, event.sourceSystem(), event.eventId(), event.eventType(), event.entityType(),
                event.entityId(), event.payload() == null ? "{}" : event.payload().toString());
        if (inserted == 0) {
            log.info("Duplicate inbound event {}:{} skipped", event.sourceSystem(), event.eventId());
            return new RegistrationDecision(Outcome.DUPLICATE_EVENT, null);
        }

        // 2. registration rule
        Optional<RegistrationRule> rule =
                rules.findBySourceSystemAndEventTypeAndEnabledTrue(event.sourceSystem(), event.eventType());
        if (rule.isEmpty() || rule.get().getDecision() == RegistrationRule.Decision.IGNORE) {
            return decide(inboundId, Outcome.IGNORED, null);
        }
        String exceptionType = rule.get().getExceptionType();
        Instant now = clock.instant();

        // 3. business deduplication / correlation
        String key = String.join(":", event.sourceSystem(), event.entityType(), event.entityId(), exceptionType);
        Optional<ExceptionRecord> active = exceptions.findActiveByKey(key);
        if (active.isPresent()) {
            ExceptionRecord existing = active.get();
            MessageContext.exceptionId(existing.getId());
            history.save(new ExceptionHistory(existing.getId(), "REPEATED_SIGNAL", existing.getStatus(),
                    existing.getStatus(), SYSTEM_ACTOR, "inboundEvent=" + event.eventId(), now));
            return decide(inboundId, Outcome.LINKED, existing.getId());
        }

        // 4. create. If a parallel transaction wins the race, the partial unique index rejects this insert,
        //    the transaction rolls back and the Kafka retry takes the LINKED branch above.
        ExceptionRecord exception = ExceptionRecord.open(event.sourceSystem(), key, exceptionType, event.region(),
                event.occurredAt(), now);
        exceptions.saveAndFlush(exception);
        MessageContext.exceptionId(exception.getId());
        ExceptionReference ref = references.save(new ExceptionReference(exception.getId(), event.sourceSystem(),
                event.entityType(), event.entityId()));
        history.save(new ExceptionHistory(exception.getId(), "CREATED", null, ExceptionStatus.OPEN, SYSTEM_ACTOR,
                "inboundEvent=" + event.eventId(), now));
        events.created(exception, ref);

        // 5. authoritative classification (separate from choosing a ResolutionPolicy)
        policies.findByExceptionType(exceptionType).ifPresentOrElse(
                policy -> classify(exception, ref, policy, now),
                () -> log.warn("No ClassificationPolicy for type {}: exception {} awaits manual classification",
                        exceptionType, exception.getId()));

        log.info("Exception {} registered: type={} status={}", exception.getId(), exceptionType, exception.getStatus());
        return decide(inboundId, Outcome.CREATED, exception.getId());
    }

    private void classify(ExceptionRecord exception, ExceptionReference ref, ClassificationPolicy policy, Instant now) {
        exception.classify(policy.getCategory(), policy.getSeverity(), policy.getPriority(), "AUTO", now);
        history.save(new ExceptionHistory(exception.getId(), "CLASSIFIED", ExceptionStatus.OPEN,
                ExceptionStatus.CLASSIFIED, SYSTEM_ACTOR,
                "severity=%s priority=%s".formatted(policy.getSeverity(), policy.getPriority()), now));
        events.classified(exception, ref);
    }

    private RegistrationDecision decide(UUID inboundId, Outcome outcome, UUID exceptionId) {
        jdbc.update("update inbound_event set decision = ?, exception_id = ? where id = ?",
                outcome.name(), exceptionId, inboundId);
        return new RegistrationDecision(outcome, exceptionId);
    }
}
