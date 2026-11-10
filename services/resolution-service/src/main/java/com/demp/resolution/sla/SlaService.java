package com.demp.resolution.sla;

import com.demp.contracts.ResolutionEvents;
import com.demp.resolution.domain.Records.Escalation;
import com.demp.resolution.domain.Records.SlaInstance;
import com.demp.resolution.domain.Repositories;
import com.demp.resolution.domain.WorkflowInstance;
import com.demp.resolution.messaging.ResolutionMessages;
import com.demp.resolution.workflow.WorkflowOps;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

/** Business deadline control. A breach creates an Escalation fact; the exception status is not changed. */
@Slf4j
@Service
@RequiredArgsConstructor
public class SlaService {

    private final Repositories.Slas slas;
    private final Repositories.Escalations escalations;
    private final Repositories.Workflows workflows;
    private final WorkflowOps ops;
    private final ResolutionMessages messages;
    private final TransactionTemplate tx;
    private final Clock clock;
    private final MeterRegistry meters;

    @Scheduled(fixedDelayString = "${resolution.sla.poll-interval}")
    public void poll() {
        Integer breached;
        do {
            breached = tx.execute(status -> escalateBreached(100));
        } while (breached != null && breached == 100);
    }

    @Transactional
    public int escalateBreached(int batchSize) {
        Instant now = clock.instant();
        List<SlaInstance> breached = slas.lockBreached(now, batchSize);
        for (SlaInstance sla : breached) {
            sla.breach(now);
            escalations.save(new Escalation(sla.getExceptionId(), "SLA_BREACHED", now));
            WorkflowInstance wf = workflows.findByExceptionId(sla.getExceptionId()).orElseThrow();
            messages.event(wf, ResolutionEvents.SLA_BREACHED,
                    new ResolutionEvents.SlaBreached(sla.getExceptionId(), sla.getDeadlineAt()));
            if (!wf.getStatus().isFinished()) {
                ops.assign(sla.getExceptionId(), "TEAM", "senior-operators", now);
                ops.createWorkItem(wf, "SLA_ESCALATION", "SLA deadline %s passed".formatted(sla.getDeadlineAt()));
            }
            meters.counter("resolution_sla_breaches_total").increment();
            log.warn("Exception {}: SLA breached (deadline {})", sla.getExceptionId(), sla.getDeadlineAt());
        }
        return breached.size();
    }
}
