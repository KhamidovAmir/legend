package com.demp.resolution.reconciliation;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Runs on every replica (no ShedLock): tasks are distributed by FOR UPDATE SKIP LOCKED.
 * Also exports "how old is the oldest UNKNOWN" - the metric that was missing during the double-delivery incident.
 */
@Component
public class ReconciliationWorker {

    private final ReconciliationService service;
    private final JdbcTemplate jdbc;
    private final int batchSize;
    private final MultiGauge oldestAge;
    private final MultiGauge active;

    public ReconciliationWorker(ReconciliationService service, JdbcTemplate jdbc, MeterRegistry meters,
                                @Value("${resolution.reconciliation.batch-size}") int batchSize) {
        this.service = service;
        this.jdbc = jdbc;
        this.batchSize = batchSize;
        this.oldestAge = MultiGauge.builder("reconciliation_oldest_unknown_age_seconds")
                .description("Age of the oldest active reconciliation task per external system").register(meters);
        this.active = MultiGauge.builder("reconciliation_active_tasks")
                .description("Active reconciliation tasks per external system").register(meters);
    }

    @Scheduled(fixedDelayString = "${resolution.reconciliation.poll-interval}")
    public void poll() {
        int claimed;
        do {
            claimed = service.processDue(batchSize);
        } while (claimed == batchSize);
    }

    @Scheduled(fixedDelayString = "PT15S")
    public void refreshGauges() {
        List<Object[]> rows = jdbc.query("""
                select target_system, count(*) as cnt, extract(epoch from now() - min(created_at)) as age
                  from reconciliation_task where status in ('READY', 'QUERYING') group by target_system
                """, (rs, i) -> new Object[]{rs.getString("target_system"), rs.getLong("cnt"), rs.getDouble("age")});
        oldestAge.register(rows.stream().map(r -> MultiGauge.Row.of(Tags.of("system", (String) r[0]), (Double) r[2])).toList(), true);
        active.register(rows.stream().map(r -> MultiGauge.Row.of(Tags.of("system", (String) r[0]), (Long) r[1])).toList(), true);
    }
}
