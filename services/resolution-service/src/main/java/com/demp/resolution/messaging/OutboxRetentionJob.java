package com.demp.resolution.messaging;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * The append-only outbox grows forever, so old rows are removed. Debezium's Event Router ignores DELETEs.
 * At higher volumes replace this with time partitioning + DROP PARTITION (no dead tuples at all).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxRetentionJob {

    private final JdbcTemplate jdbc;
    private final Clock clock;
    @Value("${resolution.outbox.retention}")
    private Duration retention;

    @Scheduled(fixedDelayString = "PT1H", initialDelayString = "PT10M")
    public void purge() {
        int deleted = jdbc.update("delete from outbox where created_at < ?", Timestamp.from(clock.instant().minus(retention)));
        if (deleted > 0) {
            log.info("Outbox retention: {} rows older than {} removed", deleted, retention);
        }
    }
}
