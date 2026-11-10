package com.demp.analytics;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Ready-to-use aggregates for reports. Period = by the moment the problem was detected. */
@RestController
@RequestMapping("/api/v1/analytics")
@RequiredArgsConstructor
public class AnalyticsController {

    private static final String AGGREGATES = """
                   count(*)                                                                    as exception_count,
                   count(*) filter (where closed_at is null)                                   as backlog,
                   avg(extract(epoch from resolved_at - detected_at))                          as avg_resolution_seconds,
                   avg(extract(epoch from first_action_at - detected_at))                      as avg_response_seconds,
                   count(*) filter (where sla_breached)                                        as sla_breaches,
                   coalesce(sum(greatest(business_attempts - 1, 0)), 0)                        as business_retries,
                   coalesce(sum(unknown_count), 0)                                             as unknown_results,
                   coalesce(avg(case when automated and work_items = 0 and resolved_at is not null then 1.0 else 0.0 end), 0) as automation_rate,
                   coalesce(avg(case when work_items > 0 then 1.0 else 0.0 end), 0)            as manual_handling_rate,
                   coalesce(avg(case when verification_failed then 1.0 else 0.0 end), 0)       as verification_failure_rate
            """;

    private final JdbcClient jdbc;

    @GetMapping("/summary")
    public Map<String, Object> summary(@RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
                                       @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
                                       @RequestParam(required = false) String region) {
        return jdbc.sql("select " + AGGREGATES + """
                          from exception_fact
                         where detected_at >= :from and detected_at < :to and (:region::text is null or region = :region)
                        """)
                .param("from", ts(from, Instant.now().minus(30, ChronoUnit.DAYS)))
                .param("to", ts(to, Instant.now().plus(1, ChronoUnit.DAYS)))
                .param("region", region)
                .query().singleRow();
    }

    /** Which exception types / regions produce the most work. */
    @GetMapping("/breakdown")
    public List<Map<String, Object>> breakdown(@RequestParam(defaultValue = "type") String by,
                                               @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
                                               @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to) {
        // the column name comes from a fixed whitelist, never from user input
        String column = switch (by) {
            case "region" -> "region";
            case "policy" -> "policy_code";
            case "category" -> "category";
            default -> "exception_type";
        };
        return jdbc.sql("select coalesce(" + column + ", 'UNCLASSIFIED') as key, " + AGGREGATES + """
                          from exception_fact
                         where detected_at >= :from and detected_at < :to
                         group by 1 order by exception_count desc
                        """)
                .param("from", ts(from, Instant.now().minus(30, ChronoUnit.DAYS)))
                .param("to", ts(to, Instant.now().plus(1, ChronoUnit.DAYS)))
                .query().listOfRows();
    }

    /** Which external systems give more UNKNOWN / failures. */
    @GetMapping("/external-systems")
    public List<Map<String, Object>> externalSystems() {
        return jdbc.sql("""
                select target_system,
                       sum(calls)                                             as results,
                       sum(calls) filter (where outcome = 'UNKNOWN')          as unknown,
                       sum(calls) filter (where outcome = 'FAILED')           as failed,
                       round(coalesce(sum(calls) filter (where outcome = 'UNKNOWN'), 0) / sum(calls), 4) as unknown_rate
                  from external_call_stat
                 where day >= current_date - 30
                 group by target_system order by unknown_rate desc
                """).query().listOfRows();
    }

    private static Timestamp ts(Instant value, Instant fallback) {
        return Timestamp.from(value != null ? value : fallback);
    }
}
