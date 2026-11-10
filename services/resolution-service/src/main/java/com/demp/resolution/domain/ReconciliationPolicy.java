package com.demp.resolution.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "reconciliation_policy")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor
public class ReconciliationPolicy {

    @Id
    private UUID id;
    private String targetSystem;
    private String actionType;
    private int settleDelaySeconds;
    private int baseIntervalSeconds;
    private int maxIntervalSeconds;
    private int queryTimeoutSeconds;
    private int notAppliedConfirmations;
    private int deadlineMinutes;
    private boolean autoRetryEnabled;
    private String processingStates;

    /** Conservative defaults for a system without explicit settings: never retry automatically. */
    public static ReconciliationPolicy conservativeDefault(String system, String actionType) {
        return new ReconciliationPolicy(null, system, actionType, 120, 60, 900, 120, 3, 60, false, "");
    }

    public Duration settleDelay() {
        return Duration.ofSeconds(settleDelaySeconds);
    }

    public Duration queryTimeout() {
        return Duration.ofSeconds(queryTimeoutSeconds);
    }

    public Duration deadline() {
        return Duration.ofMinutes(deadlineMinutes);
    }

    public Set<String> processingStateSet() {
        return Arrays.stream(processingStates.split(",")).map(String::trim).filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());
    }
}
