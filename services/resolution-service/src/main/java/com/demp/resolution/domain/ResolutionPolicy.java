package com.demp.resolution.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.UUID;
import lombok.Getter;

/** How a classified exception of a given type is resolved. Versioned: a running workflow keeps its version. */
@Entity
@Table(name = "resolution_policy")
@Getter
public class ResolutionPolicy {

    public enum ExecutionMode { EXTERNAL_AUTOMATED, MANUAL }

    @Id
    private UUID id;
    private String code;
    private int version;
    private String exceptionType;
    @Enumerated(EnumType.STRING)
    private ExecutionMode executionMode;
    private String actionType;
    private String targetSystem;
    private String expectedState;
    private String resolutionResult;
    private String botId;
    private int maxBusinessAttempts;
    private int slaMinutes;
    private boolean enabled;
}
