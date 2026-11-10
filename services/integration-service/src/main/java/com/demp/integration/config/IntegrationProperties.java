package com.demp.integration.config;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties("integration")
public class IntegrationProperties {

    private Map<String, SystemPolicy> systems = new HashMap<>();

    /** IntegrationEndpointPolicy: timeouts, transport retry limits and rate limit shares of one external system. */
    @Data
    public static class SystemPolicy {
        private String baseUrl;
        private Duration connectTimeout = Duration.ofSeconds(1);
        private Duration readTimeout = Duration.ofSeconds(3);
        private int maxCommandAttempts = 3;
        private int maxQueryAttempts = 3;
        private Duration retryBackoff = Duration.ofMillis(200);
        private int commandsPerSecond = 20;
        private int queriesPerSecond = 10;
        private Duration rateLimitWait = Duration.ofSeconds(2);
        private Duration circuitBreakerOpenDuration = Duration.ofSeconds(30);
        private int circuitBreakerMinimumCalls = 10;
        private float circuitBreakerFailureRate = 50;
    }
}
