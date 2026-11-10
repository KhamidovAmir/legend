package com.demp.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.demp.integration.config.IntegrationProperties;
import com.demp.integration.config.IntegrationProperties.SystemPolicy;
import com.demp.integration.domain.IntegrationOperation.Kind;
import com.demp.integration.execution.ExternalCallGuard;
import com.demp.integration.execution.ExternalCallGuard.CallResult;
import com.demp.integration.execution.TransportClassifier.Outcome;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;

class ExternalCallGuardTest {

    @Test
    void breakerOpensAfterFailuresAndThenCallsAreNotSentAtAll() {
        ExternalCallGuard guard = guard(policy -> policy.setCircuitBreakerMinimumCalls(2));
        AtomicInteger calls = new AtomicInteger();

        for (int i = 0; i < 2; i++) {
            CallResult<String> failed = guard.call("DELIVERY", Kind.COMMAND, () -> {
                calls.incrementAndGet();
                throw new ResourceAccessException("x", new IOException("reset"));
            });
            assertThat(failed.classified().outcome()).isEqualTo(Outcome.UNKNOWN);
        }
        assertThat(guard.isOpen("DELIVERY")).isTrue();

        CallResult<String> blocked = guard.call("DELIVERY", Kind.QUERY, () -> {
            calls.incrementAndGet();
            return "never";
        });
        assertThat(blocked.classified().outcome()).isEqualTo(Outcome.NOT_SENT);
        assertThat(blocked.classified().reason()).isEqualTo("CIRCUIT_OPEN");
        assertThat(calls).as("a degraded system is not called while the breaker is open").hasValue(2);
    }

    @Test
    void rejectionsProveTheSystemIsAliveAndDoNotOpenTheBreaker() {
        ExternalCallGuard guard = guard(policy -> policy.setCircuitBreakerMinimumCalls(2));
        for (int i = 0; i < 5; i++) {
            CallResult<String> rejected = guard.call("DELIVERY", Kind.COMMAND, () -> {
                throw HttpClientErrorException.create(HttpStatus.UNPROCESSABLE_ENTITY, "x", null, null, null);
            });
            assertThat(rejected.classified().outcome()).isEqualTo(Outcome.REJECTED);
        }
        assertThat(guard.isOpen("DELIVERY")).isFalse();
    }

    @Test
    void queriesCannotConsumeTheRateLimitOfCommands() {
        ExternalCallGuard guard = guard(policy -> {
            policy.setQueriesPerSecond(1);
            policy.setCommandsPerSecond(5);
            policy.setRateLimitWait(Duration.ZERO);
        });

        assertThat(guard.call("DELIVERY", Kind.QUERY, () -> "ok").success()).isTrue();
        CallResult<String> limited = guard.call("DELIVERY", Kind.QUERY, () -> "ok");
        assertThat(limited.classified().reason()).isEqualTo("RATE_LIMITED");
        assertThat(limited.classified().outcome()).isEqualTo(Outcome.NOT_SENT);

        assertThat(guard.call("DELIVERY", Kind.COMMAND, () -> "ok").success())
                .as("commands have their own share").isTrue();
    }

    private static ExternalCallGuard guard(java.util.function.Consumer<SystemPolicy> customizer) {
        IntegrationProperties properties = new IntegrationProperties();
        SystemPolicy policy = new SystemPolicy();
        customizer.accept(policy);
        properties.getSystems().put("DELIVERY", policy);
        return new ExternalCallGuard(properties, new SimpleMeterRegistry());
    }
}
