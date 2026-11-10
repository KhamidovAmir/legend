package com.demp.integration.execution;

import com.demp.integration.config.IntegrationProperties;
import com.demp.integration.config.IntegrationProperties.SystemPolicy;
import com.demp.integration.domain.IntegrationOperation.Kind;
import com.demp.integration.execution.TransportClassifier.Classified;
import com.demp.integration.execution.TransportClassifier.Outcome;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.github.resilience4j.micrometer.tagged.TaggedRateLimiterMetrics;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.springframework.stereotype.Component;

/**
 * Protection of an external system and of ourselves:
 * one circuit breaker per system (shared by commands and queries) and TWO rate limiters per system,
 * so that reconciliation queries can never eat the quota of commands.
 * Limiters are local to a replica: the effective limit is N x replicas (a known trade-off, no shared store).
 */
@Component
public class ExternalCallGuard {

    private final IntegrationProperties properties;
    private final CircuitBreakerRegistry breakers = CircuitBreakerRegistry.ofDefaults();
    private final RateLimiterRegistry limiters = RateLimiterRegistry.ofDefaults();
    private final MeterRegistry meters;

    public ExternalCallGuard(IntegrationProperties properties, MeterRegistry meters) {
        this.properties = properties;
        this.meters = meters;
        TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(breakers).bindTo(meters);
        TaggedRateLimiterMetrics.ofRateLimiterRegistry(limiters).bindTo(meters);
    }

    public record CallResult<T>(Classified classified, T value, long durationMs) {
        public boolean success() {
            return classified.outcome() == Outcome.SUCCESS;
        }
    }

    public <T> CallResult<T> call(String system, Kind kind, Supplier<T> call) {
        SystemPolicy policy = properties.getSystems().get(system);
        CircuitBreaker breaker = breaker(system, policy);
        RateLimiter limiter = limiter(system, kind, policy);
        long started = System.nanoTime();
        CallResult<T> result;
        if (!breaker.tryAcquirePermission()) {
            result = failure(CallNotPermittedException.createCallNotPermittedException(breaker), started);
        } else if (!limiter.acquirePermission()) {
            breaker.releasePermission();
            result = failure(RequestNotPermitted.createRequestNotPermitted(limiter), started);
        } else {
            long callStarted = System.nanoTime();
            try {
                T value = call.get();
                breaker.onSuccess(System.nanoTime() - callStarted, TimeUnit.NANOSECONDS);
                result = new CallResult<>(new Classified(Outcome.SUCCESS, null, null), value, millisSince(started));
            } catch (RuntimeException e) {
                result = failure(e, started);
                if (result.classified().outcome() == Outcome.REJECTED) {
                    // a 4xx answer proves the system is alive
                    breaker.onSuccess(System.nanoTime() - callStarted, TimeUnit.NANOSECONDS);
                } else {
                    breaker.onError(System.nanoTime() - callStarted, TimeUnit.NANOSECONDS, e);
                }
            }
        }
        meters.counter("integration_external_requests_total", "system", system, "kind", kind.name(),
                "outcome", result.classified().outcome().name()).increment();
        meters.timer("integration_external_request_duration", "system", system, "kind", kind.name())
                .record(Duration.ofMillis(result.durationMs()));
        return result;
    }

    public boolean isOpen(String system) {
        return breakers.find(system).map(b -> b.getState() == CircuitBreaker.State.OPEN).orElse(false);
    }

    private <T> CallResult<T> failure(Throwable error, long started) {
        return new CallResult<>(TransportClassifier.classify(error), null, millisSince(started));
    }

    private CircuitBreaker breaker(String system, SystemPolicy policy) {
        return breakers.circuitBreaker(system, () -> CircuitBreakerConfig.custom()
                .slidingWindowSize(Math.max(20, policy.getCircuitBreakerMinimumCalls()))
                .minimumNumberOfCalls(policy.getCircuitBreakerMinimumCalls())
                .failureRateThreshold(policy.getCircuitBreakerFailureRate())
                .waitDurationInOpenState(policy.getCircuitBreakerOpenDuration())
                .permittedNumberOfCallsInHalfOpenState(3)
                .build());
    }

    private RateLimiter limiter(String system, Kind kind, SystemPolicy policy) {
        int perSecond = kind == Kind.COMMAND ? policy.getCommandsPerSecond() : policy.getQueriesPerSecond();
        return limiters.rateLimiter(system + "-" + kind.name().toLowerCase(), () -> RateLimiterConfig.custom()
                .limitForPeriod(perSecond)
                .limitRefreshPeriod(Duration.ofSeconds(1))
                .timeoutDuration(policy.getRateLimitWait())
                .build());
    }

    private static long millisSince(long startedNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
    }
}
