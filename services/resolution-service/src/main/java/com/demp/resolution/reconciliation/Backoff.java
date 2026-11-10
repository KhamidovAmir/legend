package com.demp.resolution.reconciliation;

import java.time.Duration;
import java.util.function.DoubleSupplier;

/** Exponential backoff with jitter: checks of many tasks created by one outage do not hit the system at once. */
public final class Backoff {

    private static final double JITTER = 0.2;

    private Backoff() {
    }

    /**
     * @param attempt 1-based number of the check that has just been made
     * @param random  supplier of values in [0, 1)
     */
    public static Duration next(Duration base, Duration max, int attempt, DoubleSupplier random) {
        int exponent = Math.min(Math.max(attempt - 1, 0), 20);
        long raw = Math.min(max.toMillis(), base.toMillis() * (1L << exponent));
        double factor = 1 - JITTER + 2 * JITTER * random.getAsDouble();   // 0.8 .. 1.2
        return Duration.ofMillis(Math.min(max.toMillis(), Math.round(raw * factor)));
    }
}
