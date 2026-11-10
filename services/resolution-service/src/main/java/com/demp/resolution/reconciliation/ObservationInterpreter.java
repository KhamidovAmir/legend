package com.demp.resolution.reconciliation;

import com.demp.resolution.domain.ReconciliationTask.Result;
import java.util.Set;

/**
 * Turns an observation of the external state into a reconciliation result.
 * This is a BUSINESS decision and therefore lives in Resolution, not in Integration.
 */
public final class ObservationInterpreter {

    private ObservationInterpreter() {
    }

    public static Result interpret(boolean found, String actualState, String expectedState, Set<String> processingStates) {
        if (!found || actualState == null || actualState.isBlank()) {
            return Result.INCONCLUSIVE;
        }
        if (actualState.equals(expectedState)) {
            return Result.APPLIED;
        }
        if (processingStates.contains(actualState)) {
            return Result.STILL_PROCESSING;
        }
        // Only a CANDIDATE: the read model of the external system may lag behind the write.
        return Result.NOT_APPLIED;
    }
}
