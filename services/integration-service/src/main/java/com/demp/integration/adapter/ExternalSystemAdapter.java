package com.demp.integration.adapter;

import com.demp.integration.domain.IntegrationOperation;

/**
 * One adapter per external system: endpoints, DTO mapping, protocol. Connecting a new system = a new adapter
 * + an entry in integration.systems; the execution engine (retry, UNKNOWN, limits) stays the same.
 * Methods throw the HTTP client's exceptions; {@link com.demp.integration.execution.TransportClassifier} interprets them.
 */
public interface ExternalSystemAdapter {

    String system();

    CommandResponse execute(IntegrationOperation operation);

    StateResponse queryState(String entityType, String entityId);

    /**
     * @param completed         true if the system performed the action synchronously
     * @param externalRequestId id of the async request (when only accepted)
     * @param result            normalized result (when completed)
     */
    record CommandResponse(boolean completed, String externalRequestId, String result, int httpStatus) {
    }

    /** Normalized observation of the external entity. */
    record StateResponse(boolean found, String state) {
    }
}
