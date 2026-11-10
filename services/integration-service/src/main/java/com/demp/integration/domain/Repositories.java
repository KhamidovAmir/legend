package com.demp.integration.domain;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public final class Repositories {

    public interface Operations extends JpaRepository<IntegrationOperation, UUID> {
        Optional<IntegrationOperation> findByTargetSystemAndExternalRequestId(String system, String externalRequestId);

        Optional<IntegrationOperation> findByTargetSystemAndIdempotencyKey(String system, String idempotencyKey);
    }

    public interface Attempts extends JpaRepository<IntegrationAttempt, UUID> {
        List<IntegrationAttempt> findByOperationIdOrderByAttemptNo(UUID operationId);
    }

    private Repositories() {
    }
}
