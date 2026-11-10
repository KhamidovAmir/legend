package com.demp.integration.api;

import com.demp.integration.domain.IntegrationAttempt;
import com.demp.integration.domain.IntegrationOperation;
import com.demp.integration.domain.Repositories;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Troubleshooting API: what exactly was sent to the external system and how it answered. */
@RestController
@RequestMapping("/api/v1/operations")
@RequiredArgsConstructor
public class OperationController {

    private final Repositories.Operations operations;
    private final Repositories.Attempts attempts;

    @GetMapping("/{id}")
    public ResponseEntity<OperationView> get(@PathVariable UUID id) {
        return operations.findById(id)
                .map(op -> ResponseEntity.ok(OperationView.of(op, attempts.findByOperationIdOrderByAttemptNo(id))))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    public record AttemptView(int attemptNo, String outcome, Integer httpStatus, String error, long durationMs, Instant startedAt) {
    }

    public record OperationView(UUID id, String kind, String status, String targetSystem, String actionType,
                                String entityId, String externalRequestId, String result, List<AttemptView> attempts) {
        static OperationView of(IntegrationOperation op, List<IntegrationAttempt> attempts) {
            return new OperationView(op.getId(), op.getKind().name(), op.getStatus().name(), op.getTargetSystem(),
                    op.getActionType(), op.getEntityId(), op.getExternalRequestId(), op.getResult(),
                    attempts.stream().map(a -> new AttemptView(a.getAttemptNo(), a.getOutcome(), a.getHttpStatus(),
                            a.getError(), a.getDurationMs(), a.getStartedAt())).toList());
        }
    }
}
