package com.demp.exceptions.api;

import com.demp.exceptions.domain.CloseReason;
import com.demp.exceptions.domain.ExceptionHistory;
import com.demp.exceptions.domain.ExceptionRecord;
import com.demp.exceptions.domain.ExceptionReference;
import com.demp.exceptions.domain.ExceptionRelation;
import com.demp.exceptions.domain.ExceptionStatus;
import com.demp.exceptions.domain.Repositories;
import com.demp.exceptions.lifecycle.ExceptionLifecycleService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Operator / query API. */
@RestController
@RequestMapping("/api/v1/exceptions")
@RequiredArgsConstructor
public class ExceptionController {

    private final ExceptionLifecycleService lifecycle;
    private final Repositories.ExceptionRecords exceptions;
    private final Repositories.References references;
    private final Repositories.Relations relations;
    private final Repositories.History history;

    @GetMapping
    public PageView<ExceptionView> list(@RequestParam(required = false) ExceptionStatus status,
                                        @RequestParam(defaultValue = "0") int page,
                                        @RequestParam(defaultValue = "50") int size) {
        PageRequest request = PageRequest.of(page, Math.min(size, 200), Sort.by(Sort.Direction.DESC, "createdAt"));
        Page<ExceptionRecord> result = status == null ? exceptions.findAll(request) : exceptions.findByStatus(status, request);
        return new PageView<>(result.map(e -> ExceptionView.of(e, List.of())).getContent(), result.getTotalElements());
    }

    @GetMapping("/{id}")
    public ExceptionView get(@PathVariable UUID id) {
        return ExceptionView.of(lifecycle.get(id), references.findByExceptionId(id));
    }

    @GetMapping("/{id}/history")
    public List<HistoryView> history(@PathVariable UUID id) {
        lifecycle.get(id);
        return history.findByExceptionIdOrderByCreatedAtAscIdAsc(id).stream().map(HistoryView::of).toList();
    }

    @GetMapping("/{id}/relations")
    public List<RelationView> relations(@PathVariable UUID id) {
        return relations.findAllTouching(id).stream().map(RelationView::of).toList();
    }

    @PostMapping("/{id}/relations")
    public RelationView link(@PathVariable UUID id, @Valid @RequestBody LinkRequest request) {
        return RelationView.of(lifecycle.link(id, request.toExceptionId(), request.type(), request.operator()));
    }

    @PostMapping("/{id}/classification")
    public ExceptionView classify(@PathVariable UUID id, @Valid @RequestBody ClassifyRequest request) {
        ExceptionRecord e = lifecycle.classifyManually(id, request.category(), request.severity(), request.priority(),
                request.operator());
        return ExceptionView.of(e, references.findByExceptionId(id));
    }

    @PostMapping("/{id}/close")
    public ExceptionView close(@PathVariable UUID id, @Valid @RequestBody CloseRequest request) {
        return ExceptionView.of(lifecycle.closeByOperator(id, request.reason(), request.operator()),
                references.findByExceptionId(id));
    }

    public record PageView<T>(List<T> items, long total) {
    }

    public record ReferenceView(String system, String entityType, String entityId) {
    }

    public record ExceptionView(UUID id, String exceptionType, String category, String severity, String priority,
                                String region, ExceptionStatus status, CloseReason closeReason, String resolution,
                                String sourceSystem, Instant detectedAt, Instant createdAt, Instant resolvedAt,
                                Instant closedAt, List<ReferenceView> references) {
        static ExceptionView of(ExceptionRecord e, List<ExceptionReference> refs) {
            return new ExceptionView(e.getId(), e.getExceptionType(), e.getCategory(), e.getSeverity(), e.getPriority(),
                    e.getRegion(), e.getStatus(), e.getCloseReason(), e.getResolution(), e.getSourceSystem(),
                    e.getDetectedAt(), e.getCreatedAt(), e.getResolvedAt(), e.getClosedAt(),
                    refs.stream().map(r -> new ReferenceView(r.getSystem(), r.getEntityType(), r.getEntityId())).toList());
        }
    }

    public record HistoryView(String event, String fromStatus, String toStatus, String actor, String details, Instant at) {
        static HistoryView of(ExceptionHistory h) {
            return new HistoryView(h.getEvent(), h.getFromStatus(), h.getToStatus(), h.getActor(), h.getDetails(), h.getCreatedAt());
        }
    }

    public record RelationView(UUID from, UUID to, ExceptionRelation.Type type) {
        static RelationView of(ExceptionRelation r) {
            return new RelationView(r.getFromExceptionId(), r.getToExceptionId(), r.getRelationType());
        }
    }

    public record CloseRequest(@NotNull CloseReason reason, @NotBlank String operator) {
    }

    public record ClassifyRequest(@NotBlank String category, @NotBlank String severity, @NotBlank String priority,
                                  @NotBlank String operator) {
    }

    public record LinkRequest(@NotNull UUID toExceptionId, @NotNull ExceptionRelation.Type type, @NotBlank String operator) {
    }
}
