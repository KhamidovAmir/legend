package com.demp.exceptions.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** Directed link: {@code from CAUSED to}. */
@Entity
@Table(name = "exception_relation")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ExceptionRelation {

    public enum Type { CAUSED, RELATED_TO, DUPLICATE_OF }

    @Id
    private UUID id;
    private UUID fromExceptionId;
    private UUID toExceptionId;
    @Enumerated(EnumType.STRING)
    private Type relationType;
    private Instant createdAt;

    public ExceptionRelation(UUID from, UUID to, Type type, Instant now) {
        this.id = UUID.randomUUID();
        this.fromExceptionId = from;
        this.toExceptionId = to;
        this.relationType = type;
        this.createdAt = now;
    }
}
