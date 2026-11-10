package com.demp.exceptions.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** Only a pointer to an object owned by an external system; its data is never copied here. */
@Entity
@Table(name = "exception_reference")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ExceptionReference {

    @Id
    private UUID id;
    private UUID exceptionId;
    private String system;
    private String entityType;
    private String entityId;

    public ExceptionReference(UUID exceptionId, String system, String entityType, String entityId) {
        this.id = UUID.randomUUID();
        this.exceptionId = exceptionId;
        this.system = system;
        this.entityType = entityType;
        this.entityId = entityId;
    }
}
