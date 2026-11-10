package com.demp.exceptions.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.UUID;
import lombok.Getter;

/** Decides whether an inbound event creates an Exception. Says nothing about how it will be resolved. */
@Entity
@Table(name = "registration_rule")
@Getter
public class RegistrationRule {

    public enum Decision { CREATE, IGNORE }

    @Id
    private UUID id;
    private String sourceSystem;
    private String eventType;
    @Enumerated(EnumType.STRING)
    private Decision decision;
    private String exceptionType;
    private boolean enabled;
}
