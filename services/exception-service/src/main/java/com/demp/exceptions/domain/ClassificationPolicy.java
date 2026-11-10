package com.demp.exceptions.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.UUID;
import lombok.Getter;

@Entity
@Table(name = "classification_policy")
@Getter
public class ClassificationPolicy {

    @Id
    private UUID id;
    private String exceptionType;
    private String category;
    private String severity;
    private String priority;
}
