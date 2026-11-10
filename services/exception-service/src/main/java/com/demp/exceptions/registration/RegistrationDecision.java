package com.demp.exceptions.registration;

import java.util.UUID;

public record RegistrationDecision(Outcome outcome, UUID exceptionId) {

    public enum Outcome {
        /** the same (sourceSystem, eventId) was already registered */
        DUPLICATE_EVENT,
        IGNORED,
        CREATED,
        /** a new signal about an already active exception */
        LINKED
    }
}
