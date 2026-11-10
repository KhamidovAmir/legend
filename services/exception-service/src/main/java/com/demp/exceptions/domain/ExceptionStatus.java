package com.demp.exceptions.domain;

import java.util.EnumSet;
import java.util.Set;

/** OPEN -> CLASSIFIED -> IN_PROGRESS <-> WAITING -> RESOLVED -> CLOSED. */
public enum ExceptionStatus {
    OPEN, CLASSIFIED, IN_PROGRESS, WAITING, RESOLVED, CLOSED;

    public Set<ExceptionStatus> next() {
        return switch (this) {
            case OPEN -> EnumSet.of(CLASSIFIED);
            case CLASSIFIED -> EnumSet.of(IN_PROGRESS);
            case IN_PROGRESS -> EnumSet.of(WAITING, RESOLVED);
            case WAITING -> EnumSet.of(IN_PROGRESS, RESOLVED);
            case RESOLVED -> EnumSet.of(CLOSED);
            case CLOSED -> EnumSet.noneOf(ExceptionStatus.class);
        };
    }

    public boolean canMoveTo(ExceptionStatus target) {
        return next().contains(target);
    }

    /** "Unfinished": may still be closed without a Resolution, with a mandatory reason. */
    public boolean isUnfinished() {
        return this != RESOLVED && this != CLOSED;
    }
}
