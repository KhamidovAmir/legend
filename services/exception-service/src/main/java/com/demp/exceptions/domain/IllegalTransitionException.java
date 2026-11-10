package com.demp.exceptions.domain;

import lombok.Getter;

@Getter
public class IllegalTransitionException extends RuntimeException {

    private final ExceptionStatus from;
    private final ExceptionStatus to;

    public IllegalTransitionException(ExceptionStatus from, ExceptionStatus to) {
        super("Transition %s -> %s is not allowed".formatted(from, to));
        this.from = from;
        this.to = to;
    }
}
