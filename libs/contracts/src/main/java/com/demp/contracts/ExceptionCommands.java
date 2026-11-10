package com.demp.contracts;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.UUID;

/**
 * Commands accepted by exception-service on {@link Topics#EXCEPTION_COMMANDS}.
 * Resolution Service initiates transitions, but the state itself is owned by Exception Service.
 */
public final class ExceptionCommands {

    public static final String START_PROGRESS = "StartExceptionProgress";
    public static final String MARK_WAITING = "MarkExceptionWaiting";
    public static final String RESUME = "ResumeException";
    public static final String RESOLVE = "ResolveException";
    public static final String CLOSE = "CloseException";

    /**
     * @param reason     mandatory for CloseException (FALSE_POSITIVE, DUPLICATE, UNRESOLVABLE, CANCELLED_BY_OPERATOR)
     * @param resolution business result, set for ResolveException
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ExceptionCommand(UUID exceptionId, String actor, String reason, String resolution) {

        public static ExceptionCommand of(UUID exceptionId, String actor) {
            return new ExceptionCommand(exceptionId, actor, null, null);
        }
    }

    private ExceptionCommands() {
    }
}
