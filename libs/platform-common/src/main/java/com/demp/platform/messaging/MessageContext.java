package com.demp.platform.messaging;

import org.slf4j.MDC;

/**
 * Correlation context of the message (or HTTP request) being processed on the current thread.
 * Outbox messages written inside the scope inherit correlationId and get causationId = id of the current message.
 * The same values are put into MDC, so every JSON log line carries them.
 */
public final class MessageContext {

    public static final String MDC_CORRELATION_ID = "correlationId";
    public static final String MDC_CAUSATION_ID = "causationId";
    public static final String MDC_EVENT_ID = "eventId";
    public static final String MDC_EXCEPTION_ID = "exceptionId";

    private static final ThreadLocal<MessageContext> CURRENT = new ThreadLocal<>();

    private final String correlationId;
    private final String currentMessageId;

    private MessageContext(String correlationId, String currentMessageId) {
        this.correlationId = correlationId;
        this.currentMessageId = currentMessageId;
    }

    public static Scope open(String correlationId, String currentMessageId, String incomingCausationId) {
        MessageContext previous = CURRENT.get();
        CURRENT.set(new MessageContext(correlationId, currentMessageId));
        put(MDC_CORRELATION_ID, correlationId);
        put(MDC_EVENT_ID, currentMessageId);
        put(MDC_CAUSATION_ID, incomingCausationId);
        return () -> {
            MDC.remove(MDC_CORRELATION_ID);
            MDC.remove(MDC_EVENT_ID);
            MDC.remove(MDC_CAUSATION_ID);
            MDC.remove(MDC_EXCEPTION_ID);
            if (previous == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(previous);
            }
        };
    }

    public static Scope open(EventEnvelope envelope) {
        return open(envelope.correlationId(), envelope.id().toString(), envelope.causationId());
    }

    public static String correlationId() {
        MessageContext ctx = CURRENT.get();
        return ctx == null ? null : ctx.correlationId;
    }

    public static String currentMessageId() {
        MessageContext ctx = CURRENT.get();
        return ctx == null ? null : ctx.currentMessageId;
    }

    public static void exceptionId(Object exceptionId) {
        put(MDC_EXCEPTION_ID, exceptionId == null ? null : exceptionId.toString());
    }

    private static void put(String key, String value) {
        if (value != null) {
            MDC.put(key, value);
        }
    }

    public interface Scope extends AutoCloseable {
        @Override
        void close();
    }
}
