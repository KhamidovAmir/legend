package com.demp.platform.messaging;

/**
 * Kafka header names of the internal message envelope.
 * The same names are produced by the Debezium Outbox Event Router (see connector config) and by the polling relay,
 * so consumers do not care how a message was delivered.
 */
public final class MessageHeaders {

    /** Event id. "id" is the header name hard-coded in Debezium Outbox Event Router. */
    public static final String ID = "id";
    public static final String EVENT_TYPE = "eventType";
    public static final String EVENT_VERSION = "eventVersion";
    public static final String AGGREGATE_TYPE = "aggregateType";
    public static final String AGGREGATE_ID = "aggregateId";
    public static final String AGGREGATE_VERSION = "aggregateVersion";
    public static final String CORRELATION_ID = "correlationId";
    public static final String CAUSATION_ID = "causationId";
    public static final String OCCURRED_AT = "occurredAt";
    /** W3C trace context, so that a trace survives the outbox hop. */
    public static final String TRACEPARENT = "traceparent";

    private MessageHeaders() {
    }
}
