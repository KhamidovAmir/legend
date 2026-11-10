package com.demp.contracts;

/**
 * Kafka topics of the platform. Naming: {@code <owner>.<kind>}.
 * The owner of a topic is the only service allowed to define its schema
 * (for *.events the producer owns it, for *.commands / *.queries the consumer does).
 */
public final class Topics {

    /** Owned by external Delivery team. Consumed by exception-service. */
    public static final String EXTERNAL_DELIVERY_EVENTS = "external.delivery.events";
    /** Owned by external Delivery team: async results of our commands. Consumed by integration-service. */
    public static final String EXTERNAL_DELIVERY_RESULTS = "external.delivery.results";

    public static final String EXCEPTION_EVENTS = "exception.events";
    public static final String EXCEPTION_COMMANDS = "exception.commands";

    public static final String RESOLUTION_EVENTS = "resolution.events";

    /** Side-effecting commands. Separate from queries so that checks never starve commands. */
    public static final String INTEGRATION_COMMANDS = "integration.commands";
    public static final String INTEGRATION_QUERIES = "integration.queries";
    public static final String INTEGRATION_EVENTS = "integration.events";

    public static final String DLT_SUFFIX = ".dlt";

    private Topics() {
    }
}
