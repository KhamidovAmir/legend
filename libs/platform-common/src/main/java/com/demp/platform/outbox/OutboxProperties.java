package com.demp.platform.outbox;

import java.time.Duration;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties("platform.outbox")
public class OutboxProperties {

    private final Polling polling = new Polling();

    @Data
    public static class Polling {
        /** false for services whose outbox is delivered by Debezium CDC. */
        private boolean enabled = false;
        private Duration interval = Duration.ofMillis(500);
        private int batchSize = 200;
        private Duration sendTimeout = Duration.ofSeconds(10);
        /** published rows are kept for troubleshooting / re-publication for this long */
        private Duration retention = Duration.ofDays(7);
    }
}
