package com.demp.platform.kafka;

import com.demp.contracts.Topics;
import com.demp.platform.inbox.IdempotentConsumer;
import com.demp.platform.messaging.EnvelopeReader;
import com.demp.platform.messaging.InvalidMessageException;
import com.demp.platform.outbox.OutboxProperties;
import com.demp.platform.outbox.OutboxWriter;
import com.demp.platform.outbox.PollingOutboxRelay;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.tracing.Tracer;
import java.time.Clock;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.backoff.ExponentialBackOff;

@AutoConfiguration(after = {DataSourceAutoConfiguration.class, KafkaAutoConfiguration.class},
        afterName = "org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration")
@EnableConfigurationProperties(OutboxProperties.class)
public class PlatformAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    @ConditionalOnMissingBean
    EnvelopeReader envelopeReader(ObjectMapper mapper) {
        return new EnvelopeReader(mapper);
    }

    @Bean
    @ConditionalOnMissingBean
    TransactionTemplate transactionTemplate(PlatformTransactionManager tm) {
        return new TransactionTemplate(tm);
    }

    @Bean
    @ConditionalOnMissingBean
    IdempotentConsumer idempotentConsumer(JdbcTemplate jdbc, TransactionTemplate tx, MeterRegistry meters) {
        return new IdempotentConsumer(jdbc, tx, meters);
    }

    @Bean
    @ConditionalOnMissingBean
    OutboxWriter outboxWriter(JdbcTemplate jdbc, ObjectMapper mapper, Clock clock, MeterRegistry meters,
                              ObjectProvider<Tracer> tracer) {
        return new OutboxWriter(jdbc, mapper, clock, meters, tracer.getIfAvailable());
    }

    /**
     * Retry with exponential backoff, then publish to {@code <topic>.dlt} and move on, so one bad record
     * does not block the partition. {@link InvalidMessageException} skips retries.
     */
    @Bean
    @ConditionalOnMissingBean(CommonErrorHandler.class)
    @ConditionalOnClass(KafkaTemplate.class)
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> template, MeterRegistry meters) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template, (record, ex) -> {
            meters.counter("kafka_dead_letter_total", "topic", record.topic()).increment();
            return new TopicPartition(record.topic() + Topics.DLT_SUFFIX, -1);
        });
        ExponentialBackOff backOff = new ExponentialBackOff(200, 2.0);
        backOff.setMaxInterval(5_000);
        backOff.setMaxAttempts(6);
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(InvalidMessageException.class);
        return handler;
    }

    @Configuration(proxyBeanMethods = false)
    @EnableScheduling
    @ConditionalOnProperty(prefix = "platform.outbox.polling", name = "enabled", havingValue = "true")
    static class PollingRelayConfiguration {

        @Bean
        PollingOutboxRelay pollingOutboxRelay(JdbcTemplate jdbc, KafkaTemplate<String, String> kafka,
                                              TransactionTemplate tx, OutboxProperties props, MeterRegistry meters) {
            return new PollingOutboxRelay(jdbc, kafka, tx, props.getPolling(), meters);
        }
    }
}
