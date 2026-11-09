package org.example.backend.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.time.Duration;

/**
 * Topics are created (or updated, see {@code spring.kafka.admin.modify-topic-configs}) at startup.
 *
 * <p>Kafka keeps messages after they are read; they are deleted only by retention. Load tests write millions of
 * messages, so every topic keeps {@code caseroute.kafka.retention} (6 hours) instead of Kafka's default 7 days.
 * Segments roll every {@code caseroute.kafka.segment} so old data actually becomes deletable: Kafka only deletes
 * whole, closed segments, and by default a segment stays open for up to 7 days or 1 GB.
 */
@Configuration
public class KafkaConfig {

    private final String retentionMs;
    private final String segmentMs;

    public KafkaConfig(@Value("${caseroute.kafka.retention}") Duration retention,
            @Value("${caseroute.kafka.segment}") Duration segment) {
        this.retentionMs = String.valueOf(retention.toMillis());
        this.segmentMs = String.valueOf(segment.toMillis());
    }

    private TopicBuilder topic(String name, int partitions) {
        return TopicBuilder.name(name).partitions(partitions).replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, retentionMs)
                .config(TopicConfig.SEGMENT_MS_CONFIG, segmentMs);
    }

    @Bean
    NewTopic caseSubmissionsTopic(@Value("${caseroute.kafka.submissions-topic}") String topic,
            @Value("${caseroute.kafka.submissions-partitions}") int partitions) {
        return topic(topic, partitions).build();
    }

    /** "Case X now exists in the database." Published by the outbox relay, consumed by the processor. */
    @Bean
    NewTopic caseSavedTopic(@Value("${caseroute.kafka.saved-topic}") String topic,
            @Value("${caseroute.kafka.submissions-partitions}") int partitions) {
        return topic(topic, partitions).build();
    }

    @Bean
    NewTopic caseSavedDeadLetterTopic(@Value("${caseroute.kafka.saved-topic}") String topic) {
        return topic(topic + ".DLT", 1).build();
    }

    /** Processing results, published by the processor, consumed by the backend to update the cases table. */
    @Bean
    NewTopic caseProcessedTopic(@Value("${caseroute.kafka.processed-topic}") String topic,
            @Value("${caseroute.kafka.submissions-partitions}") int partitions) {
        return topic(topic, partitions).build();
    }

    @Bean
    NewTopic caseProcessedDeadLetterTopic(@Value("${caseroute.kafka.processed-topic}") String topic) {
        return topic(topic + ".DLT", 1).build();
    }

    @Bean
    NewTopic caseSubmissionsDeadLetterTopic(@Value("${caseroute.kafka.submissions-topic}") String topic) {
        return topic(topic + ".DLT", 1).build();
    }

    /**
     * Warm-up traffic from load tests: written exactly like a real submission so the producer path warms up,
     * but no consumer reads it, so nothing reaches the database. Kept 1 hour instead of 6.
     */
    @Bean
    NewTopic caseWarmupTopic(@Value("${caseroute.kafka.warmup-topic}") String topic,
            @Value("${caseroute.kafka.submissions-partitions}") int partitions) {
        return topic(topic, partitions)
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(Duration.ofHours(1).toMillis()))
                .config(TopicConfig.SEGMENT_MS_CONFIG, String.valueOf(Duration.ofMinutes(10).toMillis()))
                .build();
    }

    /**
     * A failed batch (for example, Postgres is down) is retried every second until it succeeds.
     * Nothing is skipped: the consumer simply pauses, and the backlog waits safely in Kafka.
     */
    @Bean
    CommonErrorHandler caseSubmissionErrorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(1_000L, FixedBackOff.UNLIMITED_ATTEMPTS));
    }
}
