package org.example.backend.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    @Bean
    NewTopic caseSubmissionsTopic(@Value("${caseroute.kafka.submissions-topic}") String topic,
            @Value("${caseroute.kafka.submissions-partitions}") int partitions) {
        return TopicBuilder.name(topic).partitions(partitions).replicas(1).build();
    }

    @Bean
    NewTopic caseSubmissionsDeadLetterTopic(@Value("${caseroute.kafka.submissions-topic}") String topic) {
        return TopicBuilder.name(topic + ".DLT").partitions(1).replicas(1).build();
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
