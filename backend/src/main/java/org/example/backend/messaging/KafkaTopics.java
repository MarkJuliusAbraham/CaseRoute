package org.example.backend.messaging;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Partition counts of the pipeline's topics, read from Kafka, and adding partitions while the system runs.
 *
 * <p>Kafka can add partitions to a topic but never remove them; fewer would mean deleting the topic and its
 * messages. Adding is safe here: messages are keyed by clientRequestId or case id, which are unique, so no ordering
 * depends on which partition a key maps to. The warm-up topic always gets as many partitions as
 * {@code cases.submitted}, so warm-ups exercise the same layout as the run. The counts in
 * {@code application.properties} only apply when a topic is first created.
 */
@Component
public class KafkaTopics {
    private final KafkaAdmin admin;
    private final String submitted;
    private final String warmup;
    private final List<String> adjustable;

    public KafkaTopics(KafkaAdmin admin,
            @Value("${caseroute.kafka.submissions-topic}") String submitted,
            @Value("${caseroute.kafka.saved-topic}") String saved,
            @Value("${caseroute.kafka.processed-topic}") String processed,
            @Value("${caseroute.kafka.warmup-topic}") String warmup) {
        this.admin = admin;
        this.submitted = submitted;
        this.warmup = warmup;
        this.adjustable = List.of(submitted, saved, processed);
    }

    /** Topic name to its current partition count, for the adjustable topics and the warm-up topic. */
    public Map<String, Integer> partitions() {
        var names = new java.util.ArrayList<>(adjustable);
        names.add(warmup);
        Map<String, TopicDescription> descriptions = admin.describeTopics(names.toArray(String[]::new));
        var counts = new LinkedHashMap<String, Integer>();
        for (String name : names) {
            TopicDescription description = descriptions.get(name);
            if (description != null) counts.put(name, description.partitions().size());
        }
        return counts;
    }

    /**
     * Raises a topic to {@code count} partitions.
     *
     * @throws IllegalArgumentException for an unknown topic, or a count not above the current one
     */
    public Map<String, Integer> increase(String topic, int count) {
        if (!adjustable.contains(topic)) throw new IllegalArgumentException("Partitions can be changed for " + adjustable);
        if (count > 1000) throw new IllegalArgumentException("At most 1,000 partitions");
        Map<String, Integer> current = partitions();
        Integer now = current.get(topic);
        if (now == null) throw new IllegalArgumentException(topic + " does not exist yet");
        if (count <= now)
            throw new IllegalArgumentException(topic + " has " + now + " partitions; Kafka can only add partitions, never remove them");
        var changes = new LinkedHashMap<String, NewPartitions>();
        changes.put(topic, NewPartitions.increaseTo(count));
        if (topic.equals(submitted) && current.getOrDefault(warmup, 0) < count)
            changes.put(warmup, NewPartitions.increaseTo(count));
        try (AdminClient client = AdminClient.create(admin.getConfigurationProperties())) {
            client.createPartitions(changes).all().get(15, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while adding partitions", ex);
        } catch (ExecutionException | TimeoutException ex) {
            throw new IllegalStateException("Kafka did not add the partitions: " + ex.getMessage(), ex);
        }
        return partitions();
    }
}
