package org.example.processor;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

/**
 * Reads cases.saved without ever waiting for slow cases.
 *
 * <p>Each of {@code consumers} platform threads owns one Kafka consumer (Kafka consumers are not thread-safe) and
 * loops: fetch, start every fetched case on its own virtual thread, commit progress, fetch again. Cases finish in
 * any order. Per partition, {@link PartitionProgress} commits only up to the oldest case still running, so a crash
 * redelivers unfinished cases and never skips one.
 *
 * <p>The only brake is {@code max-in-flight}: when too few slots are free for another full fetch, the consumer
 * pauses its partitions (it keeps polling, so it stays in the group) and the backlog waits in Kafka.
 */
@Component
public class CaseConsumerLoop implements SmartLifecycle {
    private static final Logger log = LoggerFactory.getLogger(CaseConsumerLoop.class);
    private static final Duration POLL = Duration.ofMillis(100);

    private final ConsumerFactory<String, String> consumers;
    private final CaseProcessor processor;
    private final String topic;
    private final String groupId;
    private final int consumerCount;
    private final int maxPollRecords;
    private final Semaphore inFlight;
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final List<Thread> threads = new ArrayList<>();
    private final List<Consumer<String, String>> openConsumers = new ArrayList<>();
    private volatile boolean running;

    public CaseConsumerLoop(ConsumerFactory<String, String> consumers, CaseProcessor processor,
            @Value("${caseroute.kafka.saved-topic}") String topic,
            @Value("${spring.kafka.consumer.group-id}") String groupId,
            @Value("${caseroute.processing.consumers}") int consumerCount,
            @Value("${spring.kafka.consumer.max-poll-records}") int maxPollRecords,
            @Value("${caseroute.processing.max-in-flight}") int maxInFlight) {
        this.consumers = consumers;
        this.processor = processor;
        this.topic = topic;
        this.groupId = groupId;
        this.consumerCount = consumerCount;
        this.maxPollRecords = maxPollRecords;
        this.inFlight = new Semaphore(maxInFlight);
    }

    @Override
    public synchronized void start() {
        running = true;
        for (int i = 0; i < consumerCount; i++) {
            Properties overrides = new Properties();
            overrides.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // commits are ours, per PartitionProgress
            Consumer<String, String> consumer = consumers.createConsumer(groupId, null, "-" + i, overrides);
            openConsumers.add(consumer);
            Thread thread = Thread.ofPlatform().name("case-consumer-" + i).start(() -> run(consumer));
            threads.add(thread);
        }
        log.info("Started {} consumers on {} (max {} cases in flight)", consumerCount, topic, inFlight.availablePermits());
    }

    private void run(Consumer<String, String> consumer) {
        Map<TopicPartition, PartitionProgress> progress = new ConcurrentHashMap<>();
        consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                commit(consumer, progress, true); // record what is finished before giving the partitions away
                partitions.forEach(progress::remove);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    progress.put(partition, new PartitionProgress(consumer.position(partition)));
                }
            }
        });
        try {
            while (running) {
                // Backpressure: fetch only when a full fetch fits in the in-flight limit.
                if (inFlight.availablePermits() < maxPollRecords) consumer.pause(consumer.assignment());
                else consumer.resume(consumer.paused());

                ConsumerRecords<String, String> records = consumer.poll(POLL);
                for (ConsumerRecord<String, String> record : records) {
                    PartitionProgress partition = progress.get(new TopicPartition(record.topic(), record.partition()));
                    if (partition == null) continue; // revoked meanwhile; the new owner will process it
                    inFlight.acquireUninterruptibly();
                    partition.started(record.offset());
                    workers.submit(() -> {
                        try {
                            processor.process(record);
                            partition.finished(record.offset());
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt(); // shutting down: not finished, so it is redelivered
                        } catch (RuntimeException ex) {
                            // Left unfinished on purpose: this partition's commits stop here until a restart
                            // redelivers the case, rather than skipping it.
                            log.error("Processing failed unexpectedly at {}-{}@{}", record.topic(), record.partition(),
                                    record.offset(), ex);
                        } finally {
                            inFlight.release();
                        }
                    });
                }
                commit(consumer, progress, false);
            }
        } catch (WakeupException ex) {
            if (running) throw ex;
        } catch (RuntimeException ex) {
            log.error("Consumer loop stopped", ex);
        } finally {
            try { commit(consumer, progress, true); } catch (RuntimeException ignored) { }
            consumer.close();
        }
    }

    /** Commits, per partition, up to the oldest case still in progress. Must run on the consumer's own thread. */
    private void commit(Consumer<String, String> consumer, Map<TopicPartition, PartitionProgress> progress, boolean sync) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        progress.forEach((partition, state) -> {
            long offset = state.commitCandidate();
            if (offset >= 0) offsets.put(partition, new OffsetAndMetadata(offset));
        });
        if (offsets.isEmpty()) return;
        if (sync) consumer.commitSync(offsets);
        else consumer.commitAsync(offsets, (committed, error) -> {
            if (error != null) log.warn("Offset commit failed; a later commit covers it: {}", error.toString());
        });
    }

    @Override
    public synchronized void stop() {
        running = false;
        openConsumers.forEach(Consumer::wakeup);
        workers.shutdownNow(); // unfinished cases are not committed, so they are redelivered after a restart
        for (Thread thread : threads) {
            try { thread.join(10_000); } catch (InterruptedException ex) { Thread.currentThread().interrupt(); }
        }
        threads.clear();
        openConsumers.clear();
    }

    @Override
    public boolean isRunning() { return running; }
}
