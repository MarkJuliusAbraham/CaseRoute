package org.example.backend.messaging;

import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The producers for customer submissions ({@code POST /api/cases} and its warm-up), separate from the producer the
 * outbox relay and dead-letter topics use, so a customer's message never waits behind one of the relay's bursts.
 *
 * <p>There can be several ({@link Settings#producers}), used in turn. A producer has one network thread and one
 * connection to the broker, with at most 5 requests waiting for confirmation (the cap that comes with idempotence),
 * and every submission waits for its own confirmation. With one producer, throughput is capped at
 * 5 x records per request / confirmation time: measured at ~9,700/s (5 x ~290 records / ~150 ms). Each extra
 * producer adds a network thread, a connection and 5 more request slots. More producers do not weaken any
 * guarantee: every message is still confirmed with {@code acks=all} before the reply, keys (clientRequestIds) are
 * unique so no ordering is involved, and a resubmitted clientRequestId is deduplicated by the database, not by the
 * producer.
 *
 * <p>Settings can be changed while the backend runs, between load tests ({@link #apply}). The producers are then
 * replaced: the old ones send what they hold and close, and the next submissions create new ones. Changes last until
 * the backend restarts. {@code acks=all} and idempotence are fixed: they are what makes an accepted submission
 * certain to be stored.
 */
@Component
public class IntakeProducer {
    public static final Set<String> COMPRESSION = Set.of("none", "gzip", "snappy", "lz4", "zstd");
    public static final int MAX_PRODUCERS = 8;

    /**
     * Tunable settings. {@code maxInFlight} is per producer, capped at 5 by idempotence. {@code producers} may be
     * left out (a page loaded before the setting existed) and then means 1.
     */
    public record Settings(int lingerMs, int batchSizeBytes, String compression, int maxInFlight, Integer producers) {
        public Settings {
            if (producers == null) producers = 1;
        }

        void validate() {
            if (lingerMs < 0 || lingerMs > 1000) throw new IllegalArgumentException("linger.ms must be 0 to 1000");
            if (batchSizeBytes < 1024 || batchSizeBytes > 4 * 1024 * 1024)
                throw new IllegalArgumentException("batch.size must be 1 KB to 4 MB");
            if (compression == null || !COMPRESSION.contains(compression))
                throw new IllegalArgumentException("compression.type must be one of " + COMPRESSION);
            if (maxInFlight < 1 || maxInFlight > 5)
                throw new IllegalArgumentException("max.in.flight.requests.per.connection must be 1 to 5 (idempotence)");
            if (producers < 1 || producers > MAX_PRODUCERS)
                throw new IllegalArgumentException("producers must be 1 to " + MAX_PRODUCERS);
        }

        Map<String, Object> toConfig() {
            return Map.of(ProducerConfig.LINGER_MS_CONFIG, lingerMs,
                    ProducerConfig.BATCH_SIZE_CONFIG, batchSizeBytes,
                    ProducerConfig.COMPRESSION_TYPE_CONFIG, compression,
                    ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlight);
        }
    }

    /** Producer metrics shown next to the settings: where a submission's time goes. */
    private static final List<String> METRICS = List.of(
            "record-send-rate", "record-queue-time-avg", "record-queue-time-max", "request-latency-avg",
            "request-latency-max", "requests-in-flight", "records-per-request-avg", "batch-size-avg",
            "compression-rate-avg", "bufferpool-wait-ratio", "buffer-available-bytes", "waiting-threads");
    /** Combined over producers by adding up; "-max" metrics take the largest; the rest are averages. */
    private static final Set<String> SUMMED = Set.of("record-send-rate", "requests-in-flight", "buffer-available-bytes",
            "waiting-threads");

    private record Producer(DefaultKafkaProducerFactory<String, String> factory, KafkaTemplate<String, String> template) { }

    private final Map<String, Object> baseConfig;
    private final AtomicInteger sending = new AtomicInteger();
    private final AtomicLong next = new AtomicLong();
    private volatile List<Producer> producers;
    private volatile Settings settings;

    public IntakeProducer(ProducerFactory<String, String> base) {
        Map<String, Object> config = new HashMap<>(base.getConfigurationProperties());
        // Metric averages cover the last 2 windows; 5 s windows make them follow a run within ~10 s, not ~60 s.
        config.put(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, 5_000);
        this.settings = new Settings(
                intValue(config.get(ProducerConfig.LINGER_MS_CONFIG), 5),
                intValue(config.get(ProducerConfig.BATCH_SIZE_CONFIG), 16_384),
                String.valueOf(config.getOrDefault(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none")),
                intValue(config.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION), 5),
                1);
        config.putAll(settings.toConfig());
        this.baseConfig = config;
        this.producers = List.of(create(0));
    }

    private Producer create(int index) {
        var config = new HashMap<>(baseConfig);
        config.putAll(settings.toConfig());
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "backend-intake-" + (index + 1));
        var factory = new DefaultKafkaProducerFactory<String, String>(config);
        return new Producer(factory, new KafkaTemplate<>(factory));
    }

    private static int intValue(Object value, int fallback) {
        return value == null ? fallback : Integer.parseInt(value.toString());
    }

    /** The first intake producer's template (there is always at least one). */
    public KafkaTemplate<String, String> template() { return producers.getFirst().template(); }

    /** Every intake producer's template, in turn order (diagnostics and tests). */
    public List<KafkaTemplate<String, String>> templates() {
        return producers.stream().map(Producer::template).toList();
    }

    /** Sends through the next producer in turn and waits for Kafka's confirmation, counted as in progress meanwhile. */
    public SendResult<String, String> sendAndWait(String topic, String key, String value, long timeoutMs)
            throws InterruptedException, ExecutionException, TimeoutException {
        sending.incrementAndGet();
        try {
            List<Producer> current = producers;
            Producer producer = current.get((int) Math.floorMod(next.getAndIncrement(), (long) current.size()));
            CompletableFuture<SendResult<String, String>> future = producer.template().send(topic, key, value);
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } finally {
            sending.decrementAndGet();
        }
    }

    public Settings settings() { return settings; }

    /** Submissions waiting for Kafka's confirmation right now. */
    public int sending() { return sending.get(); }

    /**
     * Replaces the producers with ones using these settings, adding or removing producers to match the count.
     * Refused while submissions are being sent: changes are meant for between load tests.
     *
     * @throws IllegalArgumentException for an invalid setting
     * @throws IllegalStateException    while submissions are in progress
     */
    public synchronized Settings apply(Settings change) {
        change.validate();
        if (sending.get() > 0)
            throw new IllegalStateException("Submissions are being sent; change producer settings between runs");
        settings = change;
        List<Producer> current = producers;
        var updated = new ArrayList<Producer>();
        for (int i = 0; i < change.producers(); i++) {
            if (i < current.size()) {
                Producer kept = current.get(i);
                kept.factory().updateConfigs(change.toConfig());
                kept.factory().reset(); // closes its producer after it sends what it holds; the next send creates a new one
                updated.add(kept);
            } else {
                updated.add(create(i));
            }
        }
        producers = List.copyOf(updated);
        for (int i = change.producers(); i < current.size(); i++) current.get(i).factory().destroy();
        return change;
    }

    /**
     * Selected metrics, combined over the producers: rates, requests in flight and waiting threads added up,
     * maximums as the largest, averages weighted by each producer's send rate. Null where Kafka has no value yet.
     */
    public Map<String, Double> metrics() {
        List<Map<String, Double>> each = new ArrayList<>();
        for (Producer producer : producers) each.add(metricsOf(producer.template()));
        var combined = new LinkedHashMap<String, Double>();
        for (String name : METRICS) {
            double sum = 0, weighted = 0, weights = 0, max = Double.NEGATIVE_INFINITY;
            int seen = 0;
            for (Map<String, Double> values : each) {
                Double value = values.get(name);
                if (value == null) continue;
                seen++;
                sum += value;
                max = Math.max(max, value);
                double weight = values.getOrDefault("record-send-rate", null) == null ? 0 : values.get("record-send-rate");
                weighted += value * weight;
                weights += weight;
            }
            if (seen == 0) { combined.put(name, null); continue; }
            if (SUMMED.contains(name)) combined.put(name, sum);
            else if (name.endsWith("-max")) combined.put(name, max);
            else combined.put(name, weights > 0 ? weighted / weights : sum / seen);
        }
        return combined;
    }

    private static Map<String, Double> metricsOf(KafkaTemplate<String, String> template) {
        var values = new HashMap<String, Double>();
        try {
            template.metrics().forEach((metricName, metric) -> {
                if (!"producer-metrics".equals(metricName.group()) || !METRICS.contains(metricName.name())) return;
                if (metric.metricValue() instanceof Number number && Double.isFinite(number.doubleValue()))
                    values.put(metricName.name(), number.doubleValue());
            });
        } catch (RuntimeException ex) {
            // Kafka unreachable: no producer could be created. No values.
        }
        return values;
    }

    @PreDestroy
    void close() {
        for (Producer producer : producers) producer.factory().destroy();
    }
}
