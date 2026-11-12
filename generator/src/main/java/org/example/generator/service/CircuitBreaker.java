package org.example.generator.service;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleSupplier;

/**
 * Stops the generator from sending at full rate into a backend that is not there, and from hitting it with a wall of
 * traffic the moment it comes back. The schedule keeps running (the generator is open-loop); submissions due while
 * sending is paused are skipped with the reason, and the retry period can send them again later.
 *
 * <ul>
 *   <li><b>CLOSED</b>: normal. {@link #FAILURES_TO_OPEN} replies in a row that never reached the backend (connection
 *       refused, broken, or timed out: no HTTP status at all) open it. Any HTTP reply, even an error, resets the count.</li>
 *   <li><b>OPEN</b>: nothing is sent for a while (1 s at first, doubling up to 10 s while the backend stays away).</li>
 *   <li><b>PROBING</b>: one submission is sent as a probe; everything else waits for its answer. An HTTP reply means
 *       the backend is back; another failure opens it again for longer.</li>
 *   <li><b>RECOVERING</b>: the share of submissions sent grows from 0 to 100% over {@link #RAMP_NANOS}, so the
 *       returning backend gets a ramp instead of every connection and request at once.</li>
 * </ul>
 * Times are {@link System#nanoTime()} values passed in by the caller, so tests can drive it.
 */
public final class CircuitBreaker {
    public enum State { CLOSED, OPEN, PROBING, RECOVERING }

    public static final int FAILURES_TO_OPEN = 20;
    static final long FIRST_PAUSE_NANOS = 1_000_000_000L;
    static final long MAX_PAUSE_NANOS = 10_000_000_000L;
    static final long RAMP_NANOS = 5_000_000_000L;

    /** What the run reports: the state, since when, how often it opened and how many submissions it held back. */
    public record Status(State state, Instant since, long opened, long heldBack) { }

    private final DoubleSupplier random;
    private final AtomicInteger failuresInARow = new AtomicInteger();
    private volatile State state = State.CLOSED;
    private long pauseNanos = FIRST_PAUSE_NANOS;   // guarded by this
    private long openUntil, rampStart;            // guarded by this
    private long opened, heldBack;                // guarded by this
    private Instant since = Instant.now();        // guarded by this

    public CircuitBreaker() { this(() -> java.util.concurrent.ThreadLocalRandom.current().nextDouble()); }

    CircuitBreaker(DoubleSupplier random) { this.random = random; }

    public State state() { return state; }

    /** Null if the submission may be sent now, else why it is held back. */
    public String admit(long now) {
        if (state == State.CLOSED) return null; // the common case, without a lock
        synchronized (this) {
            String reason = switch (state) {
                case CLOSED -> null;
                case OPEN -> {
                    if (now < openUntil) yield "Backend unreachable: sending paused";
                    move(State.PROBING);
                    yield null; // this submission is the probe
                }
                case PROBING -> "Backend unreachable: waiting for the probe's answer";
                case RECOVERING -> {
                    double share = (now - rampStart) / (double) RAMP_NANOS;
                    if (share >= 1) { move(State.CLOSED); yield null; }
                    yield random.getAsDouble() < share ? null : "Backend back: ramping up";
                }
            };
            if (reason != null) heldBack++;
            return reason;
        }
    }

    /** The submission admitted as a probe could not be sent after all (for example, the in-flight cap): probe again. */
    public synchronized void probeNotSent(long now) {
        if (state == State.PROBING) { move(State.OPEN); openUntil = now; }
    }

    /** Called with every reply: {@code unreachable} when it never reached the backend (no HTTP status). */
    public void onResult(boolean unreachable, long now) {
        if (!unreachable) {
            if (failuresInARow.get() != 0) failuresInARow.set(0);
            if (state == State.PROBING) synchronized (this) {
                if (state == State.PROBING) { move(State.RECOVERING); rampStart = now; pauseNanos = FIRST_PAUSE_NANOS; }
            }
            return;
        }
        int inARow = failuresInARow.incrementAndGet();
        synchronized (this) {
            if (state == State.PROBING) {
                pauseNanos = Math.min(pauseNanos * 2, MAX_PAUSE_NANOS);
                move(State.OPEN);
                openUntil = now + pauseNanos;
            } else if ((state == State.CLOSED || state == State.RECOVERING) && inARow >= FAILURES_TO_OPEN) {
                opened++;
                move(State.OPEN);
                openUntil = now + pauseNanos;
            }
        }
    }

    public synchronized Status status() { return new Status(state, since, opened, heldBack); }

    private void move(State next) {
        state = next;
        since = Instant.now();
    }
}
