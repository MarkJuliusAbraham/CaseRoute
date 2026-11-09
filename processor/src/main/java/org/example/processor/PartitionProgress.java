package org.example.processor;

import java.util.TreeSet;

/**
 * Tracks which messages of one partition are still being processed, so Kafka's position can be committed safely
 * while cases finish in any order.
 *
 * <p>Kafka stores one committed offset per partition, meaning "everything before this is done". So the offset that
 * can be committed is the oldest message still in progress; if nothing is in progress, it is just past the last
 * message fetched. A slow case therefore holds back only the commit, never fetching or other cases. After a crash,
 * everything from the committed offset is redelivered, so no unfinished case is skipped (finished ones after it
 * may be processed again; downstream updates are idempotent).
 */
public final class PartitionProgress {

    private final TreeSet<Long> inProgress = new TreeSet<>();
    private long nextToFetch;
    private long lastCommitted;

    /** @param position the partition position at assignment: the offset of the next message to be fetched */
    public PartitionProgress(long position) {
        this.nextToFetch = position;
        this.lastCommitted = position;
    }

    public synchronized void started(long offset) {
        inProgress.add(offset);
        nextToFetch = Math.max(nextToFetch, offset + 1);
    }

    public synchronized void finished(long offset) {
        inProgress.remove(offset);
    }

    /** The offset to commit now, or -1 if it has not moved since the last commit. */
    public synchronized long commitCandidate() {
        long committable = inProgress.isEmpty() ? nextToFetch : inProgress.first();
        if (committable <= lastCommitted) return -1;
        lastCommitted = committable;
        return committable;
    }

    public synchronized int inProgressCount() { return inProgress.size(); }
}
