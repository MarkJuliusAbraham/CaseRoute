package org.example.processor;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PartitionProgressTests {

    @Test void commitStopsAtTheOldestCaseStillInProgress() {
        var progress = new PartitionProgress(100);
        for (long offset = 100; offset < 110; offset++) progress.started(offset);
        // Everything except 100 (a slow case) finishes first.
        for (long offset = 101; offset < 110; offset++) progress.finished(offset);
        assertEquals(-1, progress.commitCandidate(), "100 is unfinished, so nothing can be committed yet");

        progress.finished(100);
        assertEquals(110, progress.commitCandidate(), "once 100 is done, everything up to 110 is");
        assertEquals(-1, progress.commitCandidate(), "no change, nothing new to commit");
    }

    @Test void commitAdvancesPartiallyPastFinishedCases() {
        var progress = new PartitionProgress(0);
        for (long offset = 0; offset < 5; offset++) progress.started(offset);
        progress.finished(0);
        progress.finished(1);
        progress.finished(3);
        assertEquals(2, progress.commitCandidate(), "0 and 1 are done; 2 is the oldest still running");
        assertEquals(2, progress.inProgressCount());
    }

    @Test void gapsInOffsetsDoNotBlockCommits() {
        var progress = new PartitionProgress(10);
        progress.started(10);
        progress.started(13); // offsets 11-12 never arrive (e.g. control records)
        progress.finished(13);
        progress.finished(10);
        assertEquals(14, progress.commitCandidate());
    }
}
