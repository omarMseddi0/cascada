package com.cascada.cache.warming;

import com.cascada.cache.adapter.tracking.QueryPopularityTracker;
import com.cascada.cache.domain.warming.WarmingQueue;
import com.cascada.identity.domain.QueryHash;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the Layer-1 warming queue (vote/dedup/drain) and the popularity tracker (Layer-2 feed).
 */
class WarmingAndTrackingTest {

    private static final QueryHash A = QueryHash.of("00000000000000000000000000000001");
    private static final QueryHash B = QueryHash.of("00000000000000000000000000000002");
    private static final QueryHash C = QueryHash.of("00000000000000000000000000000003");

    @Test
    void warmingQueueDeduplicatesVotesAndDrainsInInsertionOrder() {
        WarmingQueue queue = new WarmingQueue();
        queue.vote(A);
        queue.vote(B);
        queue.vote(A); // duplicate — collapses
        assertThat(queue.size()).isEqualTo(2);

        List<QueryHash> drained = queue.consumeAll();
        assertThat(drained).containsExactly(A, B);
        assertThat(queue.size()).isZero(); // drained and cleared
    }

    @Test
    void popularityTrackerRanksByCumulativeHits() {
        QueryPopularityTracker tracker = new QueryPopularityTracker();
        tracker.recordObservation(A);
        tracker.recordObservation(B);
        tracker.recordObservation(B);
        tracker.recordObservation(B);
        tracker.recordObservation(C);
        tracker.recordObservation(C);

        assertThat(tracker.hitCount(B)).isEqualTo(3);
        assertThat(tracker.topByPopularity(2)).containsExactly(B, C);
        assertThat(tracker.distinctQueryCount()).isEqualTo(3);
    }
}
