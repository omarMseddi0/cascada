package com.cascada.cache.adapter.out.tracking;

import com.cascada.identity.domain.QueryHash;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class QueryPopularityTrackerTest {

    private static final QueryHash A = QueryHash.of("00000000000000000000000000000001");
    private static final QueryHash B = QueryHash.of("00000000000000000000000000000002");
    private static final QueryHash C = QueryHash.of("00000000000000000000000000000003");

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
