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

    @Test
    void handlesLimitsAndTies() {
        QueryPopularityTracker tracker = new QueryPopularityTracker();
        tracker.recordObservation(C); tracker.recordObservation(B); tracker.recordObservation(A);
        assertThat(tracker.topByPopularity(0)).isEmpty();
        assertThat(tracker.topByPopularity(100)).containsExactlyInAnyOrder(A,B,C);
        assertThat(tracker.topByPopularity(2)).hasSize(2);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> tracker.topByPopularity(-1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(new QueryPopularityTracker().topByPopularity(Integer.MAX_VALUE)).isEmpty();
    }

    @Test
    void concurrentObservationsRankingAndResizingPreserveExactCounts() throws Exception {
        QueryPopularityTracker tracker = new QueryPopularityTracker();
        var executor = java.util.concurrent.Executors.newFixedThreadPool(9);
        var keys = new java.util.ArrayList<QueryHash>();
        for (int i = 0; i < 2_000; i++) keys.add(QueryHash.of(String.format("%032x",i)));
        try {
            var tasks = new java.util.ArrayList<java.util.concurrent.Callable<Void>>();
            for (int thread = 0; thread < 8; thread++) tasks.add(() -> {
                for (int round = 0; round < 10; round++) {
                    for (QueryHash key : keys) tracker.recordObservation(key);
                }
                return null;
            });
            tasks.add(() -> { for(int i=0;i<100;i++) {tracker.topByPopularity(10); tracker.distinctQueryCount();} return null; });
            for (var result : executor.invokeAll(tasks)) result.get();
            assertThat(tracker.distinctQueryCount()).isEqualTo(keys.size());
            for (QueryHash key : keys) assertThat(tracker.hitCount(key)).isEqualTo(80);
            assertThat(tracker.topByPopularity(10)).hasSize(10).allMatch(keys::contains);
        } finally { executor.shutdownNow(); }
    }

    @Test
    void boundedRankingMatchesFullOrderingForVariedCountsAndLimits() {
        var tracker = new QueryPopularityTracker();
        var expected = new java.util.ArrayList<QueryHash>();
        for (int i = 0; i < 256; i++) {
            QueryHash key = QueryHash.of(String.format("%032x",i));
            expected.add(key);
            for (int hit = 0; hit <= i; hit++) tracker.recordObservation(key);
        }
        java.util.Collections.reverse(expected);
        for (int limit : new int[]{0,1,10,64,256,512}) {
            assertThat(tracker.topByPopularity(limit)).containsExactlyElementsOf(expected.subList(0,Math.min(limit,256)));
        }
    }
}
