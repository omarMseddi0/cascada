package com.cascada.cache.adapter.tracking;

import com.cascada.identity.domain.QueryHash;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** The predictor's memory caps (plan Appendix J.7 #4): constant-size under unbounded workload drift. */
class MarkovPredictorBoundsTest {

    private QueryHash hash(int seed) {
        return new QueryHash(String.format("%032d", seed));
    }

    @Test
    void sourceCountNeverExceedsTheCapUnderDrift() {
        MarkovNextQueryPredictor predictor = new MarkovNextQueryPredictor(10, 4);
        for (int source = 0; source < 100; source++) {
            predictor.recordTransition(hash(source), hash(source + 1));
        }
        assertThat(predictor.trackedSourceCount()).isEqualTo(10);
    }

    @Test
    void heavyHitterFollowersSurviveTheFollowerCap() {
        MarkovNextQueryPredictor predictor = new MarkovNextQueryPredictor(10, 2);
        QueryHash parent = hash(1);
        // One clearly dominant follower...
        for (int i = 0; i < 10; i++) {
            predictor.recordTransition(parent, hash(100));
        }
        predictor.recordTransition(parent, hash(200));
        // ...then a stream of one-off noise that must not displace the heavy hitter.
        for (int noise = 300; noise < 320; noise++) {
            predictor.recordTransition(parent, hash(noise));
        }
        assertThat(predictor.predictNext(parent, 1)).containsExactly(hash(100));
        assertThat(predictor.transitionProbability(parent, hash(100))).isGreaterThan(0.0);
    }

    @Test
    void recentlyUsedSourcesSurviveLeastRecentlyUsedEviction() {
        MarkovNextQueryPredictor predictor = new MarkovNextQueryPredictor(3, 4);
        predictor.recordTransition(hash(1), hash(10));
        predictor.recordTransition(hash(2), hash(20));
        predictor.recordTransition(hash(3), hash(30));
        predictor.predictNext(hash(1), 1); // touch source 1 -> most recently used
        predictor.recordTransition(hash(4), hash(40)); // evicts the LRU source (2)

        assertThat(predictor.predictNext(hash(1), 1)).containsExactly(hash(10));
        assertThat(predictor.predictNext(hash(2), 1)).isEmpty();
        assertThat(predictor.trackedSourceCount()).isEqualTo(3);
    }
}
