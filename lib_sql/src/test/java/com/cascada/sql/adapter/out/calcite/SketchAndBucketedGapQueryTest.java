package com.cascada.sql.adapter.out.calcite;

import com.cascada.cache.domain.merge.AggregateFunction;
import com.cascada.cache.domain.query.CanonicalQueryObject;
import com.cascada.cache.domain.time.GapPlan;
import com.cascada.sql.exception.UnsupportedSqlException;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** APPROX_COUNT_DISTINCT canonicalization, sketch gap rewriting, and bucket-labelled gap queries. */
class SketchAndBucketedGapQueryTest {

    private static final long DAY = 86_400L;
    private final GapQueryBuilder builder = new GapQueryBuilder("ts", DAY);
    private final CalciteCanonicalObjectFactory canonicalizer = new CalciteCanonicalObjectFactory();

    @Test
    void aliasedApproximateDistinctCountIsCacheableAsSketchMeasure() {
        CanonicalQueryObject canonical = canonicalizer.canonicalize(
                "SELECT region, APPROX_COUNT_DISTINCT(user_id) AS users FROM events "
                        + "WHERE ts >= 0 AND ts <= 172799 GROUP BY region");

        assertThat(canonical.logicSignature()).isEmpty();
        assertThat(canonical.metadata().measureAggregates()).containsEntry("users", AggregateFunction.DISTINCT_SKETCH);
        assertThat(canonical.hashComponents().aggregates()).anyMatch(spec -> spec.startsWith("APPROX_COUNT_DISTINCT("));
    }

    @Test
    void unaliasedOrExactDistinctCountsStayUnmergeable() {
        assertThat(canonicalizer.canonicalize("SELECT APPROX_COUNT_DISTINCT(user_id) FROM events "
                + "WHERE ts >= 0 AND ts <= 172799").logicSignature()).contains("UNSUPPORTED_MERGE_SHAPE");
        assertThat(canonicalizer.canonicalize("SELECT COUNT(DISTINCT user_id) AS users FROM events "
                + "WHERE ts >= 0 AND ts <= 172799").logicSignature()).contains("UNSUPPORTED_MERGE_SHAPE");
    }

    @Test
    void gapQueryComputesAnHllSketchUnderTheSameAlias() {
        String gap = builder.buildGapQuery("SELECT region, APPROX_COUNT_DISTINCT(user_id) AS users FROM events "
                        + "WHERE ts >= 0 AND ts <= 172799 GROUP BY region ORDER BY users DESC LIMIT 5",
                new GapPlan(Optional.empty(), List.of(DAY), Optional.empty()));

        assertThat(gap).containsIgnoringCase("BASE64(HLL_SKETCH_AGG(CONCAT('v', CAST(user_id AS STRING)), 20)) users")
                .contains("ts >= 86400 AND ts <= 172799")
                .doesNotContainIgnoringCase("APPROX_COUNT_DISTINCT")
                .doesNotContainIgnoringCase("LIMIT");
    }

    @Test
    void sketchWithAQuantifierIsRejected() {
        assertThatThrownBy(() -> builder.buildGapQuery("SELECT APPROX_COUNT_DISTINCT(user_id, 0.01) AS users FROM events "
                        + "WHERE ts >= 0 AND ts <= 86399",
                new GapPlan(Optional.empty(), List.of(0L), Optional.empty())))
                .isInstanceOf(UnsupportedSqlException.class);
    }

    @Test
    void approximateDistinctCountPrecisionIsPartOfTheCacheIdentity() {
        CanonicalQueryObject canonical = canonicalizer.canonicalize(
                "SELECT APPROX_COUNT_DISTINCT(user_id) AS users FROM events WHERE ts >= 0 AND ts <= 172799");
        assertThat(canonical.hashComponents().aggregates()).contains("HLL_SKETCH_LG_CONFIG_K=20");
    }
}
