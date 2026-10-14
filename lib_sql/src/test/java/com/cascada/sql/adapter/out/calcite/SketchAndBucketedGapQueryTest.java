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
    void unaliasedSketchUsesSparksNameAndExactDistinctStaysUnmergeable() {
        CanonicalQueryObject unaliased = canonicalizer.canonicalize("SELECT APPROX_COUNT_DISTINCT(user_id) FROM events "
                + "WHERE ts >= 0 AND ts <= 172799");
        assertThat(unaliased.logicSignature()).isEmpty();
        assertThat(unaliased.metadata().measureAggregates())
                .containsEntry("approx_count_distinct(user_id)", AggregateFunction.DISTINCT_SKETCH);
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
    void bucketedGapGroupsByEpochAlignedBucketStart() {
        Optional<String> bucketed = builder.buildBucketedGapQuery(
                "SELECT CAST(FLOOR(ts / 3600) * 3600 AS BIGINT) AS ts, SUM(amount) AS total FROM events "
                        + "WHERE ts >= 100 AND ts <= 259199 AND region = 'a' "
                        + "GROUP BY CAST(FLOOR(ts / 3600) * 3600 AS BIGINT)",
                new GapPlan(Optional.of(new com.cascada.cache.domain.time.TimeRange(100, DAY - 1)),
                        List.of(DAY, 2 * DAY), Optional.empty()),
                "cascada_bucket_start");

        assertThat(bucketed).isPresent();
        assertThat(bucketed.get())
                .contains("CAST(ts - PMOD(ts, 86400) AS BIGINT) cascada_bucket_start")
                .contains("GROUP BY CAST(FLOOR(ts / 3600) * 3600 AS BIGINT), CAST(ts - PMOD(ts, 86400) AS BIGINT)")
                .contains("region = 'a'")
                .contains("(ts >= 100 AND ts <= 259199)");
    }

    @Test
    void globalAggregateGainsABucketGroup() {
        Optional<String> bucketed = builder.buildBucketedGapQuery(
                "SELECT COUNT(*) AS n FROM events WHERE ts >= 0 AND ts <= 172799",
                new GapPlan(Optional.empty(), List.of(0L, DAY), Optional.empty()), "cascada_bucket_start");

        assertThat(bucketed.orElseThrow()).endsWith("GROUP BY CAST(ts - PMOD(ts, 86400) AS BIGINT)");
    }

    @Test
    void bucketLabelThatShadowsAnOutputColumnIsRefused() {
        assertThat(builder.buildBucketedGapQuery("SELECT COUNT(*) AS cascada_bucket_start FROM events "
                        + "WHERE ts >= 0 AND ts <= 86399",
                new GapPlan(Optional.empty(), List.of(0L), Optional.empty()), "cascada_bucket_start")).isEmpty();
    }

    @Test
    void sketchWithAQuantifierIsRejected() {
        assertThatThrownBy(() -> builder.buildGapQuery("SELECT APPROX_COUNT_DISTINCT(user_id, 0.01) AS users FROM events "
                        + "WHERE ts >= 0 AND ts <= 86399",
                new GapPlan(Optional.empty(), List.of(0L), Optional.empty())))
                .isInstanceOf(UnsupportedSqlException.class);
    }

    @Test
    void aliasedAverageIsCachedAsSumAndCountIngredients() {
        CanonicalQueryObject canonical = canonicalizer.canonicalize(
                "SELECT region, AVG(latency) AS mean FROM events WHERE ts >= 0 AND ts <= 172799 GROUP BY region");

        assertThat(canonical.logicSignature()).isEmpty();
        assertThat(canonical.metadata().measureAggregates()).containsEntry("mean", AggregateFunction.AVERAGE);
        assertThat(canonical.hashComponents().aggregates()).contains("SUM(latency)", "COUNT(latency)");
        CanonicalQueryObject unaliased = canonicalizer.canonicalize(
                "SELECT AVG(latency) FROM events WHERE ts >= 0 AND ts <= 172799");
        assertThat(unaliased.logicSignature()).isEmpty();
        assertThat(unaliased.metadata().measureAggregates())
                .as("an unaliased AVG keeps Spark's own column name").containsEntry("avg(latency)", AggregateFunction.AVERAGE);
    }

    @Test
    void expressionOverAggregatesIsStoredAsNumberedTerms() {
        CanonicalQueryObject canonical = canonicalizer.canonicalize("SELECT region, SUM(amount) / COUNT(*) AS ratio, "
                + "MAX(amount) - MIN(amount) FROM events WHERE ts >= 0 AND ts <= 172799 GROUP BY region");

        assertThat(canonical.logicSignature()).isEmpty();
        assertThat(canonical.metadata().measureAggregates())
                .containsEntry("ratio", AggregateFunction.COMPOSITE)
                .containsEntry("ratio__cascada_term0", AggregateFunction.SUM)
                .containsEntry("ratio__cascada_term1", AggregateFunction.COUNT)
                .containsEntry("(max(amount) - min(amount))", AggregateFunction.COMPOSITE);
        assertThat(canonical.metadata().compositeAliases().get("ratio"))
                .isEqualTo("ratio__cascada_term0 / ratio__cascada_term1");

        String gap = builder.buildGapQuery(canonical.physicalSql(), new GapPlan(Optional.empty(), List.of(0L), Optional.empty()));
        assertThat(gap).contains("SUM(amount) ratio__cascada_term0")
                .contains("TYPEOF(SUM(amount)) ratio__cascada_term0__cascada_type")
                .contains("COUNT(*) ratio__cascada_term1")
                .contains("MAX(amount) `(max(amount) - min(amount))__cascada_term0`");
    }

    @Test
    void expressionReadingAColumnOutsideItsAggregatesIsNotCached() {
        assertThat(canonicalizer.canonicalize("SELECT region, SUM(amount) / MAX(price) + price AS odd FROM events "
                + "WHERE ts >= 0 AND ts <= 172799 GROUP BY region").logicSignature()).contains("UNSUPPORTED_MERGE_SHAPE");
    }

    @Test
    void gapQueryFetchesAverageIngredientsInsteadOfPerBucketAverages() {
        String gap = builder.buildGapQuery("SELECT region, AVG(latency) AS mean, SUM(amount) AS total FROM events "
                        + "WHERE ts >= 0 AND ts <= 172799 GROUP BY region ORDER BY mean DESC",
                new GapPlan(Optional.empty(), List.of(DAY), Optional.empty()));

        assertThat(gap).contains("SUM(latency) mean__cascada_avg_sum")
                .contains("COUNT(latency) mean__cascada_avg_count")
                .containsIgnoringCase("MIN(TYPEOF(latency)) mean__cascada_avg_type")
                .contains("SUM(amount) total")
                .doesNotContainIgnoringCase("AVG(");
    }

    @Test
    void approximateDistinctCountPrecisionIsPartOfTheCacheIdentity() {
        CanonicalQueryObject canonical = canonicalizer.canonicalize(
                "SELECT APPROX_COUNT_DISTINCT(user_id) AS users FROM events WHERE ts >= 0 AND ts <= 172799");
        assertThat(canonical.hashComponents().aggregates()).contains("HLL_SKETCH_LG_CONFIG_K=20");
    }
}
