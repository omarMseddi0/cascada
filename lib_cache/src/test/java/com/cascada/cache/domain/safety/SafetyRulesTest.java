package com.cascada.cache.domain.safety;

import com.cascada.cache.domain.BypassReason;
import com.cascada.cache.domain.CacheDecision;
import com.cascada.cache.domain.CanonicalQueryObject;
import com.cascada.cache.domain.HashComponents;
import com.cascada.cache.domain.PostProcessing;
import com.cascada.cache.domain.QueryMetadata;
import com.cascada.cache.domain.TimeRange;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises each guardrail ported from {@code safety_rules.py} in isolation, then the
 * first-match-wins registry behaviour ported from {@code SafetyChecker}.
 */
class SafetyRulesTest {

    private static final long DAY = 86_400L;

    private CanonicalQueryObject query(List<String> groupBy, List<String> aggregates, List<String> filters,
                                       TimeRange timeRange, QueryMetadata metadata) {
        return CanonicalQueryObject.of(HashComponents.of(groupBy, aggregates, filters), timeRange,
                PostProcessing.none(), metadata);
    }

    @Nested
    class RequiresAggregation {

        @Test
        void bypassesAPlainRowFetchSelectWithNoAggregate() {
            // README caveat 7: without this rule a raw SELECT passed every guardrail and the merge
            // fabricated a GROUP BY + SUM the user never wrote.
            CanonicalQueryObject query = query(List.of(), List.of(),
                    List.of("appName = 'netflix'"), new TimeRange(0, DAY - 1), QueryMetadata.globalAggregate());
            assertThat(new RequiresAggregationRule().evaluate(query, CacheConfiguration.defaults()))
                    .contains(BypassReason.NO_AGGREGATION);
        }

        @Test
        void allowsAnyQueryWithAtLeastOneAggregate() {
            CanonicalQueryObject query = query(List.of("appName"), List.of("SUM(bytes)"),
                    List.of(), new TimeRange(0, DAY - 1), QueryMetadata.globalAggregate());
            assertThat(new RequiresAggregationRule().evaluate(query, CacheConfiguration.defaults())).isEmpty();
        }

        @Test
        void defaultRegistryBypassesAggregatelessQueriesFirst() {
            CanonicalQueryObject query = query(List.of(), List.of(), List.of(),
                    new TimeRange(0, 2 * DAY - 1), QueryMetadata.globalAggregate());
            assertThat(SafetyRuleRegistry.defaultRegistry().findFirstBypassReason(query,
                    CacheConfiguration.defaults())).contains(BypassReason.NO_AGGREGATION);
        }
    }

    @Nested
    class NonMergeableSqlFeature {

        private CanonicalQueryObject withSignature(List<String> logicSignature) {
            return new CanonicalQueryObject(
                    HashComponents.of(List.of("appName"), List.of("SUM(bytes)"), List.of()),
                    new TimeRange(0, 2 * DAY - 1), PostProcessing.none(), QueryMetadata.globalAggregate(),
                    "", List.of(), List.of(), logicSignature);
        }

        @Test
        void bypassesDistinct() {
            // DISTINCT over two buckets is not the union of each bucket's DISTINCT — not mergeable.
            assertThat(new NonMergeableSqlFeatureRule().evaluate(withSignature(List.of("DISTINCT")),
                    CacheConfiguration.defaults())).contains(BypassReason.NON_MERGEABLE_SQL_FEATURE);
        }

        @Test
        void bypassesHaving() {
            // HAVING filters the FINAL aggregate; applying it to per-bucket partials filters wrong rows.
            assertThat(new NonMergeableSqlFeatureRule().evaluate(
                    withSignature(List.of("HAVING SUM(bytes) > 100")),
                    CacheConfiguration.defaults())).contains(BypassReason.NON_MERGEABLE_SQL_FEATURE);
        }

        @Test
        void bypassesJoinOn() {
            // A per-bucket join misses pairs whose rows fall in different buckets.
            assertThat(new NonMergeableSqlFeatureRule().evaluate(
                    withSignature(List.of("JOIN ON t.userId = d.userId")),
                    CacheConfiguration.defaults())).contains(BypassReason.NON_MERGEABLE_SQL_FEATURE);
        }

        @Test
        void allowsAQueryWithNoLogicSignature() {
            assertThat(new NonMergeableSqlFeatureRule().evaluate(withSignature(List.of()),
                    CacheConfiguration.defaults())).isEmpty();
        }

        @Test
        void defaultRegistryBypassesTheseBeforeAnyOtherOpinion() {
            assertThat(SafetyRuleRegistry.defaultRegistry().findFirstBypassReason(
                    withSignature(List.of("DISTINCT")), CacheConfiguration.defaults()))
                    .contains(BypassReason.NON_MERGEABLE_SQL_FEATURE);
        }
    }

    @Nested
    class ImpossibleMath {

        @Test
        void bypassesCountDistinctCaseInsensitively() {
            CanonicalQueryObject query = query(List.of("appName"), List.of("count(distinct subscriberId)"),
                    List.of(), new TimeRange(0, DAY - 1), QueryMetadata.globalAggregate());
            assertThat(new ImpossibleMathRule().evaluate(query, CacheConfiguration.defaults()))
                    .contains(BypassReason.IMPOSSIBLE_MATH);
        }

        @Test
        void allowsPlainSum() {
            CanonicalQueryObject query = query(List.of("appName"), List.of("SUM(bytes)"),
                    List.of(), new TimeRange(0, DAY - 1), QueryMetadata.globalAggregate());
            assertThat(new ImpossibleMathRule().evaluate(query, CacheConfiguration.defaults())).isEmpty();
        }
    }

    @Nested
    class HighCardinalityGroupBy {

        @Test
        void bypassesWhenGroupByTouchesAProfiledHighCardinalityColumn() {
            CacheConfiguration configuration =
                    CacheConfiguration.defaults().withHighCardinalityColumns(Set.of("subscriberId"));
            CanonicalQueryObject query = query(List.of("subscriberId"), List.of("SUM(bytes)"),
                    List.of(), new TimeRange(0, DAY - 1), QueryMetadata.globalAggregate());
            assertThat(new HighCardinalityGroupByRule().evaluate(query, configuration))
                    .contains(BypassReason.HIGH_CARDINALITY_GROUP_BY);
        }

        @Test
        void allowsGroupByOnALowCardinalityColumn() {
            CacheConfiguration configuration =
                    CacheConfiguration.defaults().withHighCardinalityColumns(Set.of("subscriberId"));
            CanonicalQueryObject query = query(List.of("appName"), List.of("SUM(bytes)"),
                    List.of(), new TimeRange(0, DAY - 1), QueryMetadata.globalAggregate());
            assertThat(new HighCardinalityGroupByRule().evaluate(query, configuration)).isEmpty();
        }
    }

    @Nested
    class LiquidClusteredFilter {

        @Test
        void bypassesWhenFilteringOnAClusteredColumn() {
            CacheConfiguration configuration =
                    CacheConfiguration.defaults().withLiquidClusteredFilterColumns(Set.of("subscriberId"));
            CanonicalQueryObject query = query(List.of("appName"), List.of("SUM(bytes)"),
                    List.of("subscriberId = '42'"), new TimeRange(0, DAY - 1), QueryMetadata.globalAggregate());
            assertThat(new LiquidClusteredFilterRule().evaluate(query, configuration))
                    .contains(BypassReason.LIQUID_CLUSTERED_FILTER);
        }
    }

    @Nested
    class IncompatibleTimeStep {

        @Test
        void allowsUserStepThatIsAMultipleOfFixedStep() {
            CanonicalQueryObject query = query(List.of("appName"), List.of("SUM(bytes)"), List.of(),
                    new TimeRange(0, DAY - 1), QueryMetadata.timeSeries(600));
            assertThat(new IncompatibleTimeStepRule().evaluate(query, CacheConfiguration.defaults())).isEmpty();
        }

        @Test
        void bypassesUserStepFinerThanFixedStep() {
            CanonicalQueryObject query = query(List.of("appName"), List.of("SUM(bytes)"), List.of(),
                    new TimeRange(0, DAY - 1), QueryMetadata.timeSeries(60));
            assertThat(new IncompatibleTimeStepRule().evaluate(query, CacheConfiguration.defaults()))
                    .contains(BypassReason.INCOMPATIBLE_TIME_STEP);
        }

        @Test
        void bypassesUserStepThatIsNotAMultiple() {
            CanonicalQueryObject query = query(List.of("appName"), List.of("SUM(bytes)"), List.of(),
                    new TimeRange(0, DAY - 1), QueryMetadata.timeSeries(700));
            assertThat(new IncompatibleTimeStepRule().evaluate(query, CacheConfiguration.defaults()))
                    .contains(BypassReason.INCOMPATIBLE_TIME_STEP);
        }

        @Test
        void bypassesMissingStepUnlessPreservingRawSeries() {
            QueryMetadata noStep = new QueryMetadata(true, 0L, List.of(), java.util.Map.of(),
                    Optional.empty(), false, List.of());
            CanonicalQueryObject mustBypass = query(List.of("appName"), List.of("SUM(bytes)"), List.of(),
                    new TimeRange(0, DAY - 1), noStep);
            assertThat(new IncompatibleTimeStepRule().evaluate(mustBypass, CacheConfiguration.defaults()))
                    .contains(BypassReason.INCOMPATIBLE_TIME_STEP);

            QueryMetadata preserveRaw = new QueryMetadata(true, 0L, List.of(), java.util.Map.of(),
                    Optional.empty(), true, List.of());
            CanonicalQueryObject allowed = query(List.of("appName"), List.of("SUM(bytes)"), List.of(),
                    new TimeRange(0, DAY - 1), preserveRaw);
            assertThat(new IncompatibleTimeStepRule().evaluate(allowed, CacheConfiguration.defaults())).isEmpty();
        }

        @Test
        void hasNoOpinionOnGlobalAggregates() {
            CanonicalQueryObject query = query(List.of("appName"), List.of("SUM(bytes)"), List.of(),
                    new TimeRange(0, DAY - 1), QueryMetadata.globalAggregate());
            assertThat(new IncompatibleTimeStepRule().evaluate(query, CacheConfiguration.defaults())).isEmpty();
        }
    }

    @Nested
    class MinimumTimeRange {

        @Test
        void zeroOrMinusOneDisablesCaching() {
            CanonicalQueryObject query = query(List.of("appName"), List.of("SUM(bytes)"), List.of(),
                    new TimeRange(0, 7 * DAY), QueryMetadata.globalAggregate());
            assertThat(new MinimumTimeRangeRule().evaluate(query,
                    CacheConfiguration.defaults().withMinimumCacheableTimeRangeSeconds(0)))
                    .contains(BypassReason.MINIMUM_TIME_RANGE);
            assertThat(new MinimumTimeRangeRule().evaluate(query,
                    CacheConfiguration.defaults().withMinimumCacheableTimeRangeSeconds(-1)))
                    .contains(BypassReason.MINIMUM_TIME_RANGE);
        }

        @Test
        void bypassesQueriesShorterThanThePositiveMinimum() {
            CanonicalQueryObject shortQuery = query(List.of("appName"), List.of("SUM(bytes)"), List.of(),
                    new TimeRange(0, 100), QueryMetadata.globalAggregate());
            assertThat(new MinimumTimeRangeRule().evaluate(shortQuery,
                    CacheConfiguration.defaults().withMinimumCacheableTimeRangeSeconds(DAY)))
                    .contains(BypassReason.MINIMUM_TIME_RANGE);
        }

        @Test
        void allowsQueriesAtOrAboveTheMinimumAndWhenUnset() {
            CanonicalQueryObject longQuery = query(List.of("appName"), List.of("SUM(bytes)"), List.of(),
                    new TimeRange(0, 2 * DAY), QueryMetadata.globalAggregate());
            assertThat(new MinimumTimeRangeRule().evaluate(longQuery,
                    CacheConfiguration.defaults().withMinimumCacheableTimeRangeSeconds(DAY))).isEmpty();
            assertThat(new MinimumTimeRangeRule().evaluate(longQuery, CacheConfiguration.defaults())).isEmpty();
        }
    }

    @Nested
    class PartialDayBucket {

        @Test
        void bypassesASinglePartialDay() {
            CanonicalQueryObject query = query(List.of("appName"), List.of("SUM(bytes)"), List.of(),
                    new TimeRange(100, 200), QueryMetadata.globalAggregate());
            assertThat(new PartialDayBucketRule().evaluate(query, CacheConfiguration.defaults()))
                    .contains(BypassReason.PARTIAL_DAY_BUCKET);
        }

        @Test
        void allowsAWholeDay() {
            CanonicalQueryObject query = query(List.of("appName"), List.of("SUM(bytes)"), List.of(),
                    new TimeRange(0, DAY - 1), QueryMetadata.globalAggregate());
            assertThat(new PartialDayBucketRule().evaluate(query, CacheConfiguration.defaults())).isEmpty();
        }

        @Test
        void allowsAMultiDayRange() {
            CanonicalQueryObject query = query(List.of("appName"), List.of("SUM(bytes)"), List.of(),
                    new TimeRange(100, DAY + 200), QueryMetadata.globalAggregate());
            assertThat(new PartialDayBucketRule().evaluate(query, CacheConfiguration.defaults())).isEmpty();
        }
    }

    @Nested
    class Registry {

        @Test
        void defaultRegistryAllowsASafeWholeDayQuery() {
            CanonicalQueryObject query = query(List.of("appName"), List.of("SUM(bytes)"), List.of(),
                    new TimeRange(0, 2 * DAY - 1), QueryMetadata.timeSeries(600));
            assertThat(SafetyRuleRegistry.defaultRegistry().evaluate(query, CacheConfiguration.defaults()))
                    .isEqualTo(CacheDecision.AGGREGATION_V1);
        }

        @Test
        void firstMatchingRuleWins() {
            CacheConfiguration configuration =
                    CacheConfiguration.defaults().withHighCardinalityColumns(Set.of("subscriberId"));
            // both impossible-math and high-cardinality would fire; impossible-math is earlier.
            CanonicalQueryObject query = query(List.of("subscriberId"), List.of("COUNT(DISTINCT x)"), List.of(),
                    new TimeRange(0, 2 * DAY - 1), QueryMetadata.globalAggregate());
            assertThat(SafetyRuleRegistry.defaultRegistry().findFirstBypassReason(query, configuration))
                    .contains(BypassReason.IMPOSSIBLE_MATH);
        }

        @Test
        void defaultRegistryHasNoHotViewConceptAndCachesRegardlessOfStalenessTolerance() {
            // The customer never picks "hot view"/"hot batch"; a batch-tolerant query still caches.
            QueryMetadata batchTolerant = QueryMetadata.timeSeries(600).withStalenessToleranceMillis(3_600_000L);
            CanonicalQueryObject query = query(List.of("appName"), List.of("SUM(bytes)"), List.of(),
                    new TimeRange(0, 2 * DAY - 1), batchTolerant);
            assertThat(SafetyRuleRegistry.defaultRegistry().evaluate(query, CacheConfiguration.defaults()))
                    .isEqualTo(CacheDecision.AGGREGATION_V1);
        }
    }
}
