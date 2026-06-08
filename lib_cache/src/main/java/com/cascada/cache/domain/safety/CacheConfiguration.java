package com.cascada.cache.domain.safety;

import com.cascada.cache.domain.CacheConstants;

import java.util.Optional;
import java.util.Set;

/**
 * The inputs the safety rules read. Ported from the {@code config} / {@code cache_settings}
 * dictionaries threaded through {@code safety_rules.py}, but with one deliberate change of
 * <em>ownership</em> (plan §8.10): {@code highCardinalityColumns} and
 * {@code liquidClusteredFilterColumns} are no longer hand-maintained by the customer — they are
 * populated by the auto-profiler. The rule mechanism is unchanged; only who fills the sets moved.
 *
 * @param impossibleAggregates           aggregate substrings that cannot be bucketed (e.g. {@code DISTINCT},
 *                                        {@code MEDIAN}); presence forces a bypass
 * @param highCardinalityColumns          columns whose GROUP BY would materialise ~one row per value
 * @param liquidClusteredFilterColumns    columns where Delta data-skipping already makes the query fast
 * @param fixedStepSeconds                the universal internal storage step
 * @param cacheBucketHours                the bucket width in hours (default 24)
 * @param minimumCacheableTimeRangeSeconds optional floor on query duration; empty disables the check
 */
public record CacheConfiguration(Set<String> impossibleAggregates, Set<String> highCardinalityColumns,
                                 Set<String> liquidClusteredFilterColumns, int fixedStepSeconds,
                                 int cacheBucketHours, Optional<Long> minimumCacheableTimeRangeSeconds) {

    public CacheConfiguration {
        impossibleAggregates = Set.copyOf(impossibleAggregates);
        highCardinalityColumns = Set.copyOf(highCardinalityColumns);
        liquidClusteredFilterColumns = Set.copyOf(liquidClusteredFilterColumns);
        if (fixedStepSeconds <= 0) {
            throw new IllegalArgumentException("fixedStepSeconds must be > 0, but was: " + fixedStepSeconds);
        }
        if (cacheBucketHours <= 0) {
            throw new IllegalArgumentException("cacheBucketHours must be > 0, but was: " + cacheBucketHours);
        }
    }

    /** A sensible default: the holistic-aggregate denylist, day buckets, the platform fixed step. */
    public static CacheConfiguration defaults() {
        return new CacheConfiguration(
                Set.of("DISTINCT", "MEDIAN", "PERCENTILE"),
                Set.of(),
                Set.of(),
                CacheConstants.DEFAULT_CACHE_STEP_SECONDS,
                24,
                Optional.empty());
    }

    public CacheConfiguration withHighCardinalityColumns(Set<String> columns) {
        return new CacheConfiguration(impossibleAggregates, columns, liquidClusteredFilterColumns,
                fixedStepSeconds, cacheBucketHours, minimumCacheableTimeRangeSeconds);
    }

    public CacheConfiguration withLiquidClusteredFilterColumns(Set<String> columns) {
        return new CacheConfiguration(impossibleAggregates, highCardinalityColumns, columns,
                fixedStepSeconds, cacheBucketHours, minimumCacheableTimeRangeSeconds);
    }

    public CacheConfiguration withMinimumCacheableTimeRangeSeconds(long seconds) {
        return new CacheConfiguration(impossibleAggregates, highCardinalityColumns, liquidClusteredFilterColumns,
                fixedStepSeconds, cacheBucketHours, Optional.of(seconds));
    }
}
