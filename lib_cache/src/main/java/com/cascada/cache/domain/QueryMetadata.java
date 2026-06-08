package com.cascada.cache.domain;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Out-of-band facts about a query that the cache needs but that are not part of the logic
 * hash DNA: whether it is a time series, the original (pre-decomposition) aggregate list,
 * composite-alias formulas, the user's requested display step, and how stale an answer the
 * caller will tolerate.
 *
 * <p>Ported from {@code QueryMetadata} in {@code domain.py}, but with the microservice-specific
 * {@code uses_hot_batch} / "hot view" concept <b>deliberately removed</b> (plan §8.11). The customer
 * never picks "hot view" or "hot batch" — caching just works. The generalisation is
 * {@code stalenessToleranceMillis}: a low tolerance means near-real-time (only the live sub-bucket is
 * recomputed), a high tolerance means a batch dashboard is happy with older buckets. No safety rule
 * bypasses on it; it only informs warming and the bucket pyramid.
 *
 * @param stalenessToleranceMillis how old a cached answer may be before a refresh is preferred; {@code 0}
 *                                 means "as fresh as the completed buckets allow" (the default)
 */
public record QueryMetadata(boolean isTimeSeries, long stalenessToleranceMillis, List<String> originalAggregates,
                            Map<String, String> compositeAliases, Optional<Integer> userStepSeconds,
                            boolean preserveRawTimeSeries, List<String> aggregateSpecs) {

    public QueryMetadata {
        if (stalenessToleranceMillis < 0) {
            throw new IllegalArgumentException("staleness tolerance must be non-negative");
        }
        originalAggregates = List.copyOf(originalAggregates);
        compositeAliases = Map.copyOf(compositeAliases);
        aggregateSpecs = List.copyOf(aggregateSpecs);
    }

    public static QueryMetadata timeSeries(int userStepSeconds) {
        return new QueryMetadata(true, 0L, List.of(), Map.of(), Optional.of(userStepSeconds), false, List.of());
    }

    public static QueryMetadata globalAggregate() {
        return new QueryMetadata(false, 0L, List.of(), Map.of(), Optional.empty(), false, List.of());
    }

    public QueryMetadata withOriginalAggregates(List<String> aggregates) {
        return new QueryMetadata(isTimeSeries, stalenessToleranceMillis, aggregates, compositeAliases, userStepSeconds,
                preserveRawTimeSeries, aggregateSpecs);
    }

    public QueryMetadata withCompositeAliases(Map<String, String> aliases) {
        return new QueryMetadata(isTimeSeries, stalenessToleranceMillis, originalAggregates, aliases, userStepSeconds,
                preserveRawTimeSeries, aggregateSpecs);
    }

    public QueryMetadata withAggregateSpecs(List<String> specs) {
        return new QueryMetadata(isTimeSeries, stalenessToleranceMillis, originalAggregates, compositeAliases,
                userStepSeconds, preserveRawTimeSeries, specs);
    }

    public QueryMetadata withStalenessToleranceMillis(long tolerance) {
        return new QueryMetadata(isTimeSeries, tolerance, originalAggregates, compositeAliases, userStepSeconds,
                preserveRawTimeSeries, aggregateSpecs);
    }
}
