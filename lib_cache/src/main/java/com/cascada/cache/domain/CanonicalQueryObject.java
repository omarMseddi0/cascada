package com.cascada.cache.domain;

import java.util.List;
import java.util.Optional;

/**
 * The deterministic, lossless representation of a query's intent — the contract between the
 * compiler/planner and the cache (plan §3.3). It aggregates the hash DNA, the time range, the
 * deferred post-processing, the metadata, and the optional source/projection signatures used to
 * make the logic hash robust against alias collisions.
 *
 * <p>This is the Java {@code record} graph that replaces the Python {@code dict}-passed
 * "canonical object". Every consumer (hashing, safety rules, merge) reads typed fields rather
 * than string-keyed map lookups.
 */
public record CanonicalQueryObject(HashComponents hashComponents, TimeRange timeRange,
                                   PostProcessing postProcessing, QueryMetadata metadata,
                                   String physicalSql, List<String> sourceSignature,
                                   List<String> projectionSignature) {

    public CanonicalQueryObject {
        sourceSignature = List.copyOf(sourceSignature);
        projectionSignature = List.copyOf(projectionSignature);
        if (physicalSql == null) {
            physicalSql = "";
        }
    }

    public static CanonicalQueryObject of(HashComponents hashComponents, TimeRange timeRange,
                                          PostProcessing postProcessing, QueryMetadata metadata) {
        return new CanonicalQueryObject(hashComponents, timeRange, postProcessing, metadata, "", List.of(), List.of());
    }

    public boolean isTimeSeries() {
        return metadata.isTimeSeries();
    }

    public Optional<Integer> userStepSeconds() {
        return metadata.userStepSeconds();
    }
}
