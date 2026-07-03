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
 *
 * @param logicSignature normalized logic markers that are not group-by/aggregate/filter clauses but
 *                       still change the meaning of the query — the {@code HAVING} expression, each
 *                       {@code JOIN ... ON} condition, and the {@code DISTINCT} flag. These MUST feed
 *                       the logic hash (README caveats 5 and 6): without them a query with
 *                       {@code HAVING} (or different join keys, or {@code SELECT DISTINCT}) would
 *                       share a cache entry with its unfiltered sibling and serve wrong rows.
 */
public record CanonicalQueryObject(HashComponents hashComponents, TimeRange timeRange,
                                   PostProcessing postProcessing, QueryMetadata metadata,
                                   String physicalSql, List<String> sourceSignature,
                                   List<String> projectionSignature, List<String> logicSignature) {

    public CanonicalQueryObject {
        sourceSignature = List.copyOf(sourceSignature);
        projectionSignature = List.copyOf(projectionSignature);
        logicSignature = List.copyOf(logicSignature);
        if (physicalSql == null) {
            physicalSql = "";
        }
    }

    /**
     * Pre-{@code logicSignature} shape, kept so existing call sites (and serialized fixtures) that
     * carry none of the extra logic markers keep compiling and hashing identically. An empty logic
     * signature means "no HAVING, no JOIN ON conditions, no DISTINCT" — the only shape the older
     * constructor could ever describe.
     */
    public CanonicalQueryObject(HashComponents hashComponents, TimeRange timeRange,
                                PostProcessing postProcessing, QueryMetadata metadata,
                                String physicalSql, List<String> sourceSignature,
                                List<String> projectionSignature) {
        this(hashComponents, timeRange, postProcessing, metadata, physicalSql, sourceSignature,
                projectionSignature, List.of());
    }

    public static CanonicalQueryObject of(HashComponents hashComponents, TimeRange timeRange,
                                          PostProcessing postProcessing, QueryMetadata metadata) {
        return new CanonicalQueryObject(hashComponents, timeRange, postProcessing, metadata, "", List.of(),
                List.of(), List.of());
    }

    public boolean isTimeSeries() {
        return metadata.isTimeSeries();
    }

    public Optional<Integer> userStepSeconds() {
        return metadata.userStepSeconds();
    }
}
