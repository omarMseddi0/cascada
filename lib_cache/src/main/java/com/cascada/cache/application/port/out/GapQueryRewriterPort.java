package com.cascada.cache.application.port.out;

import com.cascada.cache.domain.time.GapPlan;
import java.util.Optional;

/**
 * Rewrites a physical SQL string so it scans only the gap (the buckets not in cache). The cache
 * engine depends on this port, not on the SQL library, so the dependency points inward
 * (lib_sql implements it; lib_cache does not depend on lib_sql).
 */
public interface GapQueryRewriterPort {

    String buildGapQuery(String physicalSql, GapPlan gapPlan);

    /**
     * The same gap query, additionally grouped by bucket and projecting each row's bucket start
     * (epoch-aligned, {@code bucketSeconds} wide) as a BIGINT column named {@code bucketColumn}.
     * Splitting its rows by that column yields exactly the frame each per-bucket gap query would
     * return, so many complete buckets are computed by one Spark job. Empty when the rewriter cannot
     * produce that shape; callers then fall back to {@link #buildGapQuery}.
     */
    default Optional<String> buildBucketedGapQuery(String physicalSql, GapPlan gapPlan, String bucketColumn) {
        return Optional.empty();
    }
}
