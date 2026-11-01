package com.cascada.cache.application.port.out;

import com.cascada.cache.domain.model.time.GapPlan;

/**
 * Rewrites a physical SQL string so it scans only the gap (the buckets not in cache). The cache
 * engine depends on this port, not on the SQL library, so the dependency points inward
 * (lib_sql implements it; lib_cache does not depend on lib_sql).
 */
public interface GapQueryRewriterPort {

    String buildGapQuery(String physicalSql, GapPlan gapPlan);

    /** The warmer stores reusable fixed-grain ingredients, never display-grain answers. */
    default String buildStorageQuery(String physicalSql, GapPlan gapPlan, int fixedStepSeconds) {
        return buildGapQuery(physicalSql, gapPlan);
    }
}
