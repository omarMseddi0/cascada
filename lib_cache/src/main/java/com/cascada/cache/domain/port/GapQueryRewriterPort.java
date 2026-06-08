package com.cascada.cache.domain.port;

import com.cascada.cache.domain.GapPlan;

/**
 * Rewrites a physical SQL string so it scans only the gap (the buckets not in cache). The cache
 * engine depends on this port, not on the SQL library, so the dependency points inward
 * (lib_sql implements it; lib_cache does not depend on lib_sql).
 */
public interface GapQueryRewriterPort {

    String buildGapQuery(String physicalSql, GapPlan gapPlan);
}
