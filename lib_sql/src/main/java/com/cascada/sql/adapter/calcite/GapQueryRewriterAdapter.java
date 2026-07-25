package com.cascada.sql.rewrite;

import com.cascada.cache.domain.GapPlan;
import com.cascada.cache.domain.port.GapQueryRewriterPort;

/**
 * Adapts the Apache Calcite-based {@link GapQueryBuilder} to the cache's {@link GapQueryRewriterPort}, so
 * the engine in {@code lib_cache} depends only on the port and the SQL library stays on the adapter
 * side of the hexagon (the dependency points inward).
 */
public final class GapQueryRewriterAdapter implements GapQueryRewriterPort {

    private final GapQueryBuilder gapQueryBuilder;

    public GapQueryRewriterAdapter(String timeColumn, long bucketSeconds) {
        this.gapQueryBuilder = new GapQueryBuilder(timeColumn, bucketSeconds);
    }

    @Override
    public String buildGapQuery(String physicalSql, GapPlan gapPlan) {
        return gapQueryBuilder.buildGapQuery(physicalSql, gapPlan);
    }
}
