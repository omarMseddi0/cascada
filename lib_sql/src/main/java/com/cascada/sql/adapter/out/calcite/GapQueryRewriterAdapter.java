package com.cascada.sql.adapter.out.calcite;

import com.cascada.cache.application.port.out.GapQueryRewriterPort;
import com.cascada.cache.domain.time.GapPlan;

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

    /** Recognises every configured time column; gaps are injected on the column the query's window uses. */
    public GapQueryRewriterAdapter(com.cascada.sql.domain.TimeDimensionMap timeDimensions, String defaultTimeColumn,
                                   long bucketSeconds) {
        this.gapQueryBuilder = new GapQueryBuilder(timeDimensions, defaultTimeColumn, bucketSeconds);
    }

    @Override
    public String buildGapQuery(String physicalSql, GapPlan gapPlan) {
        return gapQueryBuilder.buildGapQuery(physicalSql, gapPlan);
    }

    @Override
    public String buildStorageQuery(String physicalSql, GapPlan gapPlan, int fixedStepSeconds) {
        return gapQueryBuilder.buildStorageQuery(physicalSql, gapPlan, fixedStepSeconds);
    }
}
