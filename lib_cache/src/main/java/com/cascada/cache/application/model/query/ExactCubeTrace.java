package com.cascada.cache.application.model.query;

import com.cascada.cache.domain.model.cube.ExactCubeFragment;
import java.util.List;

/** Internal query receipt. Version is the verified source version, or -1 for ordinary Spark/fallback. */
public record ExactCubeTrace(String route, String reason, long version, List<ExactCubeFragment> fragments,
                             List<String> residualSql, String outerSql, long planningNanos) {
    public ExactCubeTrace {
        fragments = List.copyOf(fragments);
        residualSql = List.copyOf(residualSql);
    }
}
