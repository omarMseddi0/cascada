package com.cascada.cache.application.model.query;

import com.cascada.cache.domain.model.cube.ExactCubeFragment;
import java.util.List;

/** Internal query receipt, independent of the transport response schemas. */
public record ExactCubeTrace(String route, String reason, long version, List<ExactCubeFragment> fragments,
                             List<String> residualSql, String outerSql, long planningNanos) {
    public ExactCubeTrace {
        fragments = List.copyOf(fragments);
        residualSql = List.copyOf(residualSql);
    }
}
