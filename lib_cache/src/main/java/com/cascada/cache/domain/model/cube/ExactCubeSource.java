package com.cascada.cache.domain.model.cube;

import java.util.Map;

/** Delta table identity and SQL metadata required to match compatible bucket shapes. */
public record ExactCubeSource(String path, String tableId, String schema,
                              String semantics, String accessScope, Map<String, String> types, boolean groupByAliases) {
    public ExactCubeSource {
        types = Map.copyOf(types);
    }
}
