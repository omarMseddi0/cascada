package com.cascada.cache.domain.model.cube;

import java.util.Map;

/** Admission-time Delta metadata. The schema fingerprint includes mapping metadata. */
public record ExactCubeSource(String path, String tableId, long version, String schema,
                              String semantics, String accessScope, Map<String, String> types) {
    public ExactCubeSource {
        types = Map.copyOf(types);
    }
}
