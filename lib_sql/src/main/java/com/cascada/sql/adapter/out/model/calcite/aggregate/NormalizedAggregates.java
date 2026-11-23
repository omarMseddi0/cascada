package com.cascada.sql.adapter.out.model.calcite.aggregate;

import java.util.List;

/** Hash ingredients and parsed AVG expressions retained for result reconstruction. */
public record NormalizedAggregates(List<String> normalizedForHash, List<String> originalAggregates) {

    public NormalizedAggregates {
        normalizedForHash = List.copyOf(normalizedForHash);
        originalAggregates = List.copyOf(originalAggregates);
    }
}
