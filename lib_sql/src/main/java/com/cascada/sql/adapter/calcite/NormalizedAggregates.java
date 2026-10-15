package com.cascada.sql.adapter.calcite;

import java.util.List;

/** Hash ingredients and parsed AVG expressions retained for result reconstruction. */
record NormalizedAggregates(List<String> normalizedForHash, List<String> originalAggregates) {

    NormalizedAggregates {
        normalizedForHash = List.copyOf(normalizedForHash);
        originalAggregates = List.copyOf(originalAggregates);
    }
}
