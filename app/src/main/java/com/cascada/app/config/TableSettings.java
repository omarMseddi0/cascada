package com.cascada.app.config;

import java.util.Map;
import java.util.Set;

public record TableSettings(String logicalName, String deltaPath,
                            Map<String, String> columnMappings, Set<String> timeColumns, String sourceType) {
    public TableSettings(String logicalName, String deltaPath, Map<String, String> columnMappings, Set<String> timeColumns) {
        this(logicalName, deltaPath, columnMappings, timeColumns, "HOT_BATCH");
    }

    public TableSettings { 
        sourceType = sourceType == null ? "HOT_BATCH" : sourceType.toUpperCase(java.util.Locale.ROOT);
        if (!sourceType.equals("HOT_BATCH") && !sourceType.equals("HOT_VIEW"))
            throw new IllegalArgumentException("sourceType must be HOT_BATCH or HOT_VIEW");
        if (logicalName == null || logicalName.isBlank() || deltaPath == null || deltaPath.isBlank())
            throw new IllegalArgumentException("Table name and Delta path are required");
        columnMappings = Map.copyOf(columnMappings);
        timeColumns = Set.copyOf(timeColumns);
        if (timeColumns.isEmpty() || !columnMappings.keySet().containsAll(timeColumns))
            throw new IllegalArgumentException("Every table time column requires a physical mapping");
    }
}
