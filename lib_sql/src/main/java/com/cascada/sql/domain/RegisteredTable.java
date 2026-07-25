package com.cascada.sql.translate;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A registered table's logical→physical mapping, modelling the per-table config the Python
 * {@code _build_local_translation_map} reads ({@code mapped_name} → {@code source_name}) plus the
 * Delta path it lives at. This is what lets the customer write {@code SELECT country FROM traffic}
 * and never know the physical column is {@code C5} or that the table is a Delta path.
 *
 * @param logicalTableName       the name the customer writes (e.g. {@code traffic})
 * @param deltaPath              the physical object-store path the table lives at
 * @param logicalToPhysicalColumn map of logical column name → physical column name (case-insensitive keys)
 * @param logicalTimeColumns     the logical column names that are the time dimension (e.g. {@code ts})
 */
public record RegisteredTable(String logicalTableName, String deltaPath,
                              Map<String, String> logicalToPhysicalColumn, Set<String> logicalTimeColumns) {

    public RegisteredTable {
        // Normalise lookup keys to lowercase so translation is case-insensitive.
        logicalToPhysicalColumn = logicalToPhysicalColumn.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(
                        entry -> entry.getKey().toLowerCase(Locale.ROOT), Map.Entry::getValue));
        logicalTimeColumns = logicalTimeColumns.stream()
                .map(name -> name.toLowerCase(Locale.ROOT))
                .collect(Collectors.toUnmodifiableSet());
    }

    /** The physical column name for a logical one, or empty if this table does not map it. */
    public Optional<String> physicalColumnFor(String logicalColumnName) {
        return Optional.ofNullable(logicalToPhysicalColumn.get(logicalColumnName.toLowerCase(Locale.ROOT)));
    }

    public boolean isTimeColumn(String logicalColumnName) {
        return logicalTimeColumns.contains(logicalColumnName.toLowerCase(Locale.ROOT));
    }

    /** The physical name of the time column (after mapping), if this table has one mapped. */
    public Optional<String> physicalTimeColumn() {
        for (String logicalTimeColumn : logicalTimeColumns) {
            Optional<String> physical = physicalColumnFor(logicalTimeColumn);
            if (physical.isPresent()) {
                return physical;
            }
        }
        return Optional.empty();
    }

    /** Convenience builder for a table whose physical names are given as a simple logical→physical map. */
    public static RegisteredTable of(String logicalTableName, String deltaPath,
                                     Map<String, String> logicalToPhysicalColumn, String... logicalTimeColumns) {
        return new RegisteredTable(logicalTableName, deltaPath, new HashMap<>(logicalToPhysicalColumn),
                Set.of(logicalTimeColumns));
    }
}
