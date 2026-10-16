package com.cascada.sql.domain;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The registry of logical tables, modelling the Python {@code ConfigLoader.get_table_by_path} lookup.
 * The translator consults it to resolve a logical table name to its {@link RegisteredTable} (Delta
 * path + column mapping). Uses a plain map of registered tables.
 */
public final class TableCatalog {

    private final Map<String, RegisteredTable> tablesByLogicalName = new HashMap<>();

    public TableCatalog register(RegisteredTable table) {
        tablesByLogicalName.put(table.logicalTableName().toLowerCase(Locale.ROOT), table);
        return this;
    }

    /** Physical names of every registered table's time columns. */
    public Set<String> physicalTimeColumns() {
        Set<String> columns = new java.util.HashSet<>();
        tablesByLogicalName.values().forEach(table -> columns.addAll(table.physicalTimeColumns()));
        return Set.copyOf(columns);
    }

    public Optional<RegisteredTable> findByLogicalName(String logicalTableName) {
        return Optional.ofNullable(tablesByLogicalName.get(logicalTableName.toLowerCase(Locale.ROOT)));
    }
}
