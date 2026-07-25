package com.cascada.sql.translate;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * The registry of logical tables, modelling the Python {@code ConfigLoader.get_table_by_path} lookup.
 * The translator consults it to resolve a logical table name to its {@link RegisteredTable} (Delta
 * path + column mapping). Tenant-scoped in production; a plain map here.
 */
public final class TableCatalog {

    private final Map<String, RegisteredTable> tablesByLogicalName = new HashMap<>();

    public TableCatalog register(RegisteredTable table) {
        tablesByLogicalName.put(table.logicalTableName().toLowerCase(Locale.ROOT), table);
        return this;
    }

    public Optional<RegisteredTable> findByLogicalName(String logicalTableName) {
        return Optional.ofNullable(tablesByLogicalName.get(logicalTableName.toLowerCase(Locale.ROOT)));
    }
}
