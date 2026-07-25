package com.cascada.cache.application.port.out;

/**
 * Outbound (driven) port for rewriting a query written in the customer's <em>logical</em> vocabulary
 * (logical table and column names) into the <em>physical</em> SQL that runs against the lakehouse
 * (physical column names, the storage path as the table, time column bucketed when grouped).
 *
 * <p>This is the "the customer never writes a storage path or a physical column name" promise,
 * expressed as a seam. The cache depends on the seam; {@code lib_sql} supplies the Calcite-based
 * implementation. Because the port carries only {@code String}s, the cache's domain stays free of any
 * AST type from any parser library.
 *
 * <p>Implementations MUST throw when the logical query cannot be translated (an unregistered table, a
 * multi-table {@code FROM} they do not support), so the caller bypasses rather than executing SQL that
 * silently reads the wrong table.
 */
public interface LogicalSqlTranslatorPort {

    /**
     * Translate logical SQL to physical SQL.
     *
     * @throws RuntimeException when the query cannot be translated; the caller treats any exception as
     *     a bypass signal
     */
    String translateToPhysicalSql(String logicalSql);
}
