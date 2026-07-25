package com.cascada.cache.application.port.in;

/**
 * <b>Primary (driving) port</b> — "answer this query, written the way the customer thinks about their
 * data".
 *
 * <p>Where {@link ExecuteCachedQueryUseCase} takes an already-canonicalised query, this port takes the
 * raw logical SQL string a customer or dashboard submits and owns the whole journey:
 *
 * <pre>
 *   logical SQL ──▶ translate to physical (logical names → storage path + physical columns)
 *               ──▶ canonicalise (physical SQL → CanonicalQueryObject)
 *               ──▶ safety rules → cache path or direct bypass
 * </pre>
 *
 * <p>This is the port a REST/JDBC driving adapter should actually use, because a caller outside the
 * hexagon has a SQL string, not a {@code CanonicalQueryObject}. Keeping the two ports separate matters:
 * translation and canonicalisation are performed by <em>outbound</em> adapters
 * ({@code LogicalSqlTranslatorPort}, {@code SqlCanonicalizerPort}), so this port is the one place both
 * the inbound direction and those two outbound hops are composed.
 */
public interface ExecuteLogicalQueryUseCase {

    /**
     * Translate, canonicalise, and answer a logical SQL query.
     *
     * @throws RuntimeException when the SQL cannot be translated or canonicalised — the caller decides
     *     whether to surface the error or fall back to submitting the SQL directly
     */
    ExecuteCachedQueryUseCase.Result query(String logicalSql);
}
