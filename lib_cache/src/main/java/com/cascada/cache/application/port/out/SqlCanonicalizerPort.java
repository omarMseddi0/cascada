package com.cascada.cache.application.port.out;

import com.cascada.cache.domain.CanonicalQueryObject;

/**
 * Outbound (driven) port for turning a physical SQL string into a {@link CanonicalQueryObject} — the
 * deterministic description of query intent that the safety rules, the logic hash, and the merge math
 * all read.
 *
 * <p><b>Why the port lives here and not in {@code lib_sql}.</b> The cache is the driver: it needs a
 * canonical object and does not care whether the parser is Apache Calcite, sqlglot-over-a-socket, or a
 * hand-written recursive descent. Declaring the interface in the cache's domain and letting
 * {@code lib_sql} implement it is dependency inversion — {@code lib_sql} (outer, framework-bearing)
 * depends on {@code lib_cache} (inner, framework-free), never the reverse. Execution flows outward,
 * the source dependency points inward.
 *
 * <p>Implementations MUST throw rather than guess: a statement that cannot be canonicalised (it does
 * not parse, it is not a simple {@code SELECT}, it has no extractable time range) must surface an
 * exception so the caller bypasses to direct execution. A wrong canonical object silently produces a
 * wrong cached answer, which is the one failure mode this engine must never have.
 */
public interface SqlCanonicalizerPort {

    /**
     * Extract the canonical object from a physical SQL string.
     *
     * @throws RuntimeException when the statement cannot be canonicalised; the caller treats any
     *     exception as "bypass the cache and run this SQL directly"
     */
    CanonicalQueryObject canonicalize(String physicalSql);
}
