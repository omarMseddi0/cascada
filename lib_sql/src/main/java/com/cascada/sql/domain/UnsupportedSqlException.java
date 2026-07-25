package com.cascada.sql.canonical;

/**
 * Thrown when a SQL statement cannot be canonicalised for caching — it does not parse, is not a
 * simple {@code SELECT}, lacks a time range, or uses a construct the engine will not risk caching.
 *
 * <p>Per the JSqlParser-limits policy (HARNESS §B.5.4 point 3), anything unparseable or unsupported
 * <b>bypasses to Spark</b> rather than being silently accepted — exactly like the Python
 * {@code _is_supported_order_expression} guard. Callers translate this exception into a cache bypass.
 */
public class UnsupportedSqlException extends RuntimeException {

    public UnsupportedSqlException(String message) {
        super(message);
    }

    public UnsupportedSqlException(String message, Throwable cause) {
        super(message, cause);
    }
}
