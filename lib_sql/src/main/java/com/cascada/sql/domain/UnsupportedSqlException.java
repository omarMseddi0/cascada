package com.cascada.sql.domain;

/**
 * Thrown when a SQL statement cannot be canonicalised for caching — it does not parse, is not a
 * simple {@code SELECT}, lacks a time range, or uses a construct the engine will not risk caching.
 *
 * <p>Anything unparseable or unsupported <b>bypasses to Spark</b> rather than being silently
 * accepted. Callers translate this exception into a cache bypass.
 */
public class UnsupportedSqlException extends com.cascada.cache.domain.query.UncacheableQueryException {

    public UnsupportedSqlException(String message) {
        super(message);
    }

    public UnsupportedSqlException(String message, Throwable cause) {
        super(message, cause);
    }
}
