package com.cascada.identity.domain;

/**
 * The Delta column-mapping schema identifier of a source table at the moment a
 * cache entry was written (plan §8.4).
 *
 * <p>A schema change bumps this value, which feeds the cache key so that entries
 * computed against an older shape become unreachable and are evicted naturally
 * rather than silently serving rows with a stale column set (plan §7.3).
 */
public record SchemaVersion(long value) {

    public SchemaVersion {
        if (value < 0) {
            throw new IllegalArgumentException("schema version must be non-negative, but was: " + value);
        }
    }

    public static SchemaVersion of(long value) {
        return new SchemaVersion(value);
    }

    public static SchemaVersion initial() {
        return new SchemaVersion(0L);
    }

    public SchemaVersion next() {
        return new SchemaVersion(value + 1);
    }
}
