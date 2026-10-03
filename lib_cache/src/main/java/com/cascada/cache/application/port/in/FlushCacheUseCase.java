package com.cascada.cache.application.port.in;

/**
 * <b>Primary (driving) port</b> — cache flush actions.
 *
 * <p>Every method returns how many buckets were purged, so the console can report the effect rather
 * than merely claiming success. Flush-all targets the cache bucket namespace; prefix flush supports
 * surgical eviction of a query-hash family or bucket-width band.
 */
public interface FlushCacheUseCase {

    /** Purge every bucket in the cache. */
    long flushEverything();

    /** Surgical eviction by explicit key prefix (one query-hash family, one bucket-width band, …). */
    long flushKeyPrefix(String keyPrefix);
}
