package com.cascada.cache.application.port.out;

/**
 * Compatibility composite for storage adapters that support both query-time bucket access and
 * administration operations.
 *
 * <p>Implemented by {@code InMemoryBlobCacheBackendAdapter} (tests) and {@code ValkeyCacheBackendAdapter}
 * (production). Services depend on the narrower {@link BucketCachePort} or
 * {@link CacheAdministrationPort} contract they use.
 */
public interface CacheBackendPort extends BucketCachePort, CacheAdministrationPort {
}
