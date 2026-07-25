package com.cascada.cache.application.port.in;

import com.cascada.identity.domain.TenantIdentifier;

/**
 * <b>Primary (driving) port</b> — the administrator console's "flush cache" action.
 *
 * <p>Every method returns how many buckets were purged, so the console can report the effect rather
 * than merely claiming success. Scoping is a key-prefix match over keys whose tenant segment is inside
 * the signed material, which is why a tenant-scoped flush <em>cannot</em> reach another tenant's
 * buckets — an isolation property of the key format, not of this code's carefulness.
 */
public interface FlushCacheUseCase {

    /** Purge every bucket in the cache. */
    long flushEverything();

    /** Purge one tenant's buckets only. */
    long flushTenant(TenantIdentifier tenant);

    /** Surgical eviction by explicit key prefix (one query-hash family, one bucket-width band, …). */
    long flushKeyPrefix(String keyPrefix);
}
