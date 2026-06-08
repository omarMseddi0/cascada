package com.cascada.cache.application;

import com.cascada.cache.domain.admin.CacheScope;
import com.cascada.cache.domain.admin.CacheSizeReport;
import com.cascada.cache.domain.port.CacheBackendPort;
import com.cascada.identity.domain.TenantIdentifier;

import java.util.Objects;

/**
 * The application service behind the open-source <strong>administrator console</strong> (system card
 * §10). It is the single entry point for the three operator actions that used to be gated behind paid
 * tiers and are now first-class, free features:
 *
 * <ul>
 *   <li><b>"How much is in cache?"</b> — {@link #measureCacheSize()} / {@link #measureCacheSize(TenantIdentifier)},
 *       returning the headline megabytes the button renders.</li>
 *   <li><b>"Flush cache"</b> — {@link #flushEverything()}, {@link #flushTenant(TenantIdentifier)},
 *       {@link #flushKeyPrefix(String)}, each returning how many buckets were purged.</li>
 * </ul>
 *
 * <p>It delegates the bytes/keyspace work to the {@link CacheBackendPort} so it stays framework-free and
 * works identically over the in-memory backend (dev), Valkey/Redis (hot), or a Delta-backed cold tier.
 * View creation (the third console action) belongs to the Materialization Studio service, not here —
 * the cache never invents views; the operator defines them (boundary rule, plan §9.6).
 */
public final class CacheAdministrationService {

    private final CacheBackendPort cacheBackend;

    public CacheAdministrationService(CacheBackendPort cacheBackend) {
        this.cacheBackend = Objects.requireNonNull(cacheBackend, "cacheBackend");
    }

    /** The whole-cache size report (admin "Cache size" button, all tenants). */
    public CacheSizeReport measureCacheSize() {
        return cacheBackend.sizeReport();
    }

    /**
     * One tenant's slice of the size report. Computed from the same full report (a single keyspace
     * walk), then narrowed — so the per-tenant number always reconciles with the global total shown
     * beside it.
     */
    public CacheSizeReport measureCacheSize(TenantIdentifier tenant) {
        Objects.requireNonNull(tenant, "tenant");
        CacheSizeReport full = cacheBackend.sizeReport();
        long tenantBytes = full.bytesByTenant().getOrDefault(tenant.asKeyPrefixSegment(), 0L);
        // bucketCount for the tenant is unknown from the aggregate map alone; report 0 buckets only
        // when the tenant holds nothing, otherwise leave the global bucket count out of the slice.
        if (tenantBytes == 0L) {
            return CacheSizeReport.empty();
        }
        return new CacheSizeReport(tenantBytes, full.bucketCount(),
                java.util.Map.of(tenant.asKeyPrefixSegment(), tenantBytes));
    }

    /** Purge the entire cache; returns the number of buckets removed. */
    public long flushEverything() {
        return cacheBackend.flush(CacheScope.everything());
    }

    /** Purge one tenant's buckets only; returns the number removed. */
    public long flushTenant(TenantIdentifier tenant) {
        return cacheBackend.flush(CacheScope.forTenant(tenant));
    }

    /** Surgical eviction by an explicit key prefix (e.g. a query-hash family or a bucket-size band). */
    public long flushKeyPrefix(String keyPrefix) {
        return cacheBackend.flush(CacheScope.forKeyPrefix(keyPrefix));
    }
}
