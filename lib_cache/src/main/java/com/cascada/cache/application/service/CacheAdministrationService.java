package com.cascada.cache.application.service;

import com.cascada.cache.application.port.in.FlushCacheUseCase;
import com.cascada.cache.application.port.in.MeasureCacheSizeUseCase;
import com.cascada.cache.application.port.out.CacheAdministrationPort;
import com.cascada.cache.application.port.out.CoverageIndexPort;
import com.cascada.cache.domain.admin.CacheScope;
import com.cascada.cache.domain.admin.CacheSizeReport;
import com.cascada.cache.domain.cube.CubeShapeCatalog;
import com.cascada.identity.domain.TenantIdentifier;

import java.util.Map;
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
 * <p>It delegates the bytes/keyspace work to the {@link CacheAdministrationPort} so it stays framework-free and
 * works identically over the in-memory backend (dev), Valkey/Redis (hot), or a Delta-backed cold tier.
 * View creation (the third console action) belongs to the Materialization Studio service, not here —
 * the cache never invents views; the operator defines them (boundary rule, plan §9.6).
 */
public final class CacheAdministrationService implements MeasureCacheSizeUseCase, FlushCacheUseCase {

    private final CacheAdministrationPort cacheBackend;
    private final CoverageIndexPort coverageIndex;
    private final CubeShapeCatalog cubeCatalog;

    public CacheAdministrationService(CacheAdministrationPort cacheBackend) {
        this(cacheBackend, null, null);
    }

    public CacheAdministrationService(CacheAdministrationPort cacheBackend, CoverageIndexPort coverageIndex,
                                      CubeShapeCatalog cubeCatalog) {
        this.cacheBackend = Objects.requireNonNull(cacheBackend, "cacheBackend");
        this.coverageIndex = coverageIndex;
        this.cubeCatalog = cubeCatalog;
    }

    /** The whole-cache size report (admin "Cache size" button, all tenants). */
    @Override
    public CacheSizeReport measureCacheSize() {
        return cacheBackend.sizeReport();
    }

    /**
     * One tenant's slice of the size report. Computed from the same full report (a single keyspace
     * walk), then narrowed — so the per-tenant number always reconciles with the global total shown
     * beside it.
     */
    @Override
    public CacheSizeReport measureCacheSize(TenantIdentifier tenant) {
        Objects.requireNonNull(tenant, "tenant");
        CacheSizeReport full = cacheBackend.sizeReport();
        long tenantBytes = full.bytesByTenant().getOrDefault(tenant.asKeyPrefixSegment(), 0L);
        long tenantBucketCount = full.bucketCountByTenant().getOrDefault(tenant.asKeyPrefixSegment(), 0L);
        if (tenantBytes == 0L && tenantBucketCount == 0L) {
            return CacheSizeReport.empty();
        }
        return new CacheSizeReport(tenantBytes, tenantBucketCount,
                Map.of(tenant.asKeyPrefixSegment(), tenantBytes),
                Map.of(tenant.asKeyPrefixSegment(), tenantBucketCount));
    }

    /** Purge the entire cache; returns the number of buckets removed. */
    @Override
    public long flushEverything() {
        long purged = cacheBackend.flush(CacheScope.everything());
        if (coverageIndex != null) coverageIndex.clear();
        if (cubeCatalog != null) cubeCatalog.clear();
        return purged;
    }

    /** Purge one tenant's buckets only; returns the number removed. */
    @Override
    public long flushTenant(TenantIdentifier tenant) {
        long purged = cacheBackend.flush(CacheScope.forTenant(tenant));
        // Coverage and cube entries are not tenant-addressable yet; clear their advisory state safely.
        if (coverageIndex != null) coverageIndex.clear();
        if (cubeCatalog != null) cubeCatalog.clear();
        return purged;
    }

    /** Surgical eviction by an explicit key prefix (e.g. a query-hash family or a bucket-size band). */
    @Override
    public long flushKeyPrefix(String keyPrefix) {
        long purged = cacheBackend.flush(CacheScope.forKeyPrefix(keyPrefix));
        if (coverageIndex != null) coverageIndex.clear();
        if (cubeCatalog != null) cubeCatalog.clear();
        return purged;
    }
}
