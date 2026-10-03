package com.cascada.cache.application.service;

import com.cascada.cache.application.port.in.FlushCacheUseCase;
import com.cascada.cache.application.port.in.MeasureCacheSizeUseCase;
import com.cascada.cache.application.port.out.CacheAdministrationPort;
import com.cascada.cache.application.port.out.CoverageIndexPort;
import com.cascada.cache.domain.admin.CacheScope;
import com.cascada.cache.domain.admin.CacheSizeReport;
import com.cascada.cache.domain.cube.CubeShapeCatalog;

import java.util.Objects;

/**
 * Application service behind cache administration. It reports global stored size and supports
 * flushing all cache buckets or an explicitly selected key prefix.
 *
 * <p>It delegates the bytes/keyspace work to {@link CacheAdministrationPort} so it stays
 * framework-free and works identically over the in-memory backend or Valkey/Redis. View creation
 * belongs to the Materialization Studio service, not here.
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

    /** The global cache size report. */
    @Override
    public CacheSizeReport measureCacheSize() {
        return cacheBackend.sizeReport();
    }

    /** Purge the entire cache; returns the number of buckets removed. */
    @Override
    public long flushEverything() {
        long purged = cacheBackend.flush(CacheScope.everything());
        if (coverageIndex != null) coverageIndex.clear();
        if (cubeCatalog != null) cubeCatalog.clear();
        return purged;
    }

    /** Surgical eviction by an explicit key prefix. */
    @Override
    public long flushKeyPrefix(String keyPrefix) {
        long purged = cacheBackend.flush(CacheScope.forKeyPrefix(keyPrefix));
        if (coverageIndex != null) coverageIndex.clear();
        if (cubeCatalog != null) cubeCatalog.clear();
        return purged;
    }
}
