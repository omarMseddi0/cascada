package com.cascada.cache.domain.port;

import com.cascada.cache.domain.admin.CacheScope;
import com.cascada.cache.domain.admin.CacheSizeReport;
import com.cascada.cache.domain.frame.ResultFrame;

import java.util.List;
import java.util.Optional;

/**
 * The outbound port the cache engine uses to talk to a storage tier (Valkey/Redis hot, RocksDB warm,
 * object-store cold). It mirrors the two-phase access pattern of {@code cache_execution_engine.py}:
 * a cheap pipelined EXISTS for gap analysis, then a bulk MGET of the buckets that are present.
 *
 * <p>Implemented by {@code InMemoryBlobCacheBackendAdapter} (tests) and {@code ValkeyCacheBackendAdapter}
 * (production). Because both store the same serialized blobs, the in-memory one is a true Liskov
 * substitute (ARCHITECTURE §C.2), which is what lets the correctness gate run without a container.
 */
public interface CacheBackendPort {

    /** Phase 1: pipelined existence check; element {@code i} is true iff {@code keys.get(i)} is cached. */
    List<Boolean> existsForKeys(List<String> keys);

    /** Phase 2: bulk fetch; returns the decoded frame for each key, empty where the key was absent. */
    List<Optional<ResultFrame>> multiGet(List<String> keys);

    /** Store (or overwrite) one bucket's frame under its key. */
    void store(String key, ResultFrame frame);

    /**
     * The "how much is in cache?" measurement (plan §8.17): the sum of stored blob bytes, the bucket
     * count, and a per-tenant breakdown. Implementations measure the bytes they actually hold (Valkey
     * via {@code MEMORY USAGE}, the in-memory fake by array length, a Delta cold tier via snapshot size)
     * — never a live-heap estimate — so the figure equals what the customer pays to store.
     */
    CacheSizeReport sizeReport();

    /**
     * The "flush cache" action (plan §8.17). Removes every bucket whose key falls within {@code scope}
     * and returns how many buckets were purged. Scoping is a key-prefix match, so a tenant-scoped flush
     * can never reach another tenant's buckets (the tenant segment is inside the signed key material).
     */
    long flush(CacheScope scope);
}
