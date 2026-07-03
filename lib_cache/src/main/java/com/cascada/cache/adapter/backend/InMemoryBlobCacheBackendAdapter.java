package com.cascada.cache.adapter.backend;

import com.cascada.cache.domain.admin.CacheKeyTenantSegment;
import com.cascada.cache.domain.admin.CacheScope;
import com.cascada.cache.domain.admin.CacheSizeReport;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.port.CacheBackendPort;
import com.cascada.cache.domain.port.CacheValueSerializerPort;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An in-memory {@link CacheBackendPort} that stores exactly what a Valkey/Redis adapter would — the
 * serialized blob, not the live frame — so it is a true Liskov substitute for the production backend
 * (ARCHITECTURE §C.2, TESTING §1.3). It is the fake-Redis used by the cache-correctness gate and the
 * reference backend for the runnable engine in local/dev mode.
 */
public final class InMemoryBlobCacheBackendAdapter implements CacheBackendPort {

    private final Map<String, byte[]> blobsByKey = new ConcurrentHashMap<>();
    private final CacheValueSerializerPort serializer;
    private final java.util.concurrent.atomic.AtomicLong existsCalls = new java.util.concurrent.atomic.AtomicLong();

    public InMemoryBlobCacheBackendAdapter(CacheValueSerializerPort serializer) {
        this.serializer = serializer;
    }

    @Override
    public List<Boolean> existsForKeys(List<String> keys) {
        existsCalls.incrementAndGet();
        List<Boolean> presence = new ArrayList<>(keys.size());
        for (String key : keys) {
            presence.add(blobsByKey.containsKey(key));
        }
        return presence;
    }

    @Override
    public List<Optional<ResultFrame>> multiGet(List<String> keys) {
        List<Optional<ResultFrame>> frames = new ArrayList<>(keys.size());
        for (String key : keys) {
            byte[] blob = blobsByKey.get(key);
            frames.add(blob == null ? Optional.empty() : Optional.of(serializer.deserialize(blob)));
        }
        return frames;
    }

    @Override
    public void store(String key, ResultFrame frame) {
        blobsByKey.put(key, serializer.serialize(frame));
    }

    /**
     * Measures the actual bytes held: the serialized blob is exactly what a Valkey/Redis tier would
     * store, so {@code blob.length} is the on-the-wire size. This makes the fake a faithful sizing
     * substitute for the production backend, not just a functional one.
     */
    @Override
    public CacheSizeReport sizeReport() {
        long totalBytes = 0L;
        long bucketCount = 0L;
        Map<String, Long> bytesByTenant = new HashMap<>();
        for (Map.Entry<String, byte[]> entry : blobsByKey.entrySet()) {
            long bytes = entry.getValue().length;
            totalBytes += bytes;
            bucketCount++;
            bytesByTenant.merge(CacheKeyTenantSegment.of(entry.getKey()), bytes, Long::sum);
        }
        return new CacheSizeReport(totalBytes, bucketCount, bytesByTenant);
    }

    @Override
    public long flush(CacheScope scope) {
        long purged = 0L;
        Iterator<String> keys = blobsByKey.keySet().iterator();
        while (keys.hasNext()) {
            if (scope.matches(keys.next())) {
                keys.remove();
                purged++;
            }
        }
        return purged;
    }

    /** Test/diagnostic helper: how many buckets are currently stored. */
    public int storedBucketCount() {
        return blobsByKey.size();
    }

    /** Test/diagnostic helper: how many EXISTS round-trips this backend has served. */
    public long existsCallCount() {
        return existsCalls.get();
    }
}
