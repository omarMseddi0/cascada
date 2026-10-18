package com.cascada.cache.adapter.out.cache;

import com.cascada.cache.adapter.out.cache.model.StoredBlob;
import com.cascada.cache.adapter.out.cache.model.StoredBaseline;
import com.cascada.cache.application.port.out.CacheBackendPort;
import com.cascada.cache.application.port.out.CacheValueSerializerPort;
import com.cascada.cache.domain.admin.CacheScope;
import com.cascada.cache.domain.model.admin.CacheSizeReport;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.key.CacheKeyConstants;
import com.cascada.cache.domain.model.validation.ValidationBaseline;
import com.cascada.cache.exception.CacheSerializationException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

/**
 * An in-memory {@link CacheBackendPort} that stores exactly what a Valkey/Redis adapter would — the
 * serialized blob with its expiry, not the live frame — so it is a true Liskov substitute for the
 * production backend (ARCHITECTURE §C.2, TESTING §1.3). It is the fake-Redis used by the
 * cache-correctness gate and the reference backend for the runnable engine in local/dev mode.
 */
public final class InMemoryBlobCacheBackendAdapter implements CacheBackendPort {

    private final Map<String, StoredBlob> blobsByKey = new ConcurrentHashMap<>();
    private final AtomicReference<StoredBaseline> validationBaseline = new AtomicReference<>();
    private final CacheValueSerializerPort serializer;
    private final Clock clock;
    private final AtomicLong existsCalls = new AtomicLong();
    private long writes;
    private volatile String revision = newToken();
    private String warmingGeneration = newToken();

    public InMemoryBlobCacheBackendAdapter(CacheValueSerializerPort serializer) {
        this(serializer, Clock.systemUTC());
    }

    public InMemoryBlobCacheBackendAdapter(CacheValueSerializerPort serializer, Clock clock) {
        this.serializer = serializer;
        this.clock = clock;
    }

    @Override
    public List<Boolean> existsForKeys(List<String> keys) {
        existsCalls.incrementAndGet();
        return keys.stream().map(key -> liveBlob(key).isPresent()).toList();
    }

    @Override
    public List<Optional<ResultFrame>> multiGet(List<String> keys) {
        return keys.stream().map(this::decodeOrEvict).toList();
    }

    @Override
    public void visitKeys(List<String> keys, BiConsumer<Integer, Optional<ResultFrame>> visitor) {
        IntStream.range(0, keys.size()).forEach(index -> visitor.accept(index, decodeOrEvict(keys.get(index))));
    }

    @Override
    public synchronized void store(String key, ResultFrame frame, Duration timeToLive) {
        if (timeToLive.isNegative() || timeToLive.isZero()) {
            // Same contract as the Valkey backend: a bucket always expires.
            throw new IllegalArgumentException("bucket TTL must be positive");
        }
        if ((writes++ & 63) == 0) {
            var now = clock.instant();
            blobsByKey.entrySet().removeIf(entry -> !now.isBefore(entry.getValue().expiresAt()));
        }
        blobsByKey.put(key, new StoredBlob(serializer.serialize(frame), clock.instant().plus(timeToLive)));
        revision = newToken();
    }

    @Override
    public synchronized void evict(String key) {
        blobsByKey.remove(key);
        revision = newToken();
    }

    @Override public synchronized Optional<String> revision() { return Optional.of(revision); }

    @Override public synchronized Optional<String> warmingGeneration() { return Optional.of(warmingGeneration); }
    @Override public synchronized boolean storeForGeneration(String key, ResultFrame frame, Duration ttl, String generation) {
        if (!warmingGeneration.equals(generation)) return false;
        store(key, frame, ttl);
        return true;
    }

    @Override public synchronized Optional<Duration> remainingTimeToLive(List<String> keys) {
        Duration minimum = null;
        for (String key : keys) {
            StoredBlob blob = blobsByKey.get(key);
            if (blob == null) return Optional.empty();
            Duration ttl = Duration.between(clock.instant(), blob.expiresAt());
            if (ttl.isNegative() || ttl.isZero()) return Optional.empty();
            if (minimum == null || ttl.compareTo(minimum) < 0) minimum = ttl;
        }
        return Optional.ofNullable(minimum);
    }

    @Override
    public Optional<ValidationBaseline> loadValidationBaseline() {
        return Optional.ofNullable(validationBaseline.get())
                .filter(stored -> clock.instant().isBefore(stored.expiresAt()))
                .map(StoredBaseline::baseline);
    }

    @Override
    public void saveValidationBaseline(ValidationBaseline baseline, Duration timeToLive) {
        validationBaseline.set(new StoredBaseline(baseline, clock.instant().plus(timeToLive)));
    }

    /**
     * Measures the actual bytes held: the serialized blob is exactly what a Valkey/Redis tier would
     * store, so {@code blob.length} is the on-the-wire size. This makes the fake a faithful sizing
     * substitute for the production backend, not just a functional one.
     */
    @Override
    public CacheSizeReport sizeReport() {
        List<byte[]> bucketBlobs = blobsByKey.keySet().stream()
                .filter(CacheKeyConstants::isBucketKey)
                .flatMap(key -> liveBlob(key).stream())
                .map(StoredBlob::blob)
                .toList();
        return new CacheSizeReport(bucketBlobs.stream().mapToLong(blob -> blob.length).sum(), bucketBlobs.size());
    }

    @Override
    public synchronized long flush(CacheScope scope) {
        warmingGeneration = newToken();
        List<String> matchingKeys = blobsByKey.keySet().stream().filter(scope::matches).toList();
        matchingKeys.forEach(blobsByKey::remove);
        revision = newToken();
        return matchingKeys.size();
    }

    /** Test/diagnostic helper: how many buckets are currently stored and not yet expired. */
    public int storedBucketCount() {
        return (int) blobsByKey.keySet().stream().filter(key -> liveBlob(key).isPresent()).count();
    }

    /** Test/diagnostic helper: how many EXISTS round-trips this backend has served. */
    public long existsCallCount() {
        return existsCalls.get();
    }

    private Optional<StoredBlob> liveBlob(String key) {
        StoredBlob stored = blobsByKey.get(key);
        if (stored == null) return Optional.empty();
        if (clock.instant().isBefore(stored.expiresAt())) return Optional.of(stored);
        blobsByKey.remove(key, stored);
        return Optional.empty();
    }

    private Optional<ResultFrame> decodeOrEvict(String key) {
        return liveBlob(key).flatMap(stored -> decodeOrEvict(key, stored.blob()));
    }

    private Optional<ResultFrame> decodeOrEvict(String key, byte[] blob) {
        try {
            return Optional.of(serializer.deserialize(blob));
        } catch (CacheSerializationException corrupt) {
            evict(key);
            return Optional.empty();
        }
    }

    private static String newToken() {
        return java.util.UUID.randomUUID().toString();
    }
}
