package com.cascada.cache.adapter.out.cache;

import com.cascada.cache.application.port.out.CacheBackendPort;
import com.cascada.cache.application.port.out.CacheValueSerializerPort;
import com.cascada.cache.domain.admin.CacheScope;
import com.cascada.cache.domain.admin.CacheSizeReport;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.key.CacheKeyConstants;
import com.cascada.cache.domain.validation.ValidationBaseline;
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
    public void store(String key, ResultFrame frame, Duration timeToLive) {
        blobsByKey.put(key, new StoredBlob(serializer.serialize(frame), clock.instant().plus(timeToLive)));
    }

    @Override
    public void evict(String key) {
        blobsByKey.remove(key);
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
    public long flush(CacheScope scope) {
        List<String> matchingKeys = blobsByKey.keySet().stream().filter(scope::matches).toList();
        matchingKeys.forEach(blobsByKey::remove);
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
        return Optional.ofNullable(blobsByKey.get(key))
                .filter(stored -> clock.instant().isBefore(stored.expiresAt()));
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
}
