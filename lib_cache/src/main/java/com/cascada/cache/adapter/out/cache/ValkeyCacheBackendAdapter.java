package com.cascada.cache.adapter.out.cache;

import com.cascada.cache.domain.admin.CacheKeyTenantSegment;
import com.cascada.cache.domain.admin.CacheScope;
import com.cascada.cache.domain.admin.CacheSizeReport;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.application.port.out.CacheBackendPort;
import com.cascada.cache.application.port.out.CacheValueSerializerPort;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The production hot-tier backend: a {@link CacheBackendPort} over Valkey/Redis via Lettuce, ported
 * from the Redis access in {@code cache_execution_engine.py}. It uses the documented key contract
 * ({@code QC:V4:...} rendered by {@code CacheKeyFactory}) and the same two-phase pattern — a pipelined
 * {@code EXISTS} for sub-millisecond gap analysis, then a bulk {@code MGET} of the present buckets.
 *
 * <p>Binary blobs are stored under a {@link ByteArrayCodec}. Frames are serialized through the injected
 * {@link CacheValueSerializerPort}, as they are in {@link InMemoryBlobCacheBackendAdapter}.
 */
public final class ValkeyCacheBackendAdapter implements CacheBackendPort, AutoCloseable {

    private final RedisClient redisClient;
    private final StatefulRedisConnection<byte[], byte[]> connection;
    private final CacheValueSerializerPort serializer;

    public ValkeyCacheBackendAdapter(String redisUniformResourceIdentifier, CacheValueSerializerPort serializer) {
        RedisClient client = RedisClient.create(redisUniformResourceIdentifier);
        StatefulRedisConnection<byte[], byte[]> connected;
        try {
            connected = client.connect(ByteArrayCodec.INSTANCE);
        } catch (RuntimeException | Error connectionFailure) {
            try {
                client.shutdown();
            } catch (RuntimeException cleanupFailure) {
                connectionFailure.addSuppressed(cleanupFailure);
            }
            throw connectionFailure;
        }
        this.redisClient = client;
        this.connection = connected;
        this.serializer = serializer;
    }

    @Override
    public List<Boolean> existsForKeys(List<String> keys) {
        // Async commands are pipelined by Lettuce without waiting for replies, so N EXISTS still
        // cost ~one round trip. Crucially this never calls setAutoFlushCommands(false): that flag
        // is CONNECTION-wide shared state, and the engine issues MGET/SET from parallel virtual
        // threads over this same connection — a concurrent writer would have had its commands
        // silently buffered (stalled) until this reader flushed, or flushed mid-batch.
        RedisAsyncCommands<byte[], byte[]> async = connection.async();
        List<RedisFuture<Long>> existsFutures = new ArrayList<>(keys.size());
        for (String key : keys) {
            existsFutures.add(async.exists(toBytes(key)));
        }

        List<Boolean> presence = new ArrayList<>(keys.size());
        try {
            for (RedisFuture<Long> future : existsFutures) {
                Long exists = future.get();
                presence.add(exists != null && exists > 0);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while awaiting pipelined EXISTS against Valkey",
                    interrupted);
        } catch (Exception failure) {
            throw new IllegalStateException("pipelined EXISTS against Valkey failed", failure);
        }
        return presence;
    }

    @Override
    public List<Optional<ResultFrame>> multiGet(List<String> keys) {
        RedisCommands<byte[], byte[]> sync = connection.sync();
        byte[][] keyBytes = keys.stream().map(this::toBytes).toArray(byte[][]::new);
        List<io.lettuce.core.KeyValue<byte[], byte[]>> values = sync.mget(keyBytes);

        List<Optional<ResultFrame>> frames = new ArrayList<>(keys.size());
        for (var keyValue : values) {
            frames.add(keyValue.hasValue()
                    ? Optional.of(serializer.deserialize(keyValue.getValue()))
                    : Optional.empty());
        }
        return frames;
    }

    @Override
    public void store(String key, ResultFrame frame) {
        connection.sync().set(toBytes(key), serializer.serialize(frame));
    }

    /**
     * Sums real stored bytes by walking the keyspace with {@code SCAN} (non-blocking, unlike
     * {@code KEYS}) and asking Valkey for each key's heap footprint via {@code MEMORY USAGE} — the
     * authoritative server-side size including the value, key, and per-entry overhead. If a server
     * does not support {@code MEMORY USAGE} the value's {@code STRLEN} is used as a floor.
     */
    @Override
    public CacheSizeReport sizeReport() {
        RedisCommands<byte[], byte[]> sync = connection.sync();
        long totalBytes = 0L;
        long bucketCount = 0L;
        Map<String, Long> bytesByTenant = new HashMap<>();
        Map<String, Long> bucketCountByTenant = new HashMap<>();

        ScanCursor cursor = ScanCursor.INITIAL;
        do {
            KeyScanCursor<byte[]> page = sync.scan(cursor, ScanArgs.Builder.limit(512));
            for (byte[] keyBytes : page.getKeys()) {
                String key = new String(keyBytes, StandardCharsets.UTF_8);
                if (!CacheKeyTenantSegment.isBucketKey(key)) {
                    continue;
                }
                long bytes = measureBytes(sync, keyBytes);
                totalBytes += bytes;
                bucketCount++;
                String tenant = CacheKeyTenantSegment.of(key);
                bytesByTenant.merge(tenant, bytes, Long::sum);
                bucketCountByTenant.merge(tenant, 1L, Long::sum);
            }
            cursor = page;
        } while (!cursor.isFinished());

        return new CacheSizeReport(totalBytes, bucketCount, bytesByTenant, bucketCountByTenant);
    }

    private long measureBytes(RedisCommands<byte[], byte[]> sync, byte[] keyBytes) {
        try {
            Long memoryUsage = sync.memoryUsage(keyBytes);
            if (memoryUsage != null) {
                return memoryUsage;
            }
        } catch (RuntimeException unsupportedOrDenied) {
            // ACL-restricted and older servers can reject MEMORY USAGE; STRLEN is a safe value-byte floor.
        }
        // STRLEN, not GET: the fallback only needs the value's size, and GET would drag the whole
        // blob over the network once per key during a keyspace walk.
        Long length = sync.strlen(keyBytes);
        return length == null ? 0L : length;
    }

    /**
     * Purges every key in {@code scope} using a {@code SCAN} + {@code DEL} sweep (never the
     * O(N)-blocking {@code KEYS}/{@code FLUSHDB}, which would stall the shard and ignore tenant scoping).
     * Each scanned key is checked against the requested scope before deletion; a flush-all scope matches
     * only well-formed Cascada bucket keys, so other keys in a shared database are preserved.
     */
    @Override
    public long flush(CacheScope scope) {
        RedisCommands<byte[], byte[]> sync = connection.sync();
        ScanArgs scanArgs = ScanArgs.Builder.limit(512);

        long purged = 0L;
        ScanCursor cursor = ScanCursor.INITIAL;
        do {
            KeyScanCursor<byte[]> page = sync.scan(cursor, scanArgs);
            List<byte[]> keys = page.getKeys().stream()
                    .filter(key -> scope.matches(new String(key, StandardCharsets.UTF_8))).toList();
            if (!keys.isEmpty()) {
                purged += sync.del(keys.toArray(new byte[0][]));
            }
            cursor = page;
        } while (!cursor.isFinished());
        return purged;
    }

    private byte[] toBytes(String key) {
        return key.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        connection.close();
        redisClient.shutdown();
    }
}
