package com.cascada.cache.adapter.out.cache;

import com.cascada.cache.adapter.out.cache.model.PendingMget;
import com.cascada.cache.application.port.out.CacheBackendPort;
import com.cascada.cache.application.port.out.CacheValueSerializerPort;
import com.cascada.cache.domain.admin.CacheScope;
import com.cascada.cache.domain.model.admin.CacheSizeReport;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.key.CacheKeyConstants;
import com.cascada.cache.domain.model.time.TimeRange;
import com.cascada.cache.domain.model.validation.ValidationBaseline;
import com.cascada.cache.exception.CacheSerializationException;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

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

    private static final String WARMING_GENERATION_KEY = "QC:V4:warming_generation";
    private static final String REVISION_KEY = "QC:V4:revision";
    /** KEYS: bucket, revision, warming generation. ARGV: blob, TTL ms, new revision, expected generation or "". */
    private static final String STORE_SCRIPT =
            "if ARGV[4] ~= '' and redis.call('GET', KEYS[3]) ~= ARGV[4] then return 0 end; "
            + "redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2]); "
            + "redis.call('SET', KEYS[2], ARGV[3]); return 1";
    private static final Logger LOGGER = Logger.getLogger(ValkeyCacheBackendAdapter.class.getName());

    /** Keep compressed MGET replies bounded while allowing one following request to be prefetched. */
    static final int MGET_CHUNK_SIZE = 128;
    static final int MAX_MGET_IN_FLIGHT = 2;
    private static final Duration COMMAND_TIMEOUT = Duration.ofSeconds(60);

    private final RedisClient redisClient;
    private final StatefulRedisConnection<byte[], byte[]> connection;
    private final CacheValueSerializerPort serializer;
    private final Duration commandTimeout;

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
        this.commandTimeout = connected.getTimeout();
    }

    /**
     * Test seam for an already-created connection. The adapter takes ownership and closes the
     * connection from {@link #close()}, but does not own or shut down a client.
     */
    ValkeyCacheBackendAdapter(StatefulRedisConnection<byte[], byte[]> connection,
                              CacheValueSerializerPort serializer) {
        this(connection, serializer, COMMAND_TIMEOUT);
    }

    ValkeyCacheBackendAdapter(StatefulRedisConnection<byte[], byte[]> connection,
                              CacheValueSerializerPort serializer, Duration commandTimeout) {
        this.redisClient = null;
        this.connection = connection;
        this.serializer = serializer;
        this.commandTimeout = commandTimeout;
    }

    @Override
    public List<Boolean> existsForKeys(List<String> keys) {
        if (keys.isEmpty()) {
            return List.of();
        }
        // Async commands are pipelined by Lettuce without waiting for replies, so N EXISTS still
        // cost ~one round trip. Crucially this never calls setAutoFlushCommands(false): that flag
        // is CONNECTION-wide shared state, and the engine issues MGET/SET from parallel worker
        // threads over this same connection — a concurrent writer would have had its commands
        // silently buffered (stalled) until this reader flushed, or flushed mid-batch.
        RedisAsyncCommands<byte[], byte[]> async = connection.async();
        List<RedisFuture<Long>> existsFutures = new ArrayList<>(keys.size());
        for (String key : keys) {
            existsFutures.add(async.exists(toBytes(key)));
        }

        return awaiting("pipelined EXISTS", () -> cancelAll(existsFutures), () -> {
            List<Boolean> presence = new ArrayList<>(keys.size());
            for (RedisFuture<Long> future : existsFutures) {
                Long exists = await(future);
                presence.add(exists != null && exists > 0);
            }
            return presence;
        });
    }

    @Override
    public List<Optional<ResultFrame>> multiGet(List<String> keys) {
        List<Optional<ResultFrame>> frames = new ArrayList<>(keys.size());
        visitKeys(keys, (index, frame) -> frames.add(frame));
        return frames;
    }

    @Override
    public void visitKeys(List<String> keys, java.util.function.BiConsumer<Integer, Optional<ResultFrame>> visitor) {
        if (keys.isEmpty()) {
            return;
        }

        RedisAsyncCommands<byte[], byte[]> async = connection.async();
        boolean monitor = LOGGER.isLoggable(Level.FINE);
        long started = monitor ? System.nanoTime() : 0L;
        long[] nanos = new long[2]; // reply wait, decode and consume
        Deque<PendingMget> pending = new ArrayDeque<>(MAX_MGET_IN_FLIGHT);

        awaiting("bounded MGET", () -> cancelPending(pending), () -> {
            int nextStart = 0;
            while (nextStart < keys.size() || !pending.isEmpty()) {
                while (nextStart < keys.size() && pending.size() < MAX_MGET_IN_FLIGHT) {
                    int chunkSize = Math.min(MGET_CHUNK_SIZE, keys.size() - nextStart);
                    byte[][] chunkKeys = new byte[chunkSize][];
                    for (int index = 0; index < chunkSize; index++) {
                        chunkKeys[index] = toBytes(keys.get(nextStart + index));
                    }
                    RedisFuture<List<KeyValue<byte[], byte[]>>> future = async.mget(chunkKeys);
                    pending.addLast(new PendingMget(nextStart, chunkSize, future));
                    nextStart += chunkSize;
                }

                PendingMget chunk = pending.peekFirst();
                long waiting = monitor ? System.nanoTime() : 0L;
                List<KeyValue<byte[], byte[]>> values = await(chunk.future());
                long decoding = monitor ? System.nanoTime() : 0L;
                if (monitor) nanos[0] += decoding - waiting;
                if (values == null || values.size() != chunk.keyCount()) {
                    throw new IllegalStateException("Valkey MGET returned an unexpected value count");
                }
                for (int index = 0; index < values.size(); index++) {
                    KeyValue<byte[], byte[]> keyValue = values.get(index);
                    visitor.accept(chunk.startIndex() + index, keyValue.hasValue()
                            ? decodeOrEvict(keys.get(chunk.startIndex() + index), keyValue.getValue())
                            : Optional.empty());
                }
                if (monitor) nanos[1] += System.nanoTime() - decoding;
                pending.removeFirst();
            }
            return null;
        });
        if (monitor) {
            LOGGER.fine("Valkey fetch: keys=" + keys.size() + " elapsed_ms=" + (System.nanoTime() - started) / 1_000_000.0
                    + " reply_wait_ms=" + nanos[0] / 1_000_000.0 + " decode_and_consume_ms=" + nanos[1] / 1_000_000.0);
        }
    }

    /**
     * A corrupt blob is reported as absent: the engine's vanished-bucket guard recomputes only that bucket,
     * and the delete lets the warmer rewrite it instead of skipping a key that merely exists.
     */
    private Optional<ResultFrame> decodeOrEvict(String key, byte[] blob) {
        try {
            return Optional.of(serializer.deserialize(blob));
        } catch (CacheSerializationException corrupt) {
            LOGGER.warning("evicting undecodable cache bucket " + key);
            evict(key);
            return Optional.empty();
        }
    }

    @Override
    public void store(String key, ResultFrame frame, Duration timeToLive) {
        storeAtomically(key, frame, timeToLive, "");
    }

    @Override
    public void evict(String key) {
        connection.sync().eval("redis.call('DEL', KEYS[1]); redis.call('SET', KEYS[2], ARGV[1]); return 1",
                io.lettuce.core.ScriptOutputType.INTEGER, new byte[][] {toBytes(key), toBytes(REVISION_KEY)},
                newToken());
    }

    @Override public Optional<String> warmingGeneration() { return token(WARMING_GENERATION_KEY); }

    @Override public boolean storeForGeneration(String key, ResultFrame frame, Duration ttl, String generation) {
        if (generation.isEmpty()) throw new IllegalArgumentException("warming generation must not be empty");
        return storeAtomically(key, frame, ttl, generation);
    }

    /**
     * One atomic script for every bucket write: an optional warming-generation check, the bucket with
     * its millisecond TTL, and a new revision token so readers see that the cache changed.
     */
    private boolean storeAtomically(String key, ResultFrame frame, Duration timeToLive, String generation) {
        long millis = timeToLive.toMillis();
        if (millis <= 0) throw new IllegalArgumentException("bucket TTL must be positive");
        Long stored = connection.sync().eval(STORE_SCRIPT, io.lettuce.core.ScriptOutputType.INTEGER,
                new byte[][] {toBytes(key), toBytes(REVISION_KEY), toBytes(WARMING_GENERATION_KEY)},
                serializer.serialize(frame), toBytes(Long.toString(millis)), newToken(), toBytes(generation));
        return Long.valueOf(1L).equals(stored);
    }

    @Override public Optional<String> revision() { return token(REVISION_KEY); }

    private Optional<String> token(String key) {
        RedisCommands<byte[], byte[]> sync = connection.sync();
        byte[] revision = sync.get(toBytes(key));
        if (revision == null) {
            sync.setnx(toBytes(key), newToken());
            revision = sync.get(toBytes(key));
        }
        return Optional.ofNullable(revision).map(value -> new String(value, StandardCharsets.UTF_8));
    }

    @Override public Optional<Duration> remainingTimeToLive(List<String> keys) {
        long started = System.nanoTime();
        List<RedisFuture<Long>> pending = keys.stream().map(key -> connection.async().pttl(toBytes(key))).toList();
        long minimum = awaiting("PTTL", () -> cancelAll(pending), () -> {
            long least = Long.MAX_VALUE;
            for (RedisFuture<Long> future : pending) {
                Long ttl = await(future);
                if (ttl == null || ttl <= 0) return 0L;
                least = Math.min(least, ttl);
            }
            return least;
        });
        if (minimum <= 0) return Optional.empty();
        long remaining = minimum - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        return !keys.isEmpty() && remaining > 0 ? Optional.of(Duration.ofMillis(remaining)) : Optional.empty();
    }

    @Override
    public Optional<ValidationBaseline> loadValidationBaseline() {
        return Optional.ofNullable(connection.sync().get(toBytes(CacheKeyConstants.VALIDATION_BASELINE_KEY)))
                .map(stored -> new String(stored, StandardCharsets.UTF_8).split(":", 3))
                .filter(parts -> parts.length == 3)
                .flatMap(ValkeyCacheBackendAdapter::parseBaseline);
    }

    @Override
    public void saveValidationBaseline(ValidationBaseline baseline, Duration timeToLive) {
        String encoded = baseline.window().startTimestampSeconds() + ":" + baseline.window().endTimestampSeconds()
                + ":" + baseline.signature();
        connection.sync().set(toBytes(CacheKeyConstants.VALIDATION_BASELINE_KEY),
                encoded.getBytes(StandardCharsets.UTF_8), SetArgs.Builder.ex(timeToLive));
    }

    /** An unreadable baseline counts as missing, which starts a fresh epoch rather than failing warming. */
    private static Optional<ValidationBaseline> parseBaseline(String[] parts) {
        try {
            return Optional.of(new ValidationBaseline(
                    new TimeRange(Long.parseLong(parts[0]), Long.parseLong(parts[1])), parts[2]));
        } catch (IllegalArgumentException unreadable) {
            return Optional.empty();
        }
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
        RedisAsyncCommands<byte[], byte[]> async = connection.async();
        long totalBytes = 0L;
        long bucketCount = 0L;

        ScanCursor cursor = ScanCursor.INITIAL;
        do {
            KeyScanCursor<byte[]> page = sync.scan(cursor, ScanArgs.Builder.limit(512));
            List<byte[]> bucketKeys = new ArrayList<>();
            for (byte[] keyBytes : page.getKeys()) {
                String key = new String(keyBytes, StandardCharsets.UTF_8);
                if (CacheKeyConstants.isBucketKey(key)) {
                    bucketKeys.add(keyBytes);
                }
            }

            // Async commands auto-flush individually. Awaiting them only after submission gives
            // one pipelined page instead of one network round trip per bucket; we deliberately do
            // not disable auto-flush because the connection is shared with concurrent cache reads.
            List<RedisFuture<Long>> memoryFutures = new ArrayList<>(bucketKeys.size());
            for (byte[] key : bucketKeys) {
                memoryFutures.add(async.memoryUsage(key));
            }
            List<byte[]> fallbackKeys = new ArrayList<>();
            totalBytes += awaiting("MEMORY USAGE", () -> cancelAll(memoryFutures), () -> {
                long pageBytes = 0L;
                for (int index = 0; index < bucketKeys.size(); index++) {
                    Long memoryUsage;
                    try {
                        memoryUsage = await(memoryFutures.get(index));
                    } catch (ExecutionException unsupportedOrDenied) {
                        // Older servers and restricted ACLs can reject MEMORY USAGE. Defer STRLEN
                        // calls until every future in this page has been submitted and observed.
                        memoryUsage = null;
                    }
                    if (memoryUsage == null) {
                        fallbackKeys.add(bucketKeys.get(index));
                    } else {
                        pageBytes += memoryUsage;
                    }
                }
                return pageBytes;
            });

            List<RedisFuture<Long>> lengthFutures = new ArrayList<>(fallbackKeys.size());
            for (byte[] key : fallbackKeys) {
                lengthFutures.add(async.strlen(key));
            }
            totalBytes += awaiting("STRLEN fallback", () -> cancelAll(lengthFutures), () -> {
                long pageBytes = 0L;
                for (RedisFuture<Long> future : lengthFutures) {
                    Long length = await(future);
                    pageBytes += length == null ? 0L : length;
                }
                return pageBytes;
            });
            bucketCount += bucketKeys.size();
            cursor = page;
        } while (!cursor.isFinished());

        return new CacheSizeReport(totalBytes, bucketCount);
    }

    /**
     * Purges every key in {@code scope} using a {@code SCAN} + {@code DEL} sweep (never the
     * O(N)-blocking {@code KEYS}/{@code FLUSHDB}, which would stall the shard and ignore key prefixes).
     * Each scanned key is checked against the requested scope before deletion; a flush-all scope matches
     * only well-formed Cascada bucket keys, so other keys in a shared database are preserved.
     */
    @Override
    public long flush(CacheScope scope) {
        RedisCommands<byte[], byte[]> sync = connection.sync();
        ScanArgs scanArgs = ScanArgs.Builder.limit(512);

        sync.set(toBytes(REVISION_KEY), newToken());
        sync.set(toBytes(WARMING_GENERATION_KEY), newToken());
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
        sync.set(toBytes(REVISION_KEY), newToken());
        return purged;
    }

    private byte[] toBytes(String key) {
        return key.getBytes(StandardCharsets.UTF_8);
    }

    private byte[] newToken() {
        return toBytes(java.util.UUID.randomUUID().toString());
    }

    private <T> T await(RedisFuture<T> future)
            throws InterruptedException, ExecutionException, TimeoutException {
        return future.get(commandTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Awaits pipelined replies; on any failure the still-pending futures are cancelled. Interruption is
     * restored, and Lettuce failures are reported with the command that failed.
     */
    private static <R> R awaiting(String command, Runnable cancelPending, ValkeyWait<R> body) {
        try {
            return body.run();
        } catch (InterruptedException interrupted) {
            cancelPending.run();
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while awaiting " + command + " against Valkey", interrupted);
        } catch (ExecutionException | TimeoutException | CancellationException failure) {
            cancelPending.run();
            throw new IllegalStateException(command + " against Valkey failed", failure);
        } catch (RuntimeException | Error failure) {
            cancelPending.run();
            throw failure;
        }
    }

    private static void cancelPending(Iterable<? extends PendingMget> pending) {
        for (PendingMget item : pending) {
            cancelQuietly(item.future());
        }
    }

    private static void cancelAll(Iterable<? extends RedisFuture<?>> futures) {
        for (RedisFuture<?> future : futures) {
            cancelQuietly(future);
        }
    }

    private static void cancelQuietly(RedisFuture<?> future) {
        try {
            future.cancel(true);
        } catch (RuntimeException ignored) {
            // The original timeout, interruption, or command failure is the useful exception.
        }
    }

    @Override
    public void close() {
        connection.close();
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }
}
