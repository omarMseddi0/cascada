package com.cascada.cache.adapter.out.cache;

import com.cascada.cache.domain.admin.CacheScope;
import com.cascada.cache.domain.admin.CacheSizeReport;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.key.CacheKeyConstants;
import com.cascada.cache.application.port.out.CacheBackendPort;
import com.cascada.cache.application.port.out.CacheValueSerializerPort;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.ArrayDeque;
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

        List<Boolean> presence = new ArrayList<>(keys.size());
        try {
            for (RedisFuture<Long> future : existsFutures) {
                Long exists = await(future, "EXISTS");
                presence.add(exists != null && exists > 0);
            }
        } catch (InterruptedException interrupted) {
            cancelAll(existsFutures);
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while awaiting pipelined EXISTS against Valkey",
                    interrupted);
        } catch (ExecutionException | TimeoutException | CancellationException failure) {
            cancelAll(existsFutures);
            throw new IllegalStateException("pipelined EXISTS against Valkey failed", failure);
        } catch (RuntimeException | Error failure) {
            cancelAll(existsFutures);
            throw failure;
        }
        return presence;
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
        long waitNanos = 0L, decodeNanos = 0L;
        Deque<PendingMget> pending = new ArrayDeque<>(MAX_MGET_IN_FLIGHT);
        int nextStart = 0;

        try {
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
                List<KeyValue<byte[], byte[]>> values = await(chunk.future(), "MGET");
                long decoding = monitor ? System.nanoTime() : 0L;
                if (monitor) waitNanos += decoding - waiting;
                if (values == null || values.size() != chunk.keyCount()) {
                    throw new IllegalStateException("Valkey MGET returned an unexpected value count");
                }
                for (int index = 0; index < values.size(); index++) {
                    KeyValue<byte[], byte[]> keyValue = values.get(index);
                    visitor.accept(chunk.startIndex() + index, keyValue.hasValue()
                            ? Optional.of(serializer.deserialize(keyValue.getValue()))
                            : Optional.empty());
                }
                if (monitor) decodeNanos += System.nanoTime() - decoding;
                pending.removeFirst();
            }
        } catch (InterruptedException interrupted) {
            cancelPending(pending);
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while awaiting bounded MGET against Valkey", interrupted);
        } catch (ExecutionException | TimeoutException | CancellationException failure) {
            cancelPending(pending);
            throw new IllegalStateException("bounded MGET against Valkey failed", failure);
        } catch (RuntimeException | Error failure) {
            cancelPending(pending);
            throw failure;
        }
        if (monitor) {
            LOGGER.fine("Valkey fetch: keys=" + keys.size() + " elapsed_ms=" + (System.nanoTime() - started) / 1_000_000.0
                    + " reply_wait_ms=" + waitNanos / 1_000_000.0 + " decode_and_consume_ms=" + decodeNanos / 1_000_000.0);
        }
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
            for (int index = 0; index < bucketKeys.size(); index++) {
                try {
                    Long memoryUsage = await(memoryFutures.get(index), "MEMORY USAGE");
                    if (memoryUsage == null) {
                        fallbackKeys.add(bucketKeys.get(index));
                    } else {
                        totalBytes += memoryUsage;
                    }
                } catch (ExecutionException unsupportedOrDenied) {
                    // Older servers and restricted ACLs can reject MEMORY USAGE. Defer STRLEN
                    // calls until every future in this page has been submitted and observed.
                    fallbackKeys.add(bucketKeys.get(index));
                } catch (InterruptedException interrupted) {
                    cancelAll(memoryFutures);
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted while measuring Valkey cache size",
                            interrupted);
                } catch (TimeoutException | CancellationException failure) {
                    cancelAll(memoryFutures);
                    throw new IllegalStateException("MEMORY USAGE against Valkey failed", failure);
                }
            }

            List<RedisFuture<Long>> lengthFutures = new ArrayList<>(fallbackKeys.size());
            for (byte[] key : fallbackKeys) {
                lengthFutures.add(async.strlen(key));
            }
            for (RedisFuture<Long> future : lengthFutures) {
                try {
                    Long length = await(future, "STRLEN");
                    totalBytes += length == null ? 0L : length;
                } catch (InterruptedException interrupted) {
                    cancelAll(lengthFutures);
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted while measuring Valkey cache size",
                            interrupted);
                } catch (ExecutionException | TimeoutException | CancellationException failure) {
                    cancelAll(lengthFutures);
                    throw new IllegalStateException("STRLEN fallback against Valkey failed", failure);
                }
            }
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

    private <T> T await(RedisFuture<T> future, String command)
            throws InterruptedException, ExecutionException, TimeoutException {
        return future.get(commandTimeout.toMillis(), TimeUnit.MILLISECONDS);
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

    private record PendingMget(int startIndex, int keyCount,
                               RedisFuture<List<KeyValue<byte[], byte[]>>> future) { }

    @Override
    public void close() {
        connection.close();
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }
}
