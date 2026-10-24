package com.cascada.cache.adapter.out.cache;

import com.cascada.cache.application.port.out.CacheValueSerializerPort;
import com.cascada.cache.application.execution.QueryCancellation;
import com.cascada.cache.application.execution.QueryExecutionScope;
import com.cascada.cache.domain.model.admin.CacheSizeReport;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.exception.CacheSerializationException;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.SetArgs;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ValkeyCacheBackendAdapterTest {

    @Test
    void multiGetUsesBoundedPrefetchAndPreservesOrderAndMissingEntries() {
        List<byte[][]> submittedChunks = new ArrayList<>();
        Deque<PendingResponse> unfinished = new ArrayDeque<>();
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maxActive = new AtomicInteger();
        int keyCount = ValkeyCacheBackendAdapter.MGET_CHUNK_SIZE * 2 + 11;
        int expectedCalls = (keyCount + ValkeyCacheBackendAdapter.MGET_CHUNK_SIZE - 1)
                / ValkeyCacheBackendAdapter.MGET_CHUNK_SIZE;
        String missingKey = "k127";

        RedisAsyncCommands<byte[], byte[]> async = proxy(RedisAsyncCommands.class, (proxy, method, args) -> {
            if (!method.getName().equals("mget")) {
                throw new AssertionError("unexpected async call: " + method.getName());
            }
            byte[][] chunk = ((byte[][]) args[0]).clone();
            submittedChunks.add(chunk);
            CompletableFuture<List<KeyValue<byte[], byte[]>>> response = new CompletableFuture<>();
            response.whenComplete((ignored, failure) -> active.decrementAndGet());
            int nowActive = active.incrementAndGet();
            maxActive.accumulateAndGet(nowActive, Math::max);
            unfinished.addLast(new PendingResponse(response, chunk));

            // Simulate a Valkey server replying as the next bounded request arrives.
            if (unfinished.size() > 1 || submittedChunks.size() == expectedCalls) {
                PendingResponse pending = unfinished.removeFirst();
                completeWithMgetResults(pending.future(), pending.keys(), missingKey);
            }
            if (submittedChunks.size() == expectedCalls) {
                while (!unfinished.isEmpty()) {
                    PendingResponse pending = unfinished.removeFirst();
                    completeWithMgetResults(pending.future(), pending.keys(), missingKey);
                }
            }
            return redisFuture(response);
        });

        List<String> keys = new ArrayList<>(keyCount);
        for (int index = 0; index < keyCount; index++) {
            keys.add("k" + index);
        }
        StatefulRedisConnection<byte[], byte[]> connection = connection(async, unused -> null);
        ValkeyCacheBackendAdapter adapter = new ValkeyCacheBackendAdapter(connection, serializer());

        List<java.util.Optional<ResultFrame>> result = adapter.multiGet(keys);

        assertEquals(List.of(128, 128, 11), submittedChunks.stream().map(chunk -> chunk.length).toList());
        assertEquals(2, maxActive.get());
        assertEquals(keyCount, result.size());
        assertTrue(result.get(127).isEmpty());
        assertEquals(126L, result.get(126).orElseThrow().longAt(0, 0));
        assertEquals(128L, result.get(128).orElseThrow().longAt(0, 0));
        assertEquals(266L, result.get(266).orElseThrow().longAt(0, 0));
    }

    @Test
    void emptyRequestsDoNotSendRedisCommands() {
        AtomicInteger calls = new AtomicInteger();
        RedisAsyncCommands<byte[], byte[]> async = proxy(RedisAsyncCommands.class, (proxy, method, args) -> {
            calls.incrementAndGet();
            throw new AssertionError("empty request must not issue " + method.getName());
        });
        StatefulRedisConnection<byte[], byte[]> connection = connection(async, unused -> null);
        ValkeyCacheBackendAdapter adapter = new ValkeyCacheBackendAdapter(connection, serializer());

        assertTrue(adapter.multiGet(List.of()).isEmpty());
        assertTrue(adapter.existsForKeys(List.of()).isEmpty());
        assertEquals(0, calls.get());
    }

    @Test
    void sizeReportPipelinesMemoryUsageAndBatchesFallbackLengths() {
        List<byte[]> keys = List.of(
                bytes("QC:V4:B60:query:0"),
                bytes("QC:V4:B60:query:60"),
                bytes("QC:V4:B60:query:120"),
                bytes("QT:V1:TOP"));
        KeyScanCursor<byte[]> scanPage = new KeyScanCursor<>();
        scanPage.getKeys().addAll(keys);
        scanPage.setCursor("0");
        scanPage.setFinished(true);
        RedisCommands<byte[], byte[]> sync = proxy(RedisCommands.class, (proxy, method, args) -> {
            if (method.getName().equals("scan")) {
                return scanPage;
            }
            throw new AssertionError("unexpected sync call: " + method.getName());
        });
        AtomicInteger memorySubmitted = new AtomicInteger();
        AtomicInteger firstMemoryAwaitSaw = new AtomicInteger();
        AtomicInteger lengthSubmitted = new AtomicInteger();
        AtomicInteger firstLengthAwaitSaw = new AtomicInteger();
        RedisAsyncCommands<byte[], byte[]> async = proxy(RedisAsyncCommands.class, (proxy, method, args) -> {
            String name = method.getName();
            byte[] key = (byte[]) args[0];
            if (name.equals("memoryUsage")) {
                int call = memorySubmitted.incrementAndGet();
                if (call == 1) {
                    return redisFuture(CompletableFuture.failedFuture(
                            new RedisCommandExecutionException("NOPERM MEMORY USAGE")),
                            invoked -> {
                                if (invoked.equals("get")) {
                                    firstMemoryAwaitSaw.compareAndSet(0, memorySubmitted.get());
                                }
                            });
                }
                return redisFuture(CompletableFuture.completedFuture(call == 2 ? 20L : 30L),
                        invoked -> {
                            if (invoked.equals("get")) {
                                firstMemoryAwaitSaw.compareAndSet(0, memorySubmitted.get());
                            }
                        });
            }
            if (name.equals("strlen")) {
                lengthSubmitted.incrementAndGet();
                return redisFuture(CompletableFuture.completedFuture(12L), invoked -> {
                    if (invoked.equals("get")) {
                        firstLengthAwaitSaw.compareAndSet(0, lengthSubmitted.get());
                    }
                });
            }
            throw new AssertionError("unexpected async call: " + name + " for " + text(key));
        });
        StatefulRedisConnection<byte[], byte[]> connection = connection(async, unused -> sync);
        ValkeyCacheBackendAdapter adapter = new ValkeyCacheBackendAdapter(connection, serializer());

        CacheSizeReport report = adapter.sizeReport();

        assertEquals(new CacheSizeReport(62L, 3L), report);
        assertEquals(3, memorySubmitted.get());
        assertEquals(3, firstMemoryAwaitSaw.get());
        assertEquals(1, lengthSubmitted.get());
        assertEquals(1, firstLengthAwaitSaw.get());
    }

    @Test
    void timedOutMgetCancelsOutstandingFuture() {
        AtomicReference<CompletableFuture<?>> futureReference = new AtomicReference<>();
        RedisAsyncCommands<byte[], byte[]> async = proxy(RedisAsyncCommands.class, (proxy, method, args) -> {
            CompletableFuture<List<KeyValue<byte[], byte[]>>> future = new CompletableFuture<>();
            futureReference.set(future);
            return redisFuture(future);
        });
        ValkeyCacheBackendAdapter adapter = new ValkeyCacheBackendAdapter(
                connection(async, unused -> null), serializer(), Duration.ofMillis(10));

        IllegalStateException failure = org.junit.jupiter.api.Assertions.assertThrows(
                IllegalStateException.class, () -> adapter.multiGet(List.of("k0")));

        assertInstanceOf(java.util.concurrent.TimeoutException.class, failure.getCause());
        assertTrue(futureReference.get().isCancelled());
    }

    @Test
    void failedMgetCancelsTheOtherPrefetchedRequest() {
        List<CompletableFuture<?>> responses = new ArrayList<>();
        RedisAsyncCommands<byte[], byte[]> async = proxy(RedisAsyncCommands.class, (proxy, method, args) -> {
            CompletableFuture<List<KeyValue<byte[], byte[]>>> response = new CompletableFuture<>();
            if (responses.isEmpty()) {
                response.completeExceptionally(new RedisCommandExecutionException("ERR test failure"));
            }
            responses.add(response);
            return redisFuture(response);
        });
        ValkeyCacheBackendAdapter adapter = new ValkeyCacheBackendAdapter(
                connection(async, unused -> null), serializer());
        List<String> keys = new ArrayList<>(ValkeyCacheBackendAdapter.MGET_CHUNK_SIZE + 1);
        for (int index = 0; index <= ValkeyCacheBackendAdapter.MGET_CHUNK_SIZE; index++) {
            keys.add("k" + index);
        }

        IllegalStateException failure = org.junit.jupiter.api.Assertions.assertThrows(
                IllegalStateException.class, () -> adapter.multiGet(keys));

        assertInstanceOf(java.util.concurrent.ExecutionException.class, failure.getCause());
        assertEquals(2, responses.size());
        assertTrue(responses.get(1).isCancelled());
    }

    @Test
    void storeWritesTheValueWithItsTimeToLive() {
        AtomicReference<String> sentArguments = new AtomicReference<>();
        RedisCommands<byte[], byte[]> sync = proxy(RedisCommands.class, (proxy, method, args) -> {
            assertEquals("eval", method.getName());
            assertTrue(((String) args[0]).contains("'PX'"));
            byte[][] keys = (byte[][]) args[2];
            assertEquals("QC:V4:revision", text(keys[1]));
            sentArguments.set(text(((byte[][]) args[3])[1]));
            return 1L;
        });
        CacheValueSerializerPort serializer = new CacheValueSerializerPort() {
            @Override public byte[] serialize(ResultFrame frame) { return bytes("frame"); }
            @Override public ResultFrame deserialize(byte[] blob) { throw new UnsupportedOperationException(); }
        };
        ValkeyCacheBackendAdapter adapter = new ValkeyCacheBackendAdapter(connection(null, unused -> sync), serializer);

        adapter.store("QC:V4:B86400:abc:0", ResultFrame.builder().column("n", ColumnType.LONG).row(1L).build(),
                Duration.ofDays(7));

        assertEquals("604800000", sentArguments.get());
    }

    @Test
    void anUndecodableBlobIsDeliveredAsMissingAndDeleted() {
        List<String> deletedKeys = new ArrayList<>();
        RedisAsyncCommands<byte[], byte[]> async = proxy(RedisAsyncCommands.class, (proxy, method, args) -> {
            if (method.getName().equals("del")) {
                deletedKeys.add(text(((byte[][]) args[0])[0]));
                return redisFuture(CompletableFuture.completedFuture(1L));
            }
            CompletableFuture<List<KeyValue<byte[], byte[]>>> response = new CompletableFuture<>();
            completeWithMgetResults(response, (byte[][]) args[0], "none");
            return redisFuture(response);
        });
        CountDownLatch badDecoded = new CountDownLatch(1);
        CacheValueSerializerPort corruptForBad = new CacheValueSerializerPort() {
            @Override public byte[] serialize(ResultFrame frame) { throw new UnsupportedOperationException(); }
            @Override public ResultFrame deserialize(byte[] blob) {
                if (text(blob).equals("kbad")) {
                    badDecoded.countDown();
                    throw new CacheSerializationException("corrupt", new IllegalStateException());
                }
                try {
                    assertTrue(badDecoded.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("parallel decode was interrupted", interrupted);
                }
                return ResultFrame.builder().column("value", ColumnType.LONG).row(1L).build();
            }
        };
        RedisCommands<byte[], byte[]> sync = proxy(RedisCommands.class, (proxy, method, args) -> {
            assertEquals("eval", method.getName());
            deletedKeys.add(text(((byte[][]) args[2])[0]));
            assertEquals("QC:V4:revision", text(((byte[][]) args[2])[1]));
            return 1L;
        });
        ExecutorService decodeExecutor = Executors.newFixedThreadPool(2);
        ValkeyCacheBackendAdapter adapter = new ValkeyCacheBackendAdapter(
                connection(async, unused -> sync), corruptForBad, Duration.ofSeconds(60), decodeExecutor,
                2, Long.MAX_VALUE);
        try {
            List<Integer> visitedIndexes = new ArrayList<>();
            List<java.util.Optional<ResultFrame>> frames = new ArrayList<>();
            adapter.visitKeys(List.of("kgood", "kbad"), (index, frame) -> {
                visitedIndexes.add(index);
                frames.add(frame);
            });

            assertEquals(List.of(0, 1), visitedIndexes);
            assertTrue(frames.get(0).isPresent());
            assertTrue(frames.get(1).isEmpty());
            assertEquals(List.of("kbad"), deletedKeys);
        } finally {
            adapter.close();
            assertFalse(decodeExecutor.isShutdown());
            decodeExecutor.shutdownNow();
        }
    }

    @Test
    void cancellingTheCurrentQueryCancelsDecodeTasksAndWakesTheProducer() throws Exception {
        RedisAsyncCommands<byte[], byte[]> async = proxy(RedisAsyncCommands.class, (proxy, method, args) -> {
            if (!method.getName().equals("mget")) {
                throw new AssertionError("unexpected async call: " + method.getName());
            }
            CompletableFuture<List<KeyValue<byte[], byte[]>>> response = new CompletableFuture<>();
            completeWithMgetResults(response, (byte[][]) args[0], "none");
            return redisFuture(response);
        });
        CountDownLatch decodesStarted = new CountDownLatch(2);
        CountDownLatch decodesInterrupted = new CountDownLatch(2);
        CacheValueSerializerPort blockingSerializer = new CacheValueSerializerPort() {
            @Override public byte[] serialize(ResultFrame frame) { throw new UnsupportedOperationException(); }
            @Override public ResultFrame deserialize(byte[] blob) {
                decodesStarted.countDown();
                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException cancelled) {
                    decodesInterrupted.countDown();
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("decode cancelled", cancelled);
                }
                return ResultFrame.builder().column("value", ColumnType.LONG).row(1L).build();
            }
        };
        ExecutorService decodeExecutor = Executors.newFixedThreadPool(2);
        ExecutorService producer = Executors.newSingleThreadExecutor();
        ValkeyCacheBackendAdapter adapter = new ValkeyCacheBackendAdapter(
                connection(async, unused -> null), blockingSerializer, Duration.ofSeconds(60),
                decodeExecutor, 2, Long.MAX_VALUE);
        QueryCancellation cancellation = new QueryCancellation();
        AtomicInteger callbacks = new AtomicInteger();
        Future<?> visiting = producer.submit(() -> {
            try (QueryExecutionScope ignored = new QueryExecutionScope(cancellation)) {
                adapter.visitKeys(List.of("k0", "k1"), (index, frame) -> callbacks.incrementAndGet());
            }
        });
        try {
            assertTrue(decodesStarted.await(5, TimeUnit.SECONDS));
            cancellation.cancel();

            org.junit.jupiter.api.Assertions.assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> visiting.get(5, TimeUnit.SECONDS));
            assertTrue(decodesInterrupted.await(5, TimeUnit.SECONDS));
            assertEquals(0, callbacks.get());
        } finally {
            adapter.close();
            assertFalse(decodeExecutor.isShutdown());
            decodeExecutor.shutdownNow();
            producer.shutdownNow();
        }
    }


    @Test
    void cancellationDuringMgetSubmissionCancelsTheNewFutureWithoutWaiting() {
        QueryCancellation cancellation = new QueryCancellation();
        CompletableFuture<List<KeyValue<byte[], byte[]>>> response = new CompletableFuture<>();
        AtomicInteger submissions = new AtomicInteger(), callbacks = new AtomicInteger();
        RedisAsyncCommands<byte[], byte[]> async = proxy(RedisAsyncCommands.class, (proxy, method, args) -> {
            if (!method.getName().equals("mget")) throw new AssertionError(method.getName());
            submissions.incrementAndGet();
            cancellation.cancel();
            return redisFuture(response, operation -> {
                if (operation.equals("get")) throw new AssertionError("cancelled query must not wait for this reply");
            });
        });
        try (ValkeyCacheBackendAdapter adapter = new ValkeyCacheBackendAdapter(connection(async, unused -> null),
                serializer(), Duration.ofSeconds(60));
             QueryExecutionScope ignored = new QueryExecutionScope(cancellation)) {
            org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
                    () -> adapter.visitKeys(List.of("k0"), (index, frame) -> callbacks.incrementAndGet()));
            assertTrue(response.isCancelled());
            assertEquals(1, submissions.get());
            assertEquals(0, callbacks.get());
        }
    }


    @Test
    void parallelPathBoundsCompressedRepliesToTwoKeysPerChunk() {
        List<Integer> sizes = new ArrayList<>();
        RedisAsyncCommands<byte[], byte[]> async = proxy(RedisAsyncCommands.class, (proxy, method, args) -> {
            if (!method.getName().equals("mget")) throw new AssertionError(method.getName());
            byte[][] keys = (byte[][]) args[0];
            sizes.add(keys.length);
            var values = Arrays.stream(keys).map(key -> KeyValue.just(key, key)).toList();
            return redisFuture(CompletableFuture.completedFuture(values));
        });
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try (ValkeyCacheBackendAdapter adapter = new ValkeyCacheBackendAdapter(connection(async, unused -> null),
                serializer(), Duration.ofSeconds(5), pool, 2, 1024, 2)) {
            List<String> keys = java.util.stream.IntStream.range(0, 9).mapToObj(i -> "k" + i).toList();
            var frames = adapter.multiGet(keys);
            assertEquals(List.of(2, 2, 2, 2, 1), sizes);
            assertEquals(9, frames.size());
            for (int i = 0; i < frames.size(); i++) assertEquals(i, frames.get(i).orElseThrow().longAt(0, 0));
        } finally { pool.shutdownNow(); }
    }

    private static void completeWithMgetResults(
            CompletableFuture<List<KeyValue<byte[], byte[]>>> response, byte[][] request, String missingKey) {
        List<KeyValue<byte[], byte[]>> values = Arrays.stream(request).map(key -> {
            String text = text(key);
            return text.equals(missingKey) ? KeyValue.<byte[], byte[]>empty(key)
                    : KeyValue.just(key, bytes(text));
        }).toList();
        response.complete(values);
    }

    private static CacheValueSerializerPort serializer() {
        return new CacheValueSerializerPort() {
            @Override
            public byte[] serialize(ResultFrame frame) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ResultFrame deserialize(byte[] blob) {
                long value = Long.parseLong(text(blob).substring(1));
                return ResultFrame.builder().column("value", ColumnType.LONG).row(value).build();
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static StatefulRedisConnection<byte[], byte[]> connection(
            RedisAsyncCommands<byte[], byte[]> async,
            java.util.function.Function<StatefulRedisConnection<byte[], byte[]>, RedisCommands<byte[], byte[]>> sync) {
        AtomicReference<StatefulRedisConnection<byte[], byte[]>> reference = new AtomicReference<>();
        StatefulRedisConnection<byte[], byte[]> connection = proxy(StatefulRedisConnection.class,
                (proxy, method, args) -> switch (method.getName()) {
                    case "async" -> async;
                    case "sync" -> sync.apply(reference.get());
                    case "close" -> null;
                    case "isMulti" -> false;
                    default -> throw new AssertionError("unexpected connection call: " + method.getName());
                });
        reference.set(connection);
        return connection;
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<?> type, InvocationHandler handler) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler);
    }

    private static <T> RedisFuture<T> redisFuture(CompletableFuture<T> future) {
        return redisFuture(future, unused -> { });
    }

    @SuppressWarnings("unchecked")
    private static <T> RedisFuture<T> redisFuture(CompletableFuture<T> future,
                                                   java.util.function.Consumer<String> observer) {
        return (RedisFuture<T>) Proxy.newProxyInstance(RedisFuture.class.getClassLoader(),
                new Class<?>[]{RedisFuture.class}, (proxy, method, args) -> {
                    if (method.getName().equals("getError")) {
                        return null;
                    }
                    observer.accept(method.getName());
                    if (method.getName().equals("await")) {
                        try {
                            future.get((Long) args[0], (TimeUnit) args[1]);
                            return true;
                        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException
                                 | CancellationException failure) {
                            return !future.isDone();
                        }
                    }
                    try {
                        return method.invoke(future, args);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private record PendingResponse(CompletableFuture<List<KeyValue<byte[], byte[]>>> future, byte[][] keys) { }
}
