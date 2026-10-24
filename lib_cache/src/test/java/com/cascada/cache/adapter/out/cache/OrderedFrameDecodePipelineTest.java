package com.cascada.cache.adapter.out.cache;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderedFrameDecodePipelineTest {

    @Test
    void finishesDecodesOutOfOrderButVisitsOnProducerInInputOrder() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            CountDownLatch secondTaskCompleted = new CountDownLatch(1);
            AtomicInteger submittedTasks = new AtomicInteger();
            Executor observingExecutor = task -> {
                int submission = submittedTasks.getAndIncrement();
                executor.execute(() -> {
                    task.run();
                    if (submission == 1) secondTaskCompleted.countDown();
                });
            };
            AtomicBoolean secondFinishedBeforeFirst = new AtomicBoolean();
            OrderedFrameDecodePipeline pipeline = new OrderedFrameDecodePipeline(observingExecutor, 3, 1_000);
            Thread producer = Thread.currentThread();
            List<Integer> visited = new ArrayList<>();
            List<Long> values = new ArrayList<>();

            pipeline.visit(20, 3, index -> envelope(100, index + 1), blob -> {
                int marker = Byte.toUnsignedInt(blob[4]);
                if (marker == 1) {
                    await(secondTaskCompleted);
                    secondFinishedBeforeFirst.set(true);
                }
                return frame(marker);
            }, (index, outcome) -> {
                assertSame(producer, Thread.currentThread());
                visited.add(index);
                values.add(outcome.frame().orElseThrow().longAt(0, 0));
            }, new DecodeTaskRegistry(executor));

            assertTrue(secondFinishedBeforeFirst.get());
            assertEquals(List.of(20, 21, 22), visited);
            assertEquals(List.of(1L, 2L, 3L), values);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void ipcBudgetLimitsConcurrentDecodesAndAllowsOneOversizedFrameAtATime() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            AtomicInteger active = new AtomicInteger();
            AtomicInteger maximumActive = new AtomicInteger();
            CountDownLatch twoFramesActive = new CountDownLatch(2);
            OrderedFrameDecodePipeline bounded = new OrderedFrameDecodePipeline(executor, 3, 200);

            bounded.visit(0, 3, index -> envelope(100, index), blob -> {
                int current = active.incrementAndGet();
                maximumActive.accumulateAndGet(current, Math::max);
                twoFramesActive.countDown();
                try {
                    await(twoFramesActive);
                    return frame(Byte.toUnsignedInt(blob[4]));
                } finally {
                    active.decrementAndGet();
                }
            }, (index, outcome) -> { }, new DecodeTaskRegistry(executor));

            assertEquals(2, maximumActive.get());
            assertEquals(100L, OrderedFrameDecodePipeline.estimateDecodedIpcBytes(envelope(100, 1)));

            AtomicInteger oversizedActive = new AtomicInteger();
            AtomicInteger maximumOversizedActive = new AtomicInteger();
            OrderedFrameDecodePipeline oversized = new OrderedFrameDecodePipeline(executor, 3, 50);
            List<Integer> visited = new ArrayList<>();
            oversized.visit(0, 3, index -> envelope(100, index), blob -> {
                int current = oversizedActive.incrementAndGet();
                maximumOversizedActive.accumulateAndGet(current, Math::max);
                try {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("oversized decode was interrupted", interrupted);
                    }
                    return frame(Byte.toUnsignedInt(blob[4]));
                } finally {
                    oversizedActive.decrementAndGet();
                }
            }, (index, outcome) -> visited.add(index), new DecodeTaskRegistry(executor));

            assertEquals(1, maximumOversizedActive.get());
            assertEquals(List.of(0, 1, 2), visited);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void frameCountBudgetCapsOutstandingDecodes() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            AtomicInteger active = new AtomicInteger();
            AtomicInteger maximumActive = new AtomicInteger();
            CountDownLatch twoFramesActive = new CountDownLatch(2);
            OrderedFrameDecodePipeline pipeline = new OrderedFrameDecodePipeline(executor, 2, Long.MAX_VALUE);

            pipeline.visit(0, 5, index -> envelope(10, index), blob -> {
                int current = active.incrementAndGet();
                maximumActive.accumulateAndGet(current, Math::max);
                twoFramesActive.countDown();
                try {
                    if (Byte.toUnsignedInt(blob[4]) < 2) await(twoFramesActive);
                    return frame(Byte.toUnsignedInt(blob[4]));
                } finally {
                    active.decrementAndGet();
                }
            }, (index, outcome) -> { }, new DecodeTaskRegistry(executor));

            assertEquals(2, maximumActive.get());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void visitorFailureCancelsOtherSubmittedDecodesAndStopsOrderedDelivery() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            CountDownLatch secondStarted = new CountDownLatch(1);
            CountDownLatch secondInterrupted = new CountDownLatch(1);
            OrderedFrameDecodePipeline pipeline = new OrderedFrameDecodePipeline(executor, 2, 1_000);
            IllegalArgumentException visitorFailure = new IllegalArgumentException("stop visiting");
            List<Integer> visited = new ArrayList<>();

            IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () ->
                    pipeline.visit(0, 3, index -> envelope(100, index), blob -> {
                        int marker = Byte.toUnsignedInt(blob[4]);
                        if (marker == 1) {
                            secondStarted.countDown();
                            try {
                                new CountDownLatch(1).await();
                            } catch (InterruptedException cancelled) {
                                secondInterrupted.countDown();
                                Thread.currentThread().interrupt();
                            }
                        }
                        return frame(marker);
                    }, (index, outcome) -> {
                        visited.add(index);
                        await(secondStarted);
                        throw visitorFailure;
                    }, new DecodeTaskRegistry(executor)));

            assertSame(visitorFailure, thrown);
            assertEquals(List.of(0), visited);
            assertTrue(secondInterrupted.await(5, TimeUnit.SECONDS));
            assertFalse(executor.isShutdown());
        } finally {
            executor.shutdownNow();
        }
    }

    private static byte[] envelope(int ipcBytes, int marker) {
        return ByteBuffer.allocate(5).order(ByteOrder.BIG_ENDIAN).putInt(ipcBytes).put((byte) marker).array();
    }

    private static ResultFrame frame(long value) {
        return ResultFrame.builder().column("value", ColumnType.LONG).row(value).build();
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) throw new AssertionError("timed out waiting for decode task");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("decode test was interrupted", interrupted);
        }
    }
}
