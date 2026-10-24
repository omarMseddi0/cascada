package com.cascada.cache.application.service.merge;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.frame.ResultFrameBuilder;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TypedFrameOutputParallelismTest {
    private static final int ROW_COUNT = 60_000;
    private static final int PARTITION_COUNT = 4;

    @Test
    void wideOutputMatchesSinglePartitionIncludingNulls() throws Exception {
        ResultFrame input = frame(ROW_COUNT, 30);
        ResultFrame expected = singlePartitionResult(input);

        try (RecordingExecutor executor = new RecordingExecutor(4)) {
            TypedFrameAccumulator accumulator = partitioned(input, executor);
            accumulator.accept(input);
            executor.awaitQuiescence();
            executor.recordSubmissions();

            ResultFrame actual = accumulator.finish();

            assertSameValues(actual, expected);
        }
    }

    @Test
    void narrowOutputBelowTheRowThresholdMatchesSinglePartition() throws Exception {
        ResultFrame input = frame(ROW_COUNT, 2);
        ResultFrame expected = singlePartitionResult(input);

        try (RecordingExecutor executor = new RecordingExecutor(4)) {
            TypedFrameAccumulator accumulator = partitioned(input, executor);
            accumulator.accept(input);
            executor.awaitQuiescence();
            executor.recordSubmissions();

            ResultFrame actual = accumulator.finish();

            // Only the four partition-sort tasks should be submitted during finish.
            assertThat(executor.recordedSubmissions()).isEqualTo(PARTITION_COUNT);
            assertSameValues(actual, expected);
        }
    }

    @Test
    void smallWideOutputMatchesSinglePartition() throws Exception {
        ResultFrame input = frame(1_000, 30);
        ResultFrame expected = singlePartitionResult(input);

        try (RecordingExecutor executor = new RecordingExecutor(4)) {
            TypedFrameAccumulator accumulator = partitioned(input, executor);
            accumulator.accept(input);
            executor.awaitQuiescence();
            executor.recordSubmissions();

            ResultFrame actual = accumulator.finish();

            assertThat(executor.recordedSubmissions()).isEqualTo(PARTITION_COUNT);
            assertSameValues(actual, expected);
        }
    }

    @Test
    void wideOutputCompletesWithASingleThreadBoundedExecutor() throws Exception {
        ResultFrame input = frame(40_000, 30);
        try (ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(8))) {
            TypedFrameAccumulator accumulator = partitioned(input, executor);
            // Scheduling accept from the executor must not block its only worker on routing work.
            executor.submit(() -> accumulator.accept(input)).get(10, TimeUnit.SECONDS);

            ResultFrame result = accumulator.finish();

            assertThat(result.rowCount()).isEqualTo(40_000);
            assertThat(result.longAt(0, 0)).isZero();
            assertThat(result.longAt(result.rowCount() - 1, 0)).isEqualTo(39_999L);
            assertThat(result.isNullAt(0, result.columnNames().size() - 1)).isTrue();
        }
    }

    private static TypedFrameAccumulator partitioned(ResultFrame input, Executor executor) {
        return new TypedFrameAccumulator(input, List.of("key"), Map.of(), "ts", 300,
                PARTITION_COUNT, executor);
    }

    private static ResultFrame singlePartitionResult(ResultFrame input) {
        TypedFrameAccumulator accumulator = new TypedFrameAccumulator(input, List.of("key"), Map.of(), "ts", 300);
        accumulator.accept(input);
        return accumulator.finish();
    }

    private static ResultFrame frame(int rows, int columnCount) {
        ResultFrameBuilder builder = ResultFrame.builder().expectedRows(rows).column("key", ColumnType.LONG);
        for (int column = 1; column < columnCount; column++) {
            builder.column("measure_" + column, ColumnType.LONG);
        }
        for (int row = 0; row < rows; row++) {
            builder.appendLong(row);
            for (int column = 1; column < columnCount; column++) {
                if ((column == columnCount - 1 && columnCount > 2) || (row + column) % 101 == 0) {
                    builder.appendNull();
                } else {
                    builder.appendLong((long) row * 31 + column);
                }
            }
        }
        return builder.build();
    }

    private static void assertSameValues(ResultFrame actual, ResultFrame expected) {
        assertThat(actual.columnNames()).isEqualTo(expected.columnNames());
        assertThat(actual.columnTypes()).isEqualTo(expected.columnTypes());
        assertThat(actual.rowCount()).isEqualTo(expected.rowCount());
        for (int row = 0; row < expected.rowCount(); row++) {
            for (int column = 0; column < expected.columnNames().size(); column++) {
                boolean expectedNull = expected.isNullAt(row, column);
                if (actual.isNullAt(row, column) != expectedNull) {
                    throw new AssertionError("different null state at row " + row + ", column " + column);
                }
                if (!expectedNull && actual.longAt(row, column) != expected.longAt(row, column)) {
                    throw new AssertionError("different value at row " + row + ", column " + column);
                }
            }
        }
    }

    private static final class RecordingExecutor implements Executor, AutoCloseable {
        private final ThreadPoolExecutor executor;
        private final AtomicInteger outstanding = new AtomicInteger();
        private final AtomicInteger submissionsAfterRecordingStarted = new AtomicInteger();
        private final AtomicBoolean recording = new AtomicBoolean();
        private final Object idleMonitor = new Object();

        private RecordingExecutor(int threads) {
            executor = new ThreadPoolExecutor(threads, threads, 0, TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(16));
        }

        @Override
        public void execute(Runnable command) {
            if (recording.get()) submissionsAfterRecordingStarted.incrementAndGet();
            outstanding.incrementAndGet();
            try {
                executor.execute(() -> {
                    try {
                        command.run();
                    } finally {
                        if (outstanding.decrementAndGet() == 0) {
                            synchronized (idleMonitor) {
                                idleMonitor.notifyAll();
                            }
                        }
                    }
                });
            } catch (RuntimeException failure) {
                if (outstanding.decrementAndGet() == 0) {
                    synchronized (idleMonitor) {
                        idleMonitor.notifyAll();
                    }
                }
                throw failure;
            }
        }

        private void awaitQuiescence() throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            synchronized (idleMonitor) {
                while (outstanding.get() != 0) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) throw new AssertionError("executor did not become idle");
                    TimeUnit.NANOSECONDS.timedWait(idleMonitor, remaining);
                }
            }
        }

        private void recordSubmissions() {
            submissionsAfterRecordingStarted.set(0);
            recording.set(true);
        }

        private int recordedSubmissions() {
            recording.set(false);
            return submissionsAfterRecordingStarted.get();
        }

        @Override
        public void close() throws InterruptedException {
            executor.shutdown();
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) executor.shutdownNow();
        }
    }
}
