package com.cascada.cache.adapter.out.cache;

import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.exception.CacheSerializationException;
import com.cascada.cache.application.execution.QueryCancellation;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.IntFunction;

/**
 * Decodes a chunk with a bounded number of outstanding frames and delivers results in input order.
 * The byte budget uses the cache envelope's four-byte uncompressed IPC length as an estimate; it does
 * not bound the larger heap representation of strings, decimals, Arrow vectors, or {@link ResultFrame}.
 * One frame larger than the configured estimate budget is allowed when no other decode is outstanding.
 */
final class OrderedFrameDecodePipeline {

    private final Executor executor;
    private final int maximumInflightDecodedFrames;
    private final long maximumInflightDecodedIpcBytes;

    OrderedFrameDecodePipeline(Executor executor, int maximumInflightDecodedFrames,
                               long maximumInflightDecodedIpcBytes) {
        this.executor = Objects.requireNonNull(executor, "executor");
        if (maximumInflightDecodedFrames < 1) {
            throw new IllegalArgumentException("maximumInflightDecodedFrames must be positive");
        }
        if (maximumInflightDecodedIpcBytes < 1) {
            throw new IllegalArgumentException("maximumInflightDecodedIpcBytes must be positive");
        }
        this.maximumInflightDecodedFrames = maximumInflightDecodedFrames;
        this.maximumInflightDecodedIpcBytes = maximumInflightDecodedIpcBytes;
    }

    /**
     * Decodes up to the configured in-flight bounds from {@code blobAtIndex}, then visits every
     * result in order on the calling thread. A null blob is delivered as missing and consumes no
     * decode slot. Decoder corruption is returned to the caller so adapter side effects such as
     * eviction stay ordered with callback delivery.
     */
    void visit(int startIndex, int itemCount, IntFunction<byte[]> blobAtIndex,
               Function<byte[], ResultFrame> decoder,
               BiConsumer<Integer, DecodeOutcome> visitor,
               DecodeTaskRegistry taskRegistry) throws InterruptedException {
        if (itemCount < 0) throw new IllegalArgumentException("itemCount must not be negative");
        Objects.requireNonNull(blobAtIndex, "blobAtIndex");
        Objects.requireNonNull(decoder, "decoder");
        Objects.requireNonNull(visitor, "visitor");
        Objects.requireNonNull(taskRegistry, "taskRegistry");

        @SuppressWarnings("unchecked")
        FutureTask<DecodeOutcome>[] tasks = (FutureTask<DecodeOutcome>[]) new FutureTask<?>[itemCount];
        long[] estimatedBytes = new long[itemCount];
        boolean[] missing = new boolean[itemCount];
        int nextToSchedule = 0;
        int nextToVisit = 0;
        int inflightFrames = 0;
        long inflightIpcBytes = 0L;
        boolean completed = false;

        try {
            while (nextToVisit < itemCount) {
                QueryCancellation.requireCurrentActive();

                while (nextToSchedule < itemCount) {
                    QueryCancellation.requireCurrentActive();
                    int slot = nextToSchedule;
                    byte[] blob = blobAtIndex.apply(slot);
                    if (blob == null) {
                        missing[slot] = true;
                        nextToSchedule++;
                        continue;
                    }
                    if (inflightFrames >= maximumInflightDecodedFrames) break;

                    long estimate = estimateDecodedIpcBytes(blob);
                    if (inflightFrames > 0 && wouldExceedBudget(inflightIpcBytes, estimate)) break;

                    FutureTask<DecodeOutcome> task = new FutureTask<>(() -> decode(blob, decoder));
                    tasks[slot] = task;
                    estimatedBytes[slot] = estimate;
                    inflightFrames++;
                    inflightIpcBytes = saturatingAdd(inflightIpcBytes, estimate);
                    // Record the task before execute so rejection also follows the cancellation path.
                    if (!taskRegistry.register(task)) {
                        throw new CancellationException("query was cancelled while scheduling cache decode");
                    }
                    executor.execute(task);
                    nextToSchedule++;
                }

                int slot = nextToVisit;
                if (missing[slot]) {
                    QueryCancellation.requireCurrentActive();
                    visitor.accept(startIndex + slot, DecodeOutcome.missing());
                    nextToVisit++;
                    continue;
                }

                FutureTask<DecodeOutcome> task = tasks[slot];
                if (task == null) {
                    throw new IllegalStateException("ordered frame decoder stalled before scheduling the next result");
                }
                DecodeOutcome outcome;
                try {
                    outcome = task.get();
                } catch (ExecutionException failure) {
                    throw propagate(failure.getCause());
                } catch (CancellationException cancelled) {
                    throw cancelled;
                }

                // The visitor is synchronous and remains on this producer thread. Keep the estimate
                // reserved until it returns so callback processing cannot overlap another decode slot.
                QueryCancellation.requireCurrentActive();
                visitor.accept(startIndex + slot, outcome);
                tasks[slot] = null;
                taskRegistry.remove(task);
                inflightFrames--;
                inflightIpcBytes -= estimatedBytes[slot];
                nextToVisit++;
            }
            completed = true;
        } finally {
            if (!completed) taskRegistry.cancel();
            cancelOutstanding(tasks, taskRegistry);
        }
    }

    /** Estimates decoded IPC bytes from the shared four-byte big-endian envelope header. */
    static long estimateDecodedIpcBytes(byte[] blob) {
        if (blob == null) return 0L;
        if (blob.length < 4) return Math.max(1, blob.length);
        int declaredLength = ((blob[0] & 0xff) << 24)
                | ((blob[1] & 0xff) << 16)
                | ((blob[2] & 0xff) << 8)
                | (blob[3] & 0xff);
        // A malformed negative length is still decoded and reported as corruption. Charging the
        // maximum estimate merely makes it run alone; it does not change miss or eviction behavior.
        return declaredLength < 0 ? Long.MAX_VALUE : Math.max(1L, declaredLength);
    }

    private boolean wouldExceedBudget(long current, long additional) {
        return additional > maximumInflightDecodedIpcBytes
                || current > maximumInflightDecodedIpcBytes - additional;
    }

    private static long saturatingAdd(long left, long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }

    private static DecodeOutcome decode(byte[] blob, Function<byte[], ResultFrame> decoder) {
        try {
            return DecodeOutcome.decoded(Objects.requireNonNull(decoder.apply(blob),
                    "cache serializer returned a null frame"));
        } catch (CacheSerializationException corrupt) {
            return DecodeOutcome.corrupt(corrupt);
        }
    }

    private static RuntimeException propagate(Throwable failure) {
        if (failure instanceof RuntimeException runtimeFailure) return runtimeFailure;
        if (failure instanceof Error error) throw error;
        return new IllegalStateException("cache frame decoder failed", failure);
    }

    private static void cancelOutstanding(FutureTask<?>[] tasks, DecodeTaskRegistry taskRegistry) {
        for (FutureTask<?> task : tasks) {
            if (task != null) {
                try {
                    task.cancel(true);
                } catch (RuntimeException ignored) {
                    // Preserve the callback, interruption, rejection, or decode failure.
                }
                taskRegistry.remove(task);
            }
        }
    }
}
