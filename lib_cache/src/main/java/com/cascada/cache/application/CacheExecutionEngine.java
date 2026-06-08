package com.cascada.cache.application;

import com.cascada.cache.domain.CacheKeyFactory;
import com.cascada.cache.domain.CanonicalQueryObject;
import com.cascada.cache.domain.GapPlan;
import com.cascada.cache.domain.TimeRange;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.port.CacheBackendPort;
import com.cascada.cache.domain.port.GapQueryRewriterPort;
import com.cascada.cache.domain.port.SparkQueryExecutorPort;
import com.cascada.cache.domain.time.DailyBuckets;
import com.cascada.cache.domain.time.TimeBucketCalculator;
import com.cascada.identity.domain.QueryHash;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The workhorse of the cache, ported from {@code cache_execution_engine.py}: gap analysis, a two-phase
 * EXISTS-then-MGET access pattern, a parallel cache-fetch / Spark-gap fetch, and a final merge.
 *
 * <p>The three fast-path short-circuits of the reference engine are preserved exactly:
 * <ol>
 *   <li>no cacheable body buckets → run the physical SQL directly;</li>
 *   <li>zero buckets present in cache → bypass all cache machinery and run the physical SQL directly;</li>
 *   <li>partial hit → fetch cached buckets and the Spark gap in parallel, then merge.</li>
 * </ol>
 *
 * <p>Parallelism uses virtual threads ({@code asyncio} in the Python becomes
 * {@link CompletableFuture} on a per-task virtual-thread executor).
 */
public final class CacheExecutionEngine {

    private final CacheBackendPort cacheBackend;
    private final SparkQueryExecutorPort sparkExecutor;
    private final GapQueryRewriterPort gapQueryRewriter;
    private final FrameMergeService frameMergeService;
    private final TimeBucketCalculator timeBucketCalculator;
    private final CacheExecutionConfiguration configuration;

    public CacheExecutionEngine(CacheBackendPort cacheBackend, SparkQueryExecutorPort sparkExecutor,
                                GapQueryRewriterPort gapQueryRewriter, CacheExecutionConfiguration configuration) {
        this.cacheBackend = cacheBackend;
        this.sparkExecutor = sparkExecutor;
        this.gapQueryRewriter = gapQueryRewriter;
        this.configuration = configuration;
        this.timeBucketCalculator = new TimeBucketCalculator(configuration.bucketSeconds());
        this.frameMergeService =
                new FrameMergeService(configuration.fixedStepSeconds(), configuration.timeColumnName());
    }

    public ResultFrame execute(CanonicalQueryObject canonicalObject, QueryHash queryHash) {
        long startTimestamp = canonicalObject.timeRange().startTimestampSeconds();
        long endTimestamp = canonicalObject.timeRange().endTimestampSeconds();

        DailyBuckets buckets = timeBucketCalculator.getDailyBuckets(startTimestamp, endTimestamp);
        List<Long> bodyDays = buckets.body();

        List<String> requiredKeys = new ArrayList<>(bodyDays.size());
        for (long dayStart : bodyDays) {
            requiredKeys.add(CacheKeyFactory.buildBucketKey(queryHash, dayStart, configuration.bucketSeconds()));
        }

        // Fast path 1: nothing cacheable -> run the physical SQL directly.
        if (requiredKeys.isEmpty()) {
            return sparkExecutor.execute(canonicalObject.physicalSql());
        }

        List<Boolean> presenceMask = cacheBackend.existsForKeys(requiredKeys);
        List<String> cachedKeys = new ArrayList<>();
        List<Long> missingDays = new ArrayList<>();
        for (int index = 0; index < requiredKeys.size(); index++) {
            if (Boolean.TRUE.equals(presenceMask.get(index))) {
                cachedKeys.add(requiredKeys.get(index));
            } else {
                missingDays.add(bodyDays.get(index));
            }
        }

        // Fast path 2: zero cache hits -> bypass all cache machinery.
        if (cachedKeys.isEmpty()) {
            return sparkExecutor.execute(canonicalObject.physicalSql());
        }

        GapPlan gapPlan = computeGapPlan(startTimestamp, endTimestamp, bodyDays, missingDays);

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            CompletableFuture<List<Optional<ResultFrame>>> cacheFuture =
                    CompletableFuture.supplyAsync(() -> cacheBackend.multiGet(cachedKeys), executor);

            CompletableFuture<ResultFrame> sparkFuture = gapPlan.hasGaps()
                    ? CompletableFuture.supplyAsync(
                            () -> sparkExecutor.execute(gapQueryRewriter.buildGapQuery(
                                    canonicalObject.physicalSql(), gapPlan)), executor)
                    : CompletableFuture.completedFuture(ResultFrame.empty());

            List<ResultFrame> allFrames = new ArrayList<>();
            cacheFuture.join().forEach(maybeFrame -> maybeFrame.ifPresent(allFrames::add));
            ResultFrame sparkFrame = sparkFuture.join();
            if (!sparkFrame.isEmpty()) {
                allFrames.add(sparkFrame);
            }

            return frameMergeService.mergeAndReconstruct(allFrames, canonicalObject);
        }
    }

    /**
     * Ports the engine's own head/tail computation (relative to the body days), distinct from the
     * pure bucket split: head covers {@code [start, firstFullDay-1]} and tail
     * {@code [endOfLastFullDay, end]} when the request spills past the cached whole days.
     */
    private GapPlan computeGapPlan(long startTimestamp, long endTimestamp, List<Long> bodyDays,
                                   List<Long> missingDays) {
        long firstFullDay = bodyDays.get(0);
        long lastFullDay = bodyDays.get(bodyDays.size() - 1);
        long endOfLastFullDay = lastFullDay + configuration.bucketSeconds();

        Optional<TimeRange> head = startTimestamp < firstFullDay
                ? Optional.of(new TimeRange(startTimestamp, Math.min(endTimestamp, firstFullDay - 1)))
                : Optional.empty();
        Optional<TimeRange> tail = endTimestamp >= endOfLastFullDay
                ? Optional.of(new TimeRange(Math.max(startTimestamp, endOfLastFullDay), endTimestamp))
                : Optional.empty();

        return new GapPlan(head, missingDays, tail);
    }
}
