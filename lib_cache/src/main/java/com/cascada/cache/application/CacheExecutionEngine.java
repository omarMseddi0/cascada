package com.cascada.cache.application;

import com.cascada.cache.domain.CacheKeyFactory;
import com.cascada.cache.domain.CanonicalQueryObject;
import com.cascada.cache.domain.GapPlan;
import com.cascada.cache.domain.TimeRange;
import com.cascada.cache.domain.cube.CubeShapeCatalog;
import com.cascada.cache.domain.cube.QueryShape;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.index.BucketCoverageBitmap;
import com.cascada.cache.domain.port.CacheBackendPort;
import com.cascada.cache.domain.port.CoverageIndexPort;
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
    private final CoverageIndexPort coverageIndex;
    private final CubeShapeCatalog cubeCatalog;
    private final FrameMergeService frameMergeService;
    private final TimeBucketCalculator timeBucketCalculator;
    private final CacheExecutionConfiguration configuration;

    public CacheExecutionEngine(CacheBackendPort cacheBackend, SparkQueryExecutorPort sparkExecutor,
                                GapQueryRewriterPort gapQueryRewriter, CacheExecutionConfiguration configuration) {
        this(cacheBackend, sparkExecutor, gapQueryRewriter, configuration, null);
    }

    /**
     * With a coverage-bitmap index (plan Appendix J.1): presence is answered from one bitmap fetch
     * instead of N pipelined EXISTS commands. The index is advisory — when it has no bitmap for the
     * family the engine falls back to EXISTS, and a stale present-bit is corrected by the
     * vanished-bucket guard below, so the index can cost latency but never data.
     */
    public CacheExecutionEngine(CacheBackendPort cacheBackend, SparkQueryExecutorPort sparkExecutor,
                                GapQueryRewriterPort gapQueryRewriter, CacheExecutionConfiguration configuration,
                                CoverageIndexPort coverageIndex) {
        this(cacheBackend, sparkExecutor, gapQueryRewriter, configuration, coverageIndex, null);
    }

    /**
     * With a cube catalog (plan §8.12): full-window answers are registered by shape, and a later finer
     * query over the same window is served by verified roll-up before any bucket or Spark work. The
     * catalog is advisory — when {@code null} or when it has no verified subsumer, behaviour is
     * byte-identical to the reference engine.
     */
    public CacheExecutionEngine(CacheBackendPort cacheBackend, SparkQueryExecutorPort sparkExecutor,
                                GapQueryRewriterPort gapQueryRewriter, CacheExecutionConfiguration configuration,
                                CoverageIndexPort coverageIndex, CubeShapeCatalog cubeCatalog) {
        this.cacheBackend = cacheBackend;
        this.sparkExecutor = sparkExecutor;
        this.gapQueryRewriter = gapQueryRewriter;
        this.coverageIndex = coverageIndex;
        this.cubeCatalog = cubeCatalog;
        this.configuration = configuration;
        this.timeBucketCalculator = new TimeBucketCalculator(configuration.bucketSeconds());
        this.frameMergeService =
                new FrameMergeService(configuration.fixedStepSeconds(), configuration.timeColumnName());
    }

    public ResultFrame execute(CanonicalQueryObject canonicalObject, QueryHash queryHash) {
        // Cube path (plan §8.12): only global aggregates with no deferred ORDER BY/LIMIT — a LIMIT
        // would register a truncated frame and a roll-up would not reapply the ordering. The lookup
        // is exact-time-window only, and every roll-up is verified before it is served.
        boolean cubeEligible = cubeCatalog != null
                && !canonicalObject.metadata().isTimeSeries()
                && !canonicalObject.postProcessing().hasLimit()
                && !canonicalObject.postProcessing().hasOrderBy();
        QueryShape queryShape = cubeEligible
                ? CubeShapeCatalog.shapeOf(canonicalObject.hashComponents())
                : null;
        if (cubeEligible) {
            Optional<ResultFrame> cubeAnswer = cubeCatalog.tryAnswer(canonicalObject.timeRange(), queryShape);
            if (cubeAnswer.isPresent()) {
                return cubeAnswer.get();
            }
        }

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
            return catalogFullWindowAnswer(cubeEligible, canonicalObject, queryShape,
                    sparkExecutor.execute(canonicalObject.physicalSql()));
        }

        List<Boolean> presenceMask = resolvePresence(queryHash, bodyDays, requiredKeys);
        List<String> cachedKeys = new ArrayList<>();
        List<Long> cachedDays = new ArrayList<>();
        List<Long> missingDays = new ArrayList<>();
        for (int index = 0; index < requiredKeys.size(); index++) {
            if (Boolean.TRUE.equals(presenceMask.get(index))) {
                cachedKeys.add(requiredKeys.get(index));
                cachedDays.add(bodyDays.get(index));
            } else {
                missingDays.add(bodyDays.get(index));
            }
        }

        // Fast path 2: zero cache hits -> bypass all cache machinery.
        if (cachedKeys.isEmpty()) {
            return catalogFullWindowAnswer(cubeEligible, canonicalObject, queryShape,
                    sparkExecutor.execute(canonicalObject.physicalSql()));
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

            List<Optional<ResultFrame>> cachedFrames = cacheFuture.join();

            // EXISTS->MGET race guard: a bucket present at the EXISTS check can be evicted/expired
            // before the MGET lands, and its day is NOT in the gap plan — silently skipping it
            // would merge an answer missing that day's data. Buckets are independent mergeable
            // ingredients, so the recovery is surgical: re-fetch ONLY the vanished days with one
            // supplemental gap query, never recompute the whole window.
            List<ResultFrame> allFrames = new ArrayList<>();
            List<Long> vanishedDays = new ArrayList<>();
            for (int index = 0; index < cachedFrames.size(); index++) {
                Optional<ResultFrame> maybeFrame = cachedFrames.get(index);
                if (maybeFrame.isPresent()) {
                    allFrames.add(maybeFrame.get());
                } else {
                    vanishedDays.add(cachedDays.get(index));
                }
            }

            // Submit the vanished-day recovery BEFORE joining the main gap query: it depends only on
            // the cache fetch, so running it after sparkFuture.join() would serialize two Spark
            // round-trips where one wall-clock wait suffices.
            CompletableFuture<ResultFrame> vanishedFuture = vanishedDays.isEmpty()
                    ? CompletableFuture.completedFuture(ResultFrame.empty())
                    : CompletableFuture.supplyAsync(
                            () -> sparkExecutor.execute(gapQueryRewriter.buildGapQuery(
                                    canonicalObject.physicalSql(),
                                    new GapPlan(Optional.empty(), vanishedDays, Optional.empty()))), executor);

            ResultFrame sparkFrame = sparkFuture.join();
            if (!sparkFrame.isEmpty()) {
                allFrames.add(sparkFrame);
            }

            ResultFrame vanishedFrame = vanishedFuture.join();
            if (!vanishedFrame.isEmpty()) {
                allFrames.add(vanishedFrame);
            }

            return catalogFullWindowAnswer(cubeEligible, canonicalObject, queryShape,
                    frameMergeService.mergeAndReconstruct(allFrames, canonicalObject));
        }
    }

    /** A complete full-window answer becomes a cube candidate for finer queries over the same window. */
    private ResultFrame catalogFullWindowAnswer(boolean cubeEligible, CanonicalQueryObject canonicalObject,
                                                QueryShape queryShape, ResultFrame answer) {
        if (cubeEligible) {
            cubeCatalog.register(canonicalObject.timeRange(), queryShape, answer);
        }
        return answer;
    }

    /**
     * Phase-1 presence: one coverage-bitmap fetch when the index knows this family (Appendix J.1),
     * otherwise the reference pipelined-EXISTS path. The bitmap is advisory; the vanished-bucket
     * guard in {@link #execute} corrects any stale present-bit, so this can never lose data.
     */
    private List<Boolean> resolvePresence(QueryHash queryHash, List<Long> bodyDays, List<String> requiredKeys) {
        if (coverageIndex != null) {
            java.util.Optional<BucketCoverageBitmap> bitmap =
                    coverageIndex.load(queryHash, configuration.bucketSeconds());
            if (bitmap.isPresent()) {
                return bitmap.get().presenceMask(bodyDays);
            }
        }
        return cacheBackend.existsForKeys(requiredKeys);
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
