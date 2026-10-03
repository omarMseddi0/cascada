package com.cascada.cache.application.service;

import com.cascada.cache.application.config.CacheExecutionConfiguration;
import com.cascada.cache.application.port.out.BucketCachePort;
import com.cascada.cache.application.port.out.CoverageIndexPort;
import com.cascada.cache.application.port.out.GapQueryRewriterPort;
import com.cascada.cache.application.port.out.QueryExecutorPort;
import com.cascada.cache.domain.cube.CubeShapeCatalog;
import com.cascada.cache.domain.cube.QueryShape;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.index.BucketCoverageBitmap;
import com.cascada.cache.domain.key.CacheKeyFactory;
import com.cascada.cache.domain.query.CanonicalQueryObject;
import com.cascada.cache.domain.time.BucketEnumerationLimitExceededException;
import com.cascada.cache.domain.time.DailyBuckets;
import com.cascada.cache.domain.time.GapPlan;
import com.cascada.cache.domain.time.TimeBucketCalculator;
import com.cascada.identity.domain.QueryHash;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

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
 * <p>Cache reads and gap queries run on the injected executor. The composition root owns its
 * capacity and lifecycle; convenience constructors use the shared JDK common pool.
 */
public final class CacheExecutionEngine {

    private final BucketCachePort cacheBackend;
    private final QueryExecutorPort sparkExecutor;
    private final GapQueryRewriterPort gapQueryRewriter;
    private final CoverageIndexPort coverageIndex;
    private final CubeShapeCatalog cubeCatalog;
    private final FrameMergeService frameMergeService;
    private final TimeBucketCalculator timeBucketCalculator;
    private final CacheExecutionConfiguration configuration;
    private final Executor executor;

    public CacheExecutionEngine(BucketCachePort cacheBackend, QueryExecutorPort sparkExecutor,
                                GapQueryRewriterPort gapQueryRewriter, CacheExecutionConfiguration configuration) {
        this(cacheBackend, sparkExecutor, gapQueryRewriter, configuration, null);
    }

    /**
     * With a coverage-bitmap index (plan Appendix J.1): presence is answered from one bitmap fetch
     * instead of N pipelined EXISTS commands. The index is advisory — when it has no bitmap for the
     * family the engine falls back to EXISTS, and a stale present-bit is corrected by the
     * vanished-bucket guard below, so the index can cost latency but never data.
     */
    public CacheExecutionEngine(BucketCachePort cacheBackend, QueryExecutorPort sparkExecutor,
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
    public CacheExecutionEngine(BucketCachePort cacheBackend, QueryExecutorPort sparkExecutor,
                                GapQueryRewriterPort gapQueryRewriter, CacheExecutionConfiguration configuration,
                                CoverageIndexPort coverageIndex, CubeShapeCatalog cubeCatalog) {
        this(cacheBackend, sparkExecutor, gapQueryRewriter, configuration, coverageIndex, cubeCatalog,
                ForkJoinPool.commonPool());
    }

    /**
     * The full constructor accepts the executor owned by the composition root so concurrent requests share
     * one bounded work queue. Existing overloads retain their behavior through the JDK common pool.
     */
    public CacheExecutionEngine(BucketCachePort cacheBackend, QueryExecutorPort sparkExecutor,
                                GapQueryRewriterPort gapQueryRewriter, CacheExecutionConfiguration configuration,
                                CoverageIndexPort coverageIndex, CubeShapeCatalog cubeCatalog, Executor executor) {
        this.cacheBackend = Objects.requireNonNull(cacheBackend, "cacheBackend");
        this.sparkExecutor = Objects.requireNonNull(sparkExecutor, "sparkExecutor");
        this.gapQueryRewriter = Objects.requireNonNull(gapQueryRewriter, "gapQueryRewriter");
        this.coverageIndex = coverageIndex;
        this.cubeCatalog = cubeCatalog;
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.executor = Objects.requireNonNull(executor, "executor");
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
                ? CubeShapeCatalog.shapeOf(canonicalObject)
                : null;
        if (cubeEligible) {
            Optional<ResultFrame> cubeAnswer = cubeCatalog.tryAnswer(canonicalObject.timeRange(), queryShape);
            if (cubeAnswer.isPresent()) {
                return cubeAnswer.get();
            }
        }

        long startTimestamp = canonicalObject.timeRange().startTimestampSeconds();
        long endTimestamp = canonicalObject.timeRange().endTimestampSeconds();

        DailyBuckets buckets;
        try {
            buckets = timeBucketCalculator.getDailyBuckets(startTimestamp, endTimestamp);
        } catch (BucketEnumerationLimitExceededException excessiveWindow) {
            // A large query is still valid SQL; bypass the cache plan before any bucket list or keys
            // are allocated and let the authoritative executor handle the requested window.
            return executeAuthoritatively(cubeEligible, canonicalObject, queryShape);
        }
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

        List<Boolean> presenceMask;
        try {
            presenceMask = resolvePresence(queryHash, bodyDays, requiredKeys);
        } catch (RuntimeException cacheFailure) {
            return executeAuthoritatively(cubeEligible, canonicalObject, queryShape);
        }
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
            if (!canonicalObject.metadata().isTimeSeries()) {
                return catalogFullWindowAnswer(cubeEligible, canonicalObject, queryShape,
                        sparkExecutor.execute(canonicalObject.physicalSql()));
            }
            return catalogFullWindowAnswer(cubeEligible, canonicalObject, queryShape,
                    computeAndStoreColdBuckets(canonicalObject, queryHash, buckets, requiredKeys));
        }

        GapPlan gapPlan = new GapPlan(buckets.head(), missingDays, buckets.tail());

        FrameMergeService.IncrementalMerge merge = frameMergeService.incremental(canonicalObject);
        List<Long> vanishedDays = new ArrayList<>();
        CompletableFuture<Void> cacheFuture = CompletableFuture.runAsync(() -> {
                int[] delivered = {0};
                cacheBackend.visitKeys(cachedKeys, (index, frame) -> {
                    if (index == null || index != delivered[0] || index >= cachedKeys.size() || frame == null) {
                        throw new IllegalStateException("cache returned an invalid frame index or null frame entry");
                    }
                    delivered[0]++;
                    if (frame.isPresent()) merge.add(frame.get());
                    else vanishedDays.add(cachedDays.get(index));
                });
                if (delivered[0] != cachedKeys.size()) throw new IllegalStateException("cache returned an invalid number of frames");
            }, executor);

        CompletableFuture<ResultFrame> sparkFuture = gapPlan.hasGaps()
                ? CompletableFuture.supplyAsync(
                        () -> sparkExecutor.execute(gapQueryRewriter.buildGapQuery(
                                canonicalObject.physicalSql(), gapPlan)), executor)
                : CompletableFuture.completedFuture(ResultFrame.empty());

        try {
            cacheFuture.join();
        } catch (RuntimeException cacheFailure) {
            // Prevent a queued gap task from starting after the authoritative fallback begins.
            // A running query is left untouched because the executor port has no cancellation contract.
            sparkFuture.cancel(false);
            return executeAuthoritatively(cubeEligible, canonicalObject, queryShape);
        }

        // EXISTS->MGET race guard: a bucket present at the EXISTS check can be evicted/expired
        // before the MGET lands, and its day is NOT in the gap plan — silently skipping it
        // would merge an answer missing that day's data. Buckets are independent mergeable
        // ingredients, so the recovery is surgical: re-fetch ONLY the vanished days with one
        // supplemental gap query, never recompute the whole window.

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
        if (gapPlan.hasGaps()) {
            merge.add(sparkFrame);
        }

        ResultFrame vanishedFrame = vanishedFuture.join();
        if (!vanishedDays.isEmpty()) {
            merge.add(vanishedFrame);
        }

        try {
            return catalogFullWindowAnswer(cubeEligible, canonicalObject, queryShape,
                    merge.finish());
        } catch (RuntimeException cacheOrMergeFailure) {
            // A decoded but corrupt/incompatible cache frame must not make the query unavailable.
            // The full physical query is authoritative and avoids trusting any cache-derived data.
            return executeAuthoritatively(cubeEligible, canonicalObject, queryShape);
        }
    }

    /** A complete full-window answer becomes a cube candidate for finer queries over the same window. */
    private ResultFrame catalogFullWindowAnswer(boolean cubeEligible, CanonicalQueryObject canonicalObject,
                                                QueryShape queryShape, ResultFrame answer) {
        if (cubeEligible) {
            try {
                cubeCatalog.register(canonicalObject.timeRange(), queryShape, answer);
            } catch (RuntimeException ignored) {
                // The cube is an optional accelerator; a catalog failure cannot invalidate a query result.
            }
        }
        return answer;
    }

    /**
     * A cold cache is filled with complete bucket ingredients, never with a full-window aggregate
     * under a bucket key. This makes the next request a genuine cache hit while preserving head/tail
     * correctness for partial boundary buckets.
     */
    private ResultFrame computeAndStoreColdBuckets(CanonicalQueryObject canonicalObject, QueryHash queryHash,
                                                    DailyBuckets buckets, List<String> keys) {
        FrameMergeService.IncrementalMerge merge = frameMergeService.incremental(canonicalObject);
        for (int index = 0; index < buckets.body().size(); index++) {
            long bucketStart = buckets.body().get(index);
            ResultFrame frame = sparkExecutor.execute(gapQueryRewriter.buildGapQuery(canonicalObject.physicalSql(),
                    new GapPlan(Optional.empty(), List.of(bucketStart), Optional.empty())));
            try {
                cacheBackend.store(keys.get(index), frame);
                if (coverageIndex != null) {
                    coverageIndex.markCached(queryHash, configuration.bucketSeconds(), bucketStart);
                }
            } catch (RuntimeException ignored) {
                // Cache storage is best-effort. Keep the freshly computed authoritative frame below.
            }
            merge.add(frame);
        }
        GapPlan boundaries = new GapPlan(buckets.head(), List.of(), buckets.tail());
        if (boundaries.hasGaps()) {
            ResultFrame boundaryFrame = sparkExecutor.execute(gapQueryRewriter.buildGapQuery(
                    canonicalObject.physicalSql(), boundaries));
            merge.add(boundaryFrame);
        }
        try {
            return merge.finish();
        } catch (RuntimeException mergeFailure) {
            // Only the cache-derived merge is retried. Spark failures above propagate directly, while
            // an incompatible/corrupt cache ingredient falls back to one authoritative full query.
            return sparkExecutor.execute(canonicalObject.physicalSql());
        }
    }

    /**
     * Phase-1 presence: one coverage-bitmap fetch when the index knows this family (Appendix J.1),
     * otherwise the reference pipelined-EXISTS path. The bitmap is advisory; the vanished-bucket
     * guard in {@link #execute} corrects any stale present-bit, so this can never lose data.
     */
    private List<Boolean> resolvePresence(QueryHash queryHash, List<Long> bodyDays, List<String> requiredKeys) {
        if (coverageIndex != null) {
            try {
                Optional<BucketCoverageBitmap> bitmap = coverageIndex.load(queryHash, configuration.bucketSeconds());
                if (bitmap.isPresent()) {
                    List<Boolean> mask = bitmap.get().presenceMask(bodyDays);
                    if (mask != null && mask.size() == bodyDays.size()
                            && mask.stream().noneMatch(Objects::isNull)) {
                        return mask;
                    }
                }
            } catch (RuntimeException ignored) {
                // The index is advisory. A failed/corrupt index falls through to backend EXISTS.
            }
        }
        List<Boolean> mask = cacheBackend.existsForKeys(requiredKeys);
        if (mask == null || mask.size() != requiredKeys.size() || mask.stream().anyMatch(Objects::isNull)) {
            throw new IllegalStateException("cache returned an invalid presence mask");
        }
        return mask;
    }

    private ResultFrame executeAuthoritatively(boolean cubeEligible, CanonicalQueryObject canonicalObject,
                                               QueryShape queryShape) {
        return catalogFullWindowAnswer(cubeEligible, canonicalObject, queryShape,
                sparkExecutor.execute(canonicalObject.physicalSql()));
    }

}
