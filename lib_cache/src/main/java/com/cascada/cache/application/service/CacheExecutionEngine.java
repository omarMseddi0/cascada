package com.cascada.cache.application.service;

import com.cascada.cache.application.config.CacheExecutionConfiguration;
import com.cascada.cache.application.execution.QueryExecutionScope;
import com.cascada.cache.application.port.out.BucketCachePort;
import com.cascada.cache.application.port.out.CoverageIndexPort;
import com.cascada.cache.application.port.out.DistinctSketchPort;
import com.cascada.cache.application.port.out.GapQueryRewriterPort;
import com.cascada.cache.application.port.out.QueryExecutorPort;
import com.cascada.cache.application.service.cube.CubeShapeCatalog;
import com.cascada.cache.application.service.time.TimeBucketCalculator;
import com.cascada.cache.domain.cube.QueryShape;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.index.BucketCoverageBitmap;
import com.cascada.cache.domain.key.CacheKeyFactory;
import com.cascada.cache.domain.merge.AggregateFunction;
import com.cascada.cache.domain.query.CanonicalQueryObject;
import com.cascada.cache.domain.time.DailyBuckets;
import com.cascada.cache.domain.time.GapPlan;
import com.cascada.cache.domain.time.TimeRange;
import com.cascada.cache.exception.BucketEnumerationLimitExceededException;
import com.cascada.identity.domain.QueryHash;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

/**
 * The workhorse of the cache, ported from {@code cache_execution_engine.py}: gap analysis, a two-phase
 * EXISTS-then-MGET access pattern, a parallel cache-fetch / Spark-gap fetch, and a final merge.
 *
 * <p>Execution paths:
 * <ol>
 *   <li>no cacheable body buckets → run the physical SQL directly;</li>
 *   <li>zero buckets present in cache → time series and global aggregates compute every bucket in one
 *       bucket-labelled Spark job and store them; other grouped queries run the physical SQL directly;</li>
 *   <li>partial hit → fetch cached buckets and the Spark gap in parallel, then merge. When the gap can
 *       be labelled by bucket, the missing complete buckets are stored for the next request.</li>
 * </ol>
 *
 * <p>Cache reads and gap queries run on the injected executor. The composition root owns its
 * capacity and lifecycle; convenience constructors use the shared JDK common pool.
 */
public final class CacheExecutionEngine {

    /** Name of the bucket-start column added to bucket-labelled gap queries. */
    static final String BUCKET_COLUMN = "cascada_bucket_start";

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
     * With a cube catalog: full-window answers are registered by shape, and a later finer query over
     * the same window is served by verified roll-up before any bucket or Spark work. The catalog is
     * advisory — when {@code null} or when it has no verified subsumer, behaviour is unchanged.
     */
    public CacheExecutionEngine(BucketCachePort cacheBackend, QueryExecutorPort sparkExecutor,
                                GapQueryRewriterPort gapQueryRewriter, CacheExecutionConfiguration configuration,
                                CoverageIndexPort coverageIndex, CubeShapeCatalog cubeCatalog) {
        this(cacheBackend, sparkExecutor, gapQueryRewriter, configuration, coverageIndex, cubeCatalog,
                ForkJoinPool.commonPool());
    }

    public CacheExecutionEngine(BucketCachePort cacheBackend, QueryExecutorPort sparkExecutor,
                                GapQueryRewriterPort gapQueryRewriter, CacheExecutionConfiguration configuration,
                                CoverageIndexPort coverageIndex, CubeShapeCatalog cubeCatalog, Executor executor) {
        this(cacheBackend, sparkExecutor, gapQueryRewriter, configuration, coverageIndex, cubeCatalog, executor,
                null);
    }

    /**
     * The full constructor accepts the executor owned by the composition root, and the sketch merger
     * that makes {@code APPROX_COUNT_DISTINCT} buckets mergeable. Without a merger such queries are
     * answered by the physical SQL whenever a merge would be needed.
     */
    public CacheExecutionEngine(BucketCachePort cacheBackend, QueryExecutorPort sparkExecutor,
                                GapQueryRewriterPort gapQueryRewriter, CacheExecutionConfiguration configuration,
                                CoverageIndexPort coverageIndex, CubeShapeCatalog cubeCatalog, Executor executor,
                                DistinctSketchPort distinctSketches) {
        this.cacheBackend = Objects.requireNonNull(cacheBackend, "cacheBackend");
        this.sparkExecutor = Objects.requireNonNull(sparkExecutor, "sparkExecutor");
        this.gapQueryRewriter = Objects.requireNonNull(gapQueryRewriter, "gapQueryRewriter");
        this.coverageIndex = coverageIndex;
        this.cubeCatalog = cubeCatalog;
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.timeBucketCalculator = new TimeBucketCalculator(configuration.bucketSeconds());
        this.frameMergeService = new FrameMergeService(configuration.fixedStepSeconds(),
                configuration.timeColumnName(), distinctSketches, sparkExecutor);
    }

    public ResultFrame execute(CanonicalQueryObject canonicalObject, QueryHash queryHash) {
        requireActive();
        // Cube path: only global aggregates with no deferred ORDER BY/LIMIT — a LIMIT would register
        // a truncated frame and a roll-up would not reapply the ordering. Sketch and AVG measures
        // are not rolled up. The lookup is exact-time-window only, and every roll-up is verified first.
        boolean cubeEligible = cubeCatalog != null
                && !canonicalObject.metadata().isTimeSeries()
                && !canonicalObject.postProcessing().hasLimit()
                && !canonicalObject.postProcessing().hasOrderBy()
                && !hasDerivedMeasures(canonicalObject);
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
            requiredKeys.add(bucketKey(queryHash, dayStart));
        }

        // Fast path 1: nothing cacheable -> run the physical SQL directly.
        if (requiredKeys.isEmpty()) {
            return catalogFullWindowAnswer(cubeEligible, canonicalObject, queryShape,
                    executeDirect(canonicalObject));
        }

        List<Boolean> presenceMask;
        try {
            presenceMask = resolvePresence(queryHash, bodyDays, requiredKeys);
        } catch (RuntimeException cacheFailure) {
            propagateCancellation(cacheFailure);
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

        // Fast path 2: zero cache hits.
        if (cachedKeys.isEmpty()) {
            return catalogFullWindowAnswer(cubeEligible, canonicalObject, queryShape,
                    computeColdWindow(canonicalObject, queryHash, buckets, requiredKeys));
        }

        GapPlan gapPlan = new GapPlan(buckets.head(), missingDays, buckets.tail());
        // A labelled gap lets the missing complete buckets be stored. It is used only where labelling
        // cannot multiply the rows Spark returns (see bucketLabellingIsCheap).
        Optional<String> labelledGapSql = !missingDays.isEmpty() && bucketLabellingIsCheap(canonicalObject)
                ? gapQueryRewriter.buildBucketedGapQuery(canonicalObject.physicalSql(), gapPlan, BUCKET_COLUMN)
                : Optional.empty();

        IncrementalFrameMerge merge = frameMergeService.incremental(canonicalObject);
        List<Long> vanishedDays = new ArrayList<>();
        CompletableFuture<Void> cacheFuture = CompletableFuture.runAsync(QueryExecutionScope.propagate(() -> {
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
            }), executor);

        CompletableFuture<ResultFrame> sparkFuture = gapPlan.hasGaps()
                ? CompletableFuture.supplyAsync(
                        QueryExecutionScope.propagate(() -> executeSql(labelledGapSql.orElseGet(() ->
                                gapQueryRewriter.buildGapQuery(canonicalObject.physicalSql(), gapPlan)))), executor)
                : CompletableFuture.completedFuture(ResultFrame.empty());

        try {
            cacheFuture.join();
        } catch (RuntimeException cacheFailure) {
            propagateCancellation(cacheFailure);
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
                        QueryExecutionScope.propagate(() -> executeSql(gapQueryRewriter.buildGapQuery(
                                canonicalObject.physicalSql(),
                                new GapPlan(Optional.empty(), vanishedDays, Optional.empty())))), executor);

        ResultFrame sparkFrame = sparkFuture.join();
        if (labelledGapSql.isPresent()) {
            try {
                addLabelledGap(canonicalObject, queryHash, gapPlan, sparkFrame, merge);
            } catch (RuntimeException invalidLabels) {
                propagateCancellation(invalidLabels);
                vanishedFuture.cancel(false);
                return executeAuthoritatively(cubeEligible, canonicalObject, queryShape);
            }
        } else if (gapPlan.hasGaps()) {
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
            propagateCancellation(cacheOrMergeFailure);
            // A decoded but corrupt/incompatible cache frame must not make the query unavailable.
            // The full physical query is authoritative and avoids trusting any cache-derived data.
            return executeAuthoritatively(cubeEligible, canonicalObject, queryShape);
        }
    }

    private void addLabelledGap(CanonicalQueryObject canonicalObject, QueryHash queryHash, GapPlan gapPlan,
                                ResultFrame labelled, IncrementalFrameMerge merge) {
        storeAndMergeLabelledRows(canonicalObject, queryHash, gapPlan.body(), gapPlan.head(), gapPlan.tail(),
                labelled, merge);
    }

    /**
     * Splits a bucket-labelled Spark result, stores each requested complete bucket, and feeds every
     * row, including the partial head and tail edges, to the merge.
     */
    private void storeAndMergeLabelledRows(CanonicalQueryObject canonicalObject, QueryHash queryHash,
                                           List<Long> completeBuckets, Optional<TimeRange> head,
                                           Optional<TimeRange> tail, ResultFrame labelled,
                                           IncrementalFrameMerge merge) {
        BucketSplit split = BucketSplit.of(labelled, BUCKET_COLUMN);
        Set<Long> complete = new HashSet<>(completeBuckets);
        requireExpectedBuckets(split, complete, head, tail);
        boolean global = isGlobalAggregate(canonicalObject);
        for (long bucketStart : completeBuckets) {
            // A global aggregate over an empty bucket still returns one SQL row, which a labelled
            // query cannot produce. Such a bucket is simply left uncached.
            boolean emptyGlobalBucket = global && !split.hasRowsFor(bucketStart);
            if (emptyGlobalBucket) {
                continue;
            }
            ResultFrame frame = split.frameFor(bucketStart);
            storeBucket(queryHash, bucketStart, frame);
            merge.add(frame);
        }
        for (long bucketStart : split.bucketStarts()) {
            if (!complete.contains(bucketStart)) {
                merge.add(split.frameFor(bucketStart));
            }
        }
    }

    /** A complete full-window answer becomes a cube candidate for finer queries over the same window. */
    private ResultFrame catalogFullWindowAnswer(boolean cubeEligible, CanonicalQueryObject canonicalObject,
                                                QueryShape queryShape, ResultFrame answer) {
        if (cubeEligible) {
            try {
                cubeCatalog.register(canonicalObject.timeRange(), queryShape, answer);
            } catch (RuntimeException ignored) {
                propagateCancellation(ignored);
                // The cube is an optional accelerator; a catalog failure cannot invalidate a query result.
            }
        }
        return answer;
    }

    /**
     * A cold window is filled with complete bucket ingredients, never with a full-window aggregate
     * under a bucket key, so the next request is a genuine cache hit while partial head/tail buckets
     * stay correct. One bucket-labelled Spark job computes every bucket when the rewriter supports it.
     */
    private ResultFrame computeColdWindow(CanonicalQueryObject canonicalObject, QueryHash queryHash,
                                          DailyBuckets buckets, List<String> keys) {
        if (bucketLabellingIsCheap(canonicalObject)) {
            Optional<String> labelledSql = gapQueryRewriter.buildBucketedGapQuery(canonicalObject.physicalSql(),
                    new GapPlan(buckets.head(), buckets.body(), buckets.tail()), BUCKET_COLUMN);
            if (labelledSql.isPresent()) {
                return computeColdWindowInOneQuery(canonicalObject, queryHash, buckets, labelledSql.get());
            }
        }
        if (canonicalObject.metadata().isTimeSeries()) {
            return computeColdBucketsOneByOne(canonicalObject, queryHash, buckets, keys);
        }
        return executeDirect(canonicalObject);
    }

    private ResultFrame computeColdWindowInOneQuery(CanonicalQueryObject canonicalObject, QueryHash queryHash,
                                                    DailyBuckets buckets, String labelledSql) {
        ResultFrame labelled = executeSql(labelledSql);
        if (isGlobalAggregate(canonicalObject) && labelled.isEmpty()) {
            // No row in the window: SQL still returns one global row (COUNT = 0, SUM = NULL, ...).
            return executeDirect(canonicalObject);
        }
        IncrementalFrameMerge merge = frameMergeService.incremental(canonicalObject);
        try {
            storeAndMergeLabelledRows(canonicalObject, queryHash, buckets.body(), buckets.head(), buckets.tail(),
                    labelled, merge);
            return merge.finish();
        } catch (RuntimeException invalidOrUnmergeable) {
            propagateCancellation(invalidOrUnmergeable);
            return executeDirect(canonicalObject);
        }
    }

    /** Used only when the rewriter cannot label rows by bucket: one Spark job per complete bucket. */
    private ResultFrame computeColdBucketsOneByOne(CanonicalQueryObject canonicalObject, QueryHash queryHash,
                                                   DailyBuckets buckets, List<String> keys) {
        IncrementalFrameMerge merge = frameMergeService.incremental(canonicalObject);
        for (int index = 0; index < buckets.body().size(); index++) {
            long bucketStart = buckets.body().get(index);
            ResultFrame frame = executeSql(gapQueryRewriter.buildGapQuery(canonicalObject.physicalSql(),
                    new GapPlan(Optional.empty(), List.of(bucketStart), Optional.empty())));
            storeBucket(queryHash, bucketStart, frame);
            merge.add(frame);
        }
        GapPlan boundaries = new GapPlan(buckets.head(), List.of(), buckets.tail());
        if (boundaries.hasGaps()) {
            ResultFrame boundaryFrame = executeSql(gapQueryRewriter.buildGapQuery(
                    canonicalObject.physicalSql(), boundaries));
            merge.add(boundaryFrame);
        }
        try {
            return merge.finish();
        } catch (RuntimeException mergeFailure) {
            propagateCancellation(mergeFailure);
            // Only the cache-derived merge is retried. Spark failures above propagate directly, while
            // an incompatible/corrupt cache ingredient falls back to one authoritative full query.
            return executeDirect(canonicalObject);
        }
    }

    /** Cache storage is best-effort: a failed write never discards the freshly computed frame. */
    private void storeBucket(QueryHash queryHash, long bucketStart, ResultFrame frame) {
        try {
            cacheBackend.store(bucketKey(queryHash, bucketStart), frame);
            if (coverageIndex != null) {
                coverageIndex.markCached(queryHash, configuration.bucketSeconds(), bucketStart);
            }
        } catch (RuntimeException ignored) {
            propagateCancellation(ignored);
        }
    }

    /** Every label must be a requested complete bucket or the bucket containing the head/tail edge. */
    private void requireExpectedBuckets(BucketSplit split, Set<Long> complete,
                                        Optional<TimeRange> head, Optional<TimeRange> tail) {
        Set<Long> allowed = new HashSet<>(complete);
        head.ifPresent(range -> allowed.add(bucketStartOf(range.startTimestampSeconds())));
        tail.ifPresent(range -> allowed.add(bucketStartOf(range.startTimestampSeconds())));
        for (long bucketStart : split.bucketStarts()) {
            if (!allowed.contains(bucketStart)) {
                throw new IllegalStateException("gap query returned rows for unrequested bucket " + bucketStart);
            }
        }
    }

    /**
     * Labelling rows by bucket keeps the Spark result the same size for a time series (each row
     * already belongs to one step inside one bucket) and for a global aggregate (one row per bucket).
     * Other GROUP BY queries would return one row per group per bucket instead of one per group.
     */
    private static boolean bucketLabellingIsCheap(CanonicalQueryObject canonicalObject) {
        return canonicalObject.metadata().isTimeSeries() || isGlobalAggregate(canonicalObject);
    }

    private static boolean isGlobalAggregate(CanonicalQueryObject canonicalObject) {
        return canonicalObject.hashComponents().groupBy().isEmpty();
    }

    /** AVG, APPROX_COUNT_DISTINCT and expressions over aggregates are fetched as ingredients. */
    private static boolean hasDerivedMeasures(CanonicalQueryObject canonicalObject) {
        var functions = canonicalObject.metadata().measureAggregates().values();
        return functions.contains(AggregateFunction.DISTINCT_SKETCH)
                || functions.contains(AggregateFunction.AVERAGE)
                || functions.contains(AggregateFunction.COMPOSITE);
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
                propagateCancellation(ignored);
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
                executeDirect(canonicalObject));
    }

    /**
     * Runs the query without cached ingredients. AVG is still fetched as SUM and COUNT and divided by
     * the merge, and distinct-count sketches are still unioned and estimated, so the answer is computed
     * the same way whether or not any bucket was cached.
     */
    private ResultFrame executeDirect(CanonicalQueryObject canonicalObject) {
        if (!hasDerivedMeasures(canonicalObject)) {
            return executeSql(canonicalObject.physicalSql());
        }
        ResultFrame sketches = executeSql(gapQueryRewriter.buildGapQuery(canonicalObject.physicalSql(),
                new GapPlan(Optional.of(canonicalObject.timeRange()), List.of(), Optional.empty())));
        IncrementalFrameMerge merge = frameMergeService.incremental(canonicalObject);
        merge.add(sketches);
        try {
            return merge.finish();
        } catch (RuntimeException mergeFailure) {
            propagateCancellation(mergeFailure);
            return executeSql(canonicalObject.physicalSql());
        }
    }

    private String bucketKey(QueryHash queryHash, long bucketStart) {
        return CacheKeyFactory.buildBucketKey(queryHash, bucketStart, configuration.bucketSeconds());
    }

    private long bucketStartOf(long timestampSeconds) {
        return Math.floorDiv(timestampSeconds, configuration.bucketSeconds()) * configuration.bucketSeconds();
    }

    private ResultFrame executeSql(String sql) {
        requireActive();
        return sparkExecutor.execute(sql);
    }

    private static void requireActive() {
        if (Thread.currentThread().isInterrupted()) {
            throw new java.util.concurrent.CancellationException("Query was interrupted");
        }
        QueryExecutionScope.current().ifPresent(com.cascada.cache.application.execution.QueryCancellation::requireActive);
    }

    private static void propagateCancellation(RuntimeException failure) {
        requireActive();
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof java.util.concurrent.CancellationException cancelled) throw cancelled;
            if (cause instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                throw new java.util.concurrent.CancellationException("Query was interrupted");
            }
        }
    }

}
