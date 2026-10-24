package com.cascada.cache.application.service;

import com.cascada.cache.application.model.config.CacheExecutionConfiguration;
import com.cascada.cache.application.execution.QueryCancellation;
import com.cascada.cache.application.execution.QueryAdmissionBudget;
import com.cascada.cache.application.execution.QueryAdmissionPermit;
import com.cascada.cache.application.execution.QueryExecutionScope;
import com.cascada.cache.application.port.out.BucketCachePort;
import com.cascada.cache.application.port.out.CoverageIndexPort;
import com.cascada.cache.application.port.out.GapQueryRewriterPort;
import com.cascada.cache.application.port.out.QueryExecutorPort;
import com.cascada.cache.application.service.cube.CubeShapeCatalog;
import com.cascada.cache.application.service.time.TimeBucketCalculator;
import com.cascada.cache.domain.model.cube.QueryShape;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.index.BucketCoverageBitmap;
import com.cascada.cache.domain.key.CacheKeyFactory;
import com.cascada.cache.domain.merge.AggregateFunction;
import com.cascada.cache.domain.model.query.CanonicalQueryObject;
import com.cascada.cache.domain.model.time.DailyBuckets;
import com.cascada.cache.domain.model.time.GapPlan;
import com.cascada.cache.exception.BucketEnumerationLimitExceededException;
import com.cascada.identity.domain.model.QueryHash;
import java.time.Clock;
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
    private final Clock clock;
    private final QueryAdmissionBudget queryAdmissionBudget;

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
     * Accepts the executor owned by the composition root. The separate-merge-executor overload lets
     * the application give blocking retrieval and CPU aggregation different execution budgets.
     */
    public CacheExecutionEngine(BucketCachePort cacheBackend, QueryExecutorPort sparkExecutor,
                                GapQueryRewriterPort gapQueryRewriter, CacheExecutionConfiguration configuration,
                                CoverageIndexPort coverageIndex, CubeShapeCatalog cubeCatalog, Executor executor) {
        this(cacheBackend, sparkExecutor, gapQueryRewriter, configuration, coverageIndex, cubeCatalog, executor,
                Clock.systemUTC());
    }

    /** The clock decides which buckets are complete: the bucket containing "now" is live and never cached. */
    public CacheExecutionEngine(BucketCachePort cacheBackend, QueryExecutorPort sparkExecutor,
                                GapQueryRewriterPort gapQueryRewriter, CacheExecutionConfiguration configuration,
                                CoverageIndexPort coverageIndex, CubeShapeCatalog cubeCatalog, Executor executor,
                                Clock clock) {
        this(cacheBackend, sparkExecutor, gapQueryRewriter, configuration, coverageIndex, cubeCatalog,
                executor, clock, executor);
    }

    /** Separate blocking retrieval from the composition root's shared CPU merge budget. */
    public CacheExecutionEngine(BucketCachePort cacheBackend, QueryExecutorPort sparkExecutor,
                                GapQueryRewriterPort gapQueryRewriter, CacheExecutionConfiguration configuration,
                                CoverageIndexPort coverageIndex, CubeShapeCatalog cubeCatalog, Executor executor,
                                Clock clock, Executor mergeExecutor) {
        this(cacheBackend, sparkExecutor, gapQueryRewriter, configuration, coverageIndex, cubeCatalog,
                executor, clock, mergeExecutor, null);
    }

    /**
     * Uses a composition-root-owned query admission budget to bound simultaneous query-local cache
     * and merge state. The budget is count-based because frame sizes and accumulator growth are not
     * known before execution; it does not promise a hard byte limit.
     */
    public CacheExecutionEngine(BucketCachePort cacheBackend, QueryExecutorPort sparkExecutor,
                                GapQueryRewriterPort gapQueryRewriter, CacheExecutionConfiguration configuration,
                                CoverageIndexPort coverageIndex, CubeShapeCatalog cubeCatalog, Executor executor,
                                Clock clock, Executor mergeExecutor, QueryAdmissionBudget queryAdmissionBudget) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.cacheBackend = Objects.requireNonNull(cacheBackend, "cacheBackend");
        this.sparkExecutor = Objects.requireNonNull(sparkExecutor, "sparkExecutor");
        this.gapQueryRewriter = Objects.requireNonNull(gapQueryRewriter, "gapQueryRewriter");
        this.coverageIndex = coverageIndex;
        this.cubeCatalog = cubeCatalog;
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.timeBucketCalculator = new TimeBucketCalculator(configuration.bucketSeconds());
        this.frameMergeService = new FrameMergeService(configuration.fixedStepSeconds(),
                configuration.timeColumnName(), configuration.mergeWorkerCount(),
                Objects.requireNonNull(mergeExecutor, "mergeExecutor"));
        this.queryAdmissionBudget = queryAdmissionBudget;
    }

    public ResultFrame execute(CanonicalQueryObject canonicalObject, QueryHash queryHash) {
        QueryAdmissionPermit admission = queryAdmissionBudget == null
                ? null : queryAdmissionBudget.acquire();
        try {
            return executeAdmitted(canonicalObject, queryHash);
        } finally {
            if (admission != null) admission.close();
        }
    }

    private ResultFrame executeAdmitted(CanonicalQueryObject canonicalObject, QueryHash queryHash) {
        requireActive();
        // Cube path (plan §8.12): only global aggregates with no deferred ORDER BY/LIMIT — a LIMIT
        // would register a truncated frame and a roll-up would not reapply the ordering. The lookup
        // is exact-time-window only, and every roll-up is verified before it is served.
        boolean cubeShapeEligible = cubeCatalog != null
                && !canonicalObject.metadata().isTimeSeries()
                && !canonicalObject.postProcessing().hasLimit()
                && !canonicalObject.postProcessing().hasOrderBy()
                && !hasDerivedMeasures(canonicalObject)
                && canonicalObject.metadata().compositeAliases().isEmpty();
        String revision = null;
        if (cubeShapeEligible) {
            try { revision = cacheBackend.revision().orElse(null); }
            catch (RuntimeException unavailable) { propagateCancellation(unavailable); }
        }
        boolean cubeEligible = cubeShapeEligible && revision != null;
        QueryShape queryShape = cubeEligible
                ? CubeShapeCatalog.shapeOf(canonicalObject)
                : null;
        if (cubeEligible) {
            try {
                Optional<ResultFrame> cubeAnswer = cubeCatalog.tryAnswer(canonicalObject.timeRange(), queryShape, revision);
                if (cubeAnswer.isPresent() && cacheBackend.revision().filter(revision::equals).isPresent()) {
                    return cubeAnswer.get();
                }
            } catch (RuntimeException unavailable) { propagateCancellation(unavailable); }
        }

        long startTimestamp = canonicalObject.timeRange().startTimestampSeconds();
        long endTimestamp = canonicalObject.timeRange().endTimestampSeconds();

        DailyBuckets buckets;
        try {
            buckets = timeBucketCalculator.getCompletedBuckets(startTimestamp, endTimestamp,
                    clock.instant().getEpochSecond());
        } catch (BucketEnumerationLimitExceededException excessiveWindow) {
            // A large query is still valid SQL; bypass the cache plan before any bucket list or keys
            // are allocated and let the authoritative executor handle the requested window.
            return executeAuthoritatively(cubeEligible, canonicalObject, queryShape, revision);
        }
        List<Long> bodyDays = buckets.body();

        List<String> requiredKeys = new ArrayList<>(bodyDays.size());
        for (long dayStart : bodyDays) {
            requiredKeys.add(CacheKeyFactory.buildBucketKey(queryHash, dayStart, configuration.bucketSeconds()));
        }

        // Fast path 1: nothing cacheable -> run the physical SQL directly.
        if (requiredKeys.isEmpty()) {
            return catalogFullWindowAnswer(cubeEligible, canonicalObject, queryShape,
                    executeDirect(canonicalObject), revision, List.of());
        }

        List<Boolean> presenceMask;
        try {
            presenceMask = resolvePresence(queryHash, bodyDays, requiredKeys);
        } catch (RuntimeException cacheFailure) {
            propagateCancellation(cacheFailure);
            return executeAuthoritatively(cubeEligible, canonicalObject, queryShape, revision);
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

        // Fast path 2: zero cache hits -> bypass all cache machinery. The read path never writes; the
        // warmer is the only writer, so a miss is answered and voted for warming, never stored here.
        if (cachedKeys.isEmpty()) {
            return executeAuthoritatively(cubeEligible, canonicalObject, queryShape, revision);
        }

        GapPlan gapPlan = new GapPlan(buckets.head(), missingDays, buckets.tail());

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
                        QueryExecutionScope.propagate(() -> executeSql(gapQueryRewriter.buildGapQuery(
                                canonicalObject.physicalSql(), gapPlan))), executor)
                : CompletableFuture.completedFuture(ResultFrame.empty());

        try {
            cacheFuture.join();
        } catch (RuntimeException cacheFailure) {
            propagateCancellation(cacheFailure);
            // Prevent a queued gap task from starting after the authoritative fallback begins.
            // A running query is left untouched because the executor port has no cancellation contract.
            sparkFuture.cancel(false);
            return executeAuthoritatively(cubeEligible, canonicalObject, queryShape, revision);
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
        if (gapPlan.hasGaps()) {
            merge.add(sparkFrame);
        }

        ResultFrame vanishedFrame = vanishedFuture.join();
        if (!vanishedDays.isEmpty()) {
            merge.add(vanishedFrame);
        }

        try {
            return catalogFullWindowAnswer(cubeEligible, canonicalObject, queryShape,
                    merge.finish(), revision, cachedKeys);
        } catch (RuntimeException cacheOrMergeFailure) {
            propagateCancellation(cacheOrMergeFailure);
            // A decoded but corrupt/incompatible cache frame must not make the query unavailable.
            // The full physical query is authoritative and avoids trusting any cache-derived data.
            return executeAuthoritatively(cubeEligible, canonicalObject, queryShape, revision);
        }
    }

    /** A complete full-window answer becomes a cube candidate for finer queries over the same window. */
    private ResultFrame catalogFullWindowAnswer(boolean cubeEligible, CanonicalQueryObject canonicalObject,
                                                QueryShape queryShape, ResultFrame answer, String revision, List<String> dependencies) {
        if (cubeEligible && isCompletedWindow(canonicalObject)) {
            try {
                Optional<java.time.Duration> ttl = dependencies.isEmpty() ? Optional.empty()
                        : cacheBackend.remainingTimeToLive(dependencies);
                if (!dependencies.isEmpty() && ttl.isEmpty()) return answer;
                if (cacheBackend.revision().filter(revision::equals).isPresent()) {
                    cubeCatalog.register(canonicalObject.timeRange(), queryShape, answer, revision, ttl);
                }
            } catch (RuntimeException ignored) {
                propagateCancellation(ignored);
                // The cube is an optional accelerator; a catalog failure cannot invalidate a query result.
            }
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
                                               QueryShape queryShape, String revision) {
        return catalogFullWindowAnswer(cubeEligible, canonicalObject, queryShape,
                executeDirect(canonicalObject), revision, List.of());
    }

    /** A window touching the live bucket would freeze in-progress data, so only finished windows are catalogued. */
    private boolean isCompletedWindow(CanonicalQueryObject canonicalObject) {
        return canonicalObject.timeRange().endTimestampSeconds()
                < timeBucketCalculator.currentBucketStart(clock.instant().getEpochSecond());
    }

    /** AVG is fetched as SUM and COUNT ingredients and divided by the merge. */
    private static boolean hasDerivedMeasures(CanonicalQueryObject canonicalObject) {
        return canonicalObject.metadata().measureAggregates().containsValue(AggregateFunction.AVERAGE);
    }

    /** Zero-hit requests execute the original SQL; only the warmer stores decomposed ingredients. */
    private ResultFrame executeDirect(CanonicalQueryObject canonicalObject) {
        return executeSql(canonicalObject.physicalSql());
    }

    private ResultFrame executeSql(String sql) {
        requireActive();
        return sparkExecutor.execute(sql);
    }

    private static void requireActive() {
        QueryCancellation.requireCurrentActive();
    }

    private static void propagateCancellation(RuntimeException failure) {
        QueryCancellation.rethrowIfCancelled(failure);
    }

}
