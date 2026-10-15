package com.cascada.cache.application.service;

import com.cascada.cache.application.config.WarmingConfiguration;
import com.cascada.cache.application.execution.QueryExecutionScope;
import com.cascada.cache.application.port.in.WarmCacheUseCase;
import com.cascada.cache.application.port.out.BucketCachePort;
import com.cascada.cache.application.port.out.CoverageIndexPort;
import com.cascada.cache.application.port.out.GapQueryRewriterPort;
import com.cascada.cache.application.port.out.QueryExecutorPort;
import com.cascada.cache.application.port.out.QueryPopularityPort;
import com.cascada.cache.application.port.out.SqlCanonicalizerPort;
import com.cascada.cache.application.result.PatternWarmingResult;
import com.cascada.cache.application.result.WarmingReport;
import com.cascada.cache.application.service.cube.CubeShapeCatalog;
import com.cascada.cache.application.service.time.TimeBucketCalculator;
import com.cascada.cache.application.service.warming.DataChangeDetector;
import com.cascada.cache.application.service.warming.WarmingQueue;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.index.BucketCoverageBitmap;
import com.cascada.cache.domain.key.CacheKeyFactory;
import com.cascada.cache.domain.query.CanonicalQueryObject;
import com.cascada.cache.domain.query.PopularQuery;
import com.cascada.cache.domain.time.GapPlan;
import com.cascada.cache.domain.time.TimeRange;
import com.cascada.cache.exception.BucketEnumerationLimitExceededException;
import com.cascada.identity.domain.QueryHash;
import java.math.BigInteger;
import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.logging.Logger;

/**
 * The Java heir to {@code auto_warmer.py}: the <em>only</em> writer of cache buckets. It pre-computes
 * likely-to-be-hit buckets so the read path finds them already in cache. It ties together the Layer-1
 * {@link WarmingQueue} (recent misses) and the Layer-2 {@link QueryPopularityPort} (persistent top-N), and
 * warms each pattern <em>bucket by bucket</em> over the lookback window via the same gap-rewriter and Spark
 * executor the engine uses, so a warmed bucket is byte-identical to one the engine computes for a gap.
 *
 * <ul>
 *   <li><b>Complete buckets only:</b> the bucket containing "now" is live and never stored;</li>
 *   <li><b>EXISTS-skip:</b> a bucket already present is not recomputed unless the data-change canary
 *       (or an operator) forces an overwrite;</li>
 *   <li><b>Expiring writes:</b> every bucket is stored with a time-to-live, so nothing lives forever;</li>
 *   <li><b>Bounded writes:</b> a frame larger than the configured row cap is not stored;</li>
 *   <li><b>two layers, de-duplicated:</b> Layer 2 never re-warms a hash Layer 1 already did, and rebuilds
 *       canonical objects from persisted SQL after a restart.</li>
 * </ul>
 */
public final class WarmingOrchestrator implements WarmCacheUseCase {

    private static final Logger LOGGER = Logger.getLogger(WarmingOrchestrator.class.getName());

    private final BucketCachePort cacheBackend;
    private final QueryExecutorPort sparkExecutor;
    private final GapQueryRewriterPort gapQueryRewriter;
    private final WarmingQueue warmingQueue;
    private final QueryPopularityPort popularityTracker;
    private final CoverageIndexPort coverageIndex;
    private final SqlCanonicalizerPort canonicalizer;
    private final DataChangeDetector dataChangeDetector;
    private final CubeShapeCatalog cubeCatalog;
    private final WarmingConfiguration configuration;
    private final Clock clock;
    private final Map<QueryHash, CanonicalQueryObject> canonicalRegistry = new LinkedHashMap<>();

    public WarmingOrchestrator(BucketCachePort cacheBackend, QueryExecutorPort sparkExecutor,
                               GapQueryRewriterPort gapQueryRewriter, WarmingQueue warmingQueue,
                               QueryPopularityPort popularityTracker,
                               long bucketSeconds, int topNQueries) {
        this(cacheBackend, sparkExecutor, gapQueryRewriter, warmingQueue, popularityTracker,
                null, bucketSeconds, topNQueries);
    }

    /** With a coverage-bitmap index (Appendix J.1): every warmed bucket also sets its coverage bit. */
    public WarmingOrchestrator(BucketCachePort cacheBackend, QueryExecutorPort sparkExecutor,
                               GapQueryRewriterPort gapQueryRewriter, WarmingQueue warmingQueue,
                               QueryPopularityPort popularityTracker,
                               CoverageIndexPort coverageIndex,
                               long bucketSeconds, int topNQueries) {
        this(cacheBackend, sparkExecutor, gapQueryRewriter, warmingQueue, popularityTracker,
                coverageIndex, bucketSeconds, topNQueries, 10_000);
    }

    public WarmingOrchestrator(BucketCachePort cacheBackend, QueryExecutorPort sparkExecutor,
                              GapQueryRewriterPort gapQueryRewriter, WarmingQueue warmingQueue,
                              QueryPopularityPort popularityTracker, CoverageIndexPort coverageIndex,
                              long bucketSeconds, int topNQueries, int maximumPatterns) {
        this(cacheBackend, sparkExecutor, gapQueryRewriter, warmingQueue, popularityTracker, coverageIndex,
                null, null, null, WarmingConfiguration.of(bucketSeconds > 0 ? bucketSeconds : 86_400L,
                        Math.max(0, topNQueries), maximumPatterns), Clock.systemUTC());
    }

    /**
     * Full wiring. {@code coverageIndex}, {@code canonicalizer}, {@code dataChangeDetector} and
     * {@code cubeCatalog} are optional: without a canonicalizer Layer 2 only warms queries seen since start,
     * without a detector forced overwrites come only from operators, and without a catalog there is no cube
     * to invalidate.
     */
    public WarmingOrchestrator(BucketCachePort cacheBackend, QueryExecutorPort sparkExecutor,
                               GapQueryRewriterPort gapQueryRewriter, WarmingQueue warmingQueue,
                               QueryPopularityPort popularityTracker, CoverageIndexPort coverageIndex,
                               SqlCanonicalizerPort canonicalizer, DataChangeDetector dataChangeDetector,
                               CubeShapeCatalog cubeCatalog, WarmingConfiguration configuration, Clock clock) {
        this.cacheBackend = cacheBackend;
        this.sparkExecutor = sparkExecutor;
        this.gapQueryRewriter = gapQueryRewriter;
        this.warmingQueue = warmingQueue;
        this.popularityTracker = popularityTracker;
        this.coverageIndex = coverageIndex;
        this.canonicalizer = canonicalizer;
        this.dataChangeDetector = dataChangeDetector;
        this.cubeCatalog = cubeCatalog;
        this.configuration = configuration;
        this.clock = clock;
    }

    /**
     * The read-path hook (ported from {@code CacheManager.handle_query}'s warming vote + tracker
     * track): register the canonical so the warmer can rebuild its SQL, vote it into Layer 1, and
     * record an observation for Layer 2.
     */
    @Override
    public synchronized void recordQuery(QueryHash queryHash, CanonicalQueryObject canonicalObject) {
        canonicalRegistry.put(queryHash, canonicalObject);
        if (canonicalRegistry.size() > configuration.maximumPatterns()) {
            var oldest = canonicalRegistry.keySet().iterator();
            oldest.next();
            oldest.remove();
        }
        warmingQueue.vote(queryHash);
        popularityTracker.recordObservation(queryHash, canonicalObject.physicalSql());
    }

    private synchronized Optional<CanonicalQueryObject> findCanonicalQuery(QueryHash queryHash) {
        return Optional.ofNullable(canonicalRegistry.get(queryHash));
    }

    /**
     * The scheduled cycle: warm every complete bucket of the lookback window, first asking the data-change
     * canary whether already-cached history moved. The validation window leaves out the oldest bucket (it
     * is about to slide out), the live bucket and the bucket before it (late data still lands there).
     */
    @Override
    public WarmingReport warmLookbackWindow(long nowSeconds, Duration lookback) {
        long bucketSeconds = configuration.bucketSeconds();
        long currentBucketStart = Math.floorDiv(nowSeconds, bucketSeconds) * bucketSeconds;
        long warmStart = nowSeconds - lookback.toSeconds();
        long validationStart = Math.floorDiv(warmStart, bucketSeconds) * bucketSeconds + bucketSeconds;
        long validationEnd = currentBucketStart - bucketSeconds - 1;
        boolean historyChanged = dataChangeDetector != null && validationEnd >= validationStart
                && dataChangeDetector.hasHistoricalDataChanged(new TimeRange(validationStart, validationEnd));
        return warmCycle(warmStart, currentBucketStart - 1, historyChanged);
    }

    /**
     * One warming cycle over {@code [warmStartTs, warmEndTs]} — Layer 1 then Layer 2, de-duplicated. The
     * window is clamped to buckets that have finished, whatever end the caller asked for.
     */
    @Override
    public WarmingReport warmCycle(long warmStartTimestampSeconds, long requestedWarmEndTimestampSeconds,
                                   boolean forceOverwrite) {
        long nowSeconds = clock.instant().getEpochSecond();
        long warmEndTimestampSeconds = Math.min(requestedWarmEndTimestampSeconds,
                Math.floorDiv(nowSeconds, configuration.bucketSeconds()) * configuration.bucketSeconds() - 1);
        if (forceOverwrite && cubeCatalog != null) {
            // Catalogued full-window answers were computed from the same history that is being rewritten.
            cubeCatalog.clear();
        }
        // LAYER 1: recent patterns voted by the read path.
        List<QueryHash> layer1 = warmingQueue.consumeAll();
        try {
            Map<QueryHash, CanonicalQueryObject> toWarm = new LinkedHashMap<>();
            layer1.forEach(hash -> findCanonicalQuery(hash).ifPresent(canonical -> toWarm.put(hash, canonical)));
            // LAYER 2: persistent top-N popular queries, skipping anything Layer 1 already covered.
            popularityTracker.topByPopularity(configuration.topNQueries()).stream()
                    .filter(popularQuery -> !toWarm.containsKey(popularQuery.queryHash()))
                    .forEach(popularQuery -> resolveCanonicalQuery(popularQuery)
                            .ifPresent(canonical -> toWarm.put(popularQuery.queryHash(), canonical)));
            int patternsWarmed = 0;
            int bucketsWarmed = 0;
            int bucketsSkipped = 0;
            for (Map.Entry<QueryHash, CanonicalQueryObject> entry : toWarm.entrySet()) {
                QueryExecutionScope.current().ifPresent(cancellation -> cancellation.requireActive());
                try {
                    PatternWarmingResult result = warmSinglePattern(entry.getKey(), entry.getValue(),
                            warmStartTimestampSeconds, warmEndTimestampSeconds, forceOverwrite);
                    patternsWarmed++;
                    bucketsWarmed += result.bucketsWarmed();
                    bucketsSkipped += result.bucketsSkipped();
                } catch (RuntimeException failure) {
                    // A bad pattern must not discard itself or prevent later patterns from warming.
                    warmingQueue.vote(entry.getKey());
                    LOGGER.warning("warming failed for query hash " + entry.getKey() + " ("
                            + failure.getClass().getSimpleName() + "); it was returned to the queue");
                }
            }
            return new WarmingReport(patternsWarmed, bucketsWarmed, bucketsSkipped);
        } catch (RuntimeException setupFailure) {
            // Failures while constructing the work set happen after consumeAll drained Layer 1.
            layer1.forEach(warmingQueue::vote);
            throw setupFailure;
        }
    }

    /** Queries seen since start come from the registry; anything older is rebuilt from its persisted SQL. */
    private Optional<CanonicalQueryObject> resolveCanonicalQuery(PopularQuery popularQuery) {
        return findCanonicalQuery(popularQuery.queryHash())
                .or(() -> Optional.ofNullable(canonicalizer).flatMap(parser -> canonicalize(parser, popularQuery)));
    }

    private Optional<CanonicalQueryObject> canonicalize(SqlCanonicalizerPort parser, PopularQuery popularQuery) {
        try {
            return Optional.of(parser.canonicalize(popularQuery.physicalSql()));
        } catch (CancellationException cancelled) {
            throw cancelled;
        } catch (RuntimeException unsupported) {
            LOGGER.fine("popular query " + popularQuery.queryHash() + " can no longer be canonicalised; skipping");
            return Optional.empty();
        }
    }

    /** Warm a single pattern's buckets, ported from {@code _warm_single_pattern_sequential}. */
    public PatternWarmingResult warmSinglePattern(QueryHash queryHash, CanonicalQueryObject canonicalObject,
                                                  long warmStartTimestampSeconds, long warmEndTimestampSeconds,
                                                  boolean forceOverwrite) {
        if (warmEndTimestampSeconds < warmStartTimestampSeconds) {
            return new PatternWarmingResult(0, 0);
        }
        long bucketSeconds = configuration.bucketSeconds();
        BigInteger width = BigInteger.valueOf(bucketSeconds);
        BigInteger minimumTimestamp = BigInteger.valueOf(Long.MIN_VALUE);
        BigInteger maximumTimestamp = BigInteger.valueOf(Long.MAX_VALUE);
        BigInteger firstBucketStart = floorBucketStart(BigInteger.valueOf(warmStartTimestampSeconds), width);
        BigInteger firstBucketEnd = firstBucketStart.add(width).subtract(BigInteger.ONE);
        if (firstBucketEnd.compareTo(BigInteger.valueOf(warmEndTimestampSeconds)) > 0) {
            return new PatternWarmingResult(0, 0);
        }
        if (firstBucketStart.compareTo(minimumTimestamp) < 0) {
            firstBucketStart = firstBucketStart.add(width); // the unrepresentable edge can never be keyed
        }
        if (firstBucketStart.compareTo(maximumTimestamp) > 0) {
            return new PatternWarmingResult(0, 0);
        }
        long firstRepresentableBucketStart = firstBucketStart.longValueExact();
        ensureWarmWindowWithinLimit(firstRepresentableBucketStart, warmEndTimestampSeconds);
        int bucketsWarmed = 0;
        int bucketsSkipped = 0;
        // Never re-warm what is already cached: one coverage-bitmap load answers presence for the
        // whole lookback window (plan J.1) instead of one EXISTS round-trip per bucket. No bitmap (or
        // forceOverwrite) falls back to the per-bucket EXISTS check. A stale present-bit can only
        // skip a warm, never corrupt data: the read path's vanished-bucket guard recomputes that
        // bucket live as a gap.
        Optional<BucketCoverageBitmap> coverage = (coverageIndex != null && !forceOverwrite)
                ? coverageIndex.load(queryHash, bucketSeconds)
                : Optional.empty();
        // README caveat 1: warm only COMPLETE buckets. The bucket key claims the FULL window
        // [bucketStart, bucketStart + bucketSeconds - 1]; storing a frame truncated at warmEnd under
        // that key would permanently undercount. The trailing partial bucket is simply not warmed;
        // the read path computes it live as a gap, exactly as on any miss.
        long currentBucketStart = firstRepresentableBucketStart;
        while (true) {
            QueryExecutionScope.current().ifPresent(cancellation -> cancellation.requireActive());
            long bucketEnd;
            try {
                bucketEnd = Math.addExact(currentBucketStart, bucketSeconds - 1);
            } catch (ArithmeticException outsideLongDomain) {
                break; // This bucket extends beyond the representable timestamp domain.
            }
            if (bucketEnd > warmEndTimestampSeconds) {
                break;
            }
            String key = CacheKeyFactory.buildBucketKey(queryHash, currentBucketStart, bucketSeconds);
            boolean alreadyWarmed;
            if (forceOverwrite) {
                alreadyWarmed = false;
            } else if (coverage.isPresent()) {
                alreadyWarmed = coverage.get().isCovered(currentBucketStart) && isAlreadyWarmed(key);
                if (!alreadyWarmed) {
                    coverageIndex.markEvicted(queryHash, bucketSeconds, currentBucketStart);
                }
            } else {
                alreadyWarmed = isAlreadyWarmed(key);
            }
            if (alreadyWarmed || !storeBucket(queryHash, canonicalObject, key, currentBucketStart, bucketEnd)) {
                bucketsSkipped++;
            } else {
                bucketsWarmed++;
            }
            try {
                currentBucketStart = Math.addExact(currentBucketStart, bucketSeconds);
            } catch (ArithmeticException outsideLongDomain) {
                break;
            }
        }
        return new PatternWarmingResult(bucketsWarmed, bucketsSkipped);
    }

    /**
     * Computes one complete bucket and stores it with its time-to-live. Empty results are stored too (a
     * cached empty frame is a valid "no data that day" hit); frames above the row cap are not, so a
     * near-unique GROUP BY keeps being answered by Spark instead of filling the cache tier.
     */
    private boolean storeBucket(QueryHash queryHash, CanonicalQueryObject canonicalObject, String key,
                                long bucketStart, long bucketEnd) {
        String bucketSql = gapQueryRewriter.buildGapQuery(canonicalObject.physicalSql(),
                new GapPlan(Optional.empty(), List.of(), Optional.of(new TimeRange(bucketStart, bucketEnd))));
        ResultFrame frame = sparkExecutor.execute(bucketSql);
        if (frame.rowCount() > configuration.maximumCachedRows()) {
            LOGGER.fine("bucket " + key + " has " + frame.rowCount() + " rows, above the cache row cap; not stored");
            return false;
        }
        cacheBackend.store(key, frame, configuration.bucketTimeToLive());
        if (coverageIndex != null) {
            coverageIndex.markCached(queryHash, configuration.bucketSeconds(), bucketStart);
        }
        return true;
    }

    private boolean isAlreadyWarmed(String key) {
        List<Boolean> presence = cacheBackend.existsForKeys(List.of(key));
        return !presence.isEmpty() && Boolean.TRUE.equals(presence.get(0));
    }

    private void ensureWarmWindowWithinLimit(long startSeconds, long endSeconds) {
        BigInteger width = BigInteger.valueOf(configuration.bucketSeconds());
        BigInteger start = BigInteger.valueOf(startSeconds);
        BigInteger end = BigInteger.valueOf(endSeconds);
        BigInteger firstBucketStart = floorBucketStart(start, width);
        BigInteger firstBucketEnd = firstBucketStart.add(width).subtract(BigInteger.ONE);
        if (firstBucketEnd.compareTo(end) > 0) {
            return;
        }
        BigInteger bucketCount = end.subtract(firstBucketEnd).divide(width).add(BigInteger.ONE);
        if (bucketCount.compareTo(BigInteger.valueOf(TimeBucketCalculator.MAX_BUCKETS_PER_PLAN)) > 0) {
            throw new BucketEnumerationLimitExceededException(
                    TimeBucketCalculator.MAX_BUCKETS_PER_PLAN);
        }
    }

    private BigInteger floorBucketStart(BigInteger timestamp, BigInteger width) {
        BigInteger[] division = timestamp.divideAndRemainder(width);
        BigInteger quotient = division[0];
        if (division[1].signum() < 0) {
            quotient = quotient.subtract(BigInteger.ONE);
        }
        return quotient.multiply(width);
    }

    /** Diagnostic: the hashes currently known to the warmer (registered via {@link #recordQuery}). */
    public synchronized Set<QueryHash> knownQueryHashes() {
        return new LinkedHashSet<>(canonicalRegistry.keySet());
    }
}
