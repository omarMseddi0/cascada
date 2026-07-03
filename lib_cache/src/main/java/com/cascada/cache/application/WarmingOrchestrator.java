package com.cascada.cache.application;

import com.cascada.cache.adapter.tracking.QueryPopularityTracker;
import com.cascada.cache.domain.CacheKeyFactory;
import com.cascada.cache.domain.CanonicalQueryObject;
import com.cascada.cache.domain.GapPlan;
import com.cascada.cache.domain.TimeRange;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.index.BucketCoverageBitmap;
import com.cascada.cache.domain.port.CacheBackendPort;
import com.cascada.cache.domain.port.CoverageIndexPort;
import com.cascada.cache.domain.port.GapQueryRewriterPort;
import com.cascada.cache.domain.port.SparkQueryExecutorPort;
import com.cascada.cache.domain.warming.WarmingQueue;
import com.cascada.identity.domain.QueryHash;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Java heir to {@code auto_warmer.py}: the application service that pre-computes likely-to-be-hit
 * buckets so the read path finds them already in cache. It ties together the existing warming
 * machinery — the Layer-1 {@link WarmingQueue} (recent misses) and the Layer-2
 * {@link QueryPopularityTracker} (persistent top-N) — and
 * warms each pattern <em>bucket by bucket</em> over the lookback window via the same gap-rewriter and
 * Spark executor the engine uses, so a warmed bucket is byte-identical to one the engine would have
 * computed on a miss (the 100%-data-consistency invariant).
 *
 * <p>Ported faithfully from {@code _perform_warming_cycle} / {@code _warm_single_pattern_sequential}:
 * <ul>
 *   <li><b>EXISTS-skip:</b> a bucket already present is not recomputed (unless {@code forceOverwrite});</li>
 *   <li><b>force-overwrite:</b> the validation gate's data-change signal re-warms every bucket;</li>
 *   <li><b>per-bucket gap SQL:</b> the whole bucket is a gap fetched via
 *       {@link GapQueryRewriterPort#buildGapQuery} with a single tail range, which also strips the
 *       original wide time predicate (no double-predicate 0-row bug);</li>
 *   <li><b>two layers, de-duplicated:</b> Layer-2 never re-warms a hash Layer-1 already did.</li>
 * </ul>
 *
 * <p>Unlike the Python monolith (which carried the canonical object inside each queue vote), the Java
 * {@link WarmingQueue} carries only the hash; this orchestrator owns the hash→canonical
 * {@link #canonicalRegistry} that {@link #recordQuery} populates from the read path.
 */
public final class WarmingOrchestrator {

    private final CacheBackendPort cacheBackend;
    private final SparkQueryExecutorPort sparkExecutor;
    private final GapQueryRewriterPort gapQueryRewriter;
    private final WarmingQueue warmingQueue;
    private final QueryPopularityTracker popularityTracker;
    private final CoverageIndexPort coverageIndex;
    private final long bucketSeconds;
    private final int topNQueries;

    private final Map<QueryHash, CanonicalQueryObject> canonicalRegistry = new ConcurrentHashMap<>();

    public WarmingOrchestrator(CacheBackendPort cacheBackend, SparkQueryExecutorPort sparkExecutor,
                               GapQueryRewriterPort gapQueryRewriter, WarmingQueue warmingQueue,
                               QueryPopularityTracker popularityTracker,
                               long bucketSeconds, int topNQueries) {
        this(cacheBackend, sparkExecutor, gapQueryRewriter, warmingQueue, popularityTracker,
                null, bucketSeconds, topNQueries);
    }

    /** With a coverage-bitmap index (Appendix J.1): every warmed bucket also sets its coverage bit. */
    public WarmingOrchestrator(CacheBackendPort cacheBackend, SparkQueryExecutorPort sparkExecutor,
                               GapQueryRewriterPort gapQueryRewriter, WarmingQueue warmingQueue,
                               QueryPopularityTracker popularityTracker,
                               CoverageIndexPort coverageIndex,
                               long bucketSeconds, int topNQueries) {
        this.cacheBackend = cacheBackend;
        this.sparkExecutor = sparkExecutor;
        this.gapQueryRewriter = gapQueryRewriter;
        this.warmingQueue = warmingQueue;
        this.popularityTracker = popularityTracker;
        this.coverageIndex = coverageIndex;
        this.bucketSeconds = bucketSeconds > 0 ? bucketSeconds : 86_400L;
        this.topNQueries = Math.max(0, topNQueries);
    }

    /**
     * The read-path hook (ported from {@code CacheManager.handle_query}'s warming vote + tracker
     * track): register the canonical so the warmer can rebuild its SQL, vote it into Layer 1, and
     * record an observation for Layer 2.
     */
    public void recordQuery(QueryHash queryHash, CanonicalQueryObject canonicalObject) {
        canonicalRegistry.put(queryHash, canonicalObject);
        warmingQueue.vote(queryHash);
        popularityTracker.recordObservation(queryHash);
    }

    /** One warming cycle over {@code [warmStartTs, warmEndTs]} — Layer 1 then Layer 2, de-duplicated. */
    public WarmingReport warmCycle(long warmStartTimestampSeconds, long warmEndTimestampSeconds,
                                   boolean forceOverwrite) {
        // LAYER 1: recent patterns voted by the read path.
        List<QueryHash> layer1 = warmingQueue.consumeAll();
        Map<QueryHash, CanonicalQueryObject> toWarm = new LinkedHashMap<>();
        for (QueryHash hash : layer1) {
            CanonicalQueryObject canonical = canonicalRegistry.get(hash);
            if (canonical != null) {
                toWarm.put(hash, canonical);
            }
        }

        // LAYER 2: persistent top-N popular queries, skipping anything Layer 1 already covered.
        for (QueryHash hash : popularityTracker.topByPopularity(topNQueries)) {
            if (toWarm.containsKey(hash)) {
                continue;
            }
            CanonicalQueryObject canonical = canonicalRegistry.get(hash);
            if (canonical != null) {
                toWarm.put(hash, canonical);
            }
        }

        int patternsWarmed = 0;
        int bucketsWarmed = 0;
        int bucketsSkipped = 0;
        for (Map.Entry<QueryHash, CanonicalQueryObject> entry : toWarm.entrySet()) {
            PatternWarmingResult result = warmSinglePattern(entry.getKey(), entry.getValue(),
                    warmStartTimestampSeconds, warmEndTimestampSeconds, forceOverwrite);
            patternsWarmed++;
            bucketsWarmed += result.bucketsWarmed();
            bucketsSkipped += result.bucketsSkipped();
        }
        return new WarmingReport(patternsWarmed, bucketsWarmed, bucketsSkipped);
    }

    /** Warm a single pattern's buckets, ported from {@code _warm_single_pattern_sequential}. */
    public PatternWarmingResult warmSinglePattern(QueryHash queryHash, CanonicalQueryObject canonicalObject,
                                                  long warmStartTimestampSeconds, long warmEndTimestampSeconds,
                                                  boolean forceOverwrite) {
        int bucketsWarmed = 0;
        int bucketsSkipped = 0;

        // Never re-warm what is already cached: one coverage-bitmap load answers presence for the
        // whole lookback window (plan J.1) instead of one EXISTS round-trip per bucket — a 55-day
        // window steady-state costs one fetch and warms only the missing buckets. No bitmap (or
        // forceOverwrite) falls back to the per-bucket EXISTS check. A stale present-bit can only
        // skip a warm, never corrupt data: the read path's vanished-bucket guard recomputes that
        // bucket live as a gap.
        Optional<BucketCoverageBitmap> coverage = (coverageIndex != null && !forceOverwrite)
                ? coverageIndex.load(queryHash, bucketSeconds)
                : Optional.empty();

        // README caveat 1: warm only COMPLETE buckets. The bucket key claims the FULL window
        // [bucketStart, bucketStart + bucketSeconds - 1]; storing a frame truncated at warmEnd under
        // that key would permanently undercount — the EXISTS-skip then sees the bucket as present and
        // never recomputes it, so the hole can never self-heal. The trailing partial bucket is simply
        // not warmed; the read path computes it live as a gap, exactly as on any miss.
        long currentBucketStart = Math.floorDiv(warmStartTimestampSeconds, bucketSeconds) * bucketSeconds;
        while (currentBucketStart + bucketSeconds - 1 <= warmEndTimestampSeconds) {
            long bucketEnd = currentBucketStart + bucketSeconds - 1;
            String key = CacheKeyFactory.buildBucketKey(queryHash, currentBucketStart, bucketSeconds);

            boolean alreadyWarmed;
            if (forceOverwrite) {
                alreadyWarmed = false;
            } else if (coverage.isPresent()) {
                alreadyWarmed = coverage.get().isCovered(currentBucketStart);
            } else {
                alreadyWarmed = isAlreadyWarmed(key);
            }
            if (alreadyWarmed) {
                bucketsSkipped++;
                currentBucketStart += bucketSeconds;
                continue;
            }

            String bucketSql = gapQueryRewriter.buildGapQuery(
                    canonicalObject.physicalSql(),
                    new GapPlan(Optional.empty(), List.of(), Optional.of(new TimeRange(currentBucketStart, bucketEnd))));
            // Cache empty results too (Python caches an empty frame to avoid re-querying).
            ResultFrame frame = sparkExecutor.execute(bucketSql);
            cacheBackend.store(key, frame);
            if (coverageIndex != null) {
                coverageIndex.markCached(queryHash, bucketSeconds, currentBucketStart);
            }
            bucketsWarmed++;

            currentBucketStart += bucketSeconds;
        }
        return new PatternWarmingResult(bucketsWarmed, bucketsSkipped);
    }

    private boolean isAlreadyWarmed(String key) {
        List<Boolean> presence = cacheBackend.existsForKeys(List.of(key));
        return !presence.isEmpty() && Boolean.TRUE.equals(presence.get(0));
    }

    /** Diagnostic: the hashes currently known to the warmer (registered via {@link #recordQuery}). */
    public java.util.Set<QueryHash> knownQueryHashes() {
        return new LinkedHashSet<>(canonicalRegistry.keySet());
    }

    /** Per-cycle totals. */
    public record WarmingReport(int patternsWarmed, int bucketsWarmed, int bucketsSkipped) {
    }

    /** Per-pattern totals. */
    public record PatternWarmingResult(int bucketsWarmed, int bucketsSkipped) {
        public PatternWarmingResult {
            if (bucketsWarmed < 0 || bucketsSkipped < 0) {
                throw new IllegalArgumentException("bucket counts must be non-negative");
            }
        }
    }
}
