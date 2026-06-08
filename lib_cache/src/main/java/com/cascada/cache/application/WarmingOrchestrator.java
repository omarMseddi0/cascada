package com.cascada.cache.application;

import com.cascada.cache.adapter.tracking.MarkovNextQueryPredictor;
import com.cascada.cache.adapter.tracking.QueryPopularityTracker;
import com.cascada.cache.domain.CacheKeyFactory;
import com.cascada.cache.domain.CanonicalQueryObject;
import com.cascada.cache.domain.GapPlan;
import com.cascada.cache.domain.TimeRange;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.port.CacheBackendPort;
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
 * machinery — the Layer-1 {@link WarmingQueue} (recent misses), the Layer-2 {@link QueryPopularityTracker}
 * (persistent top-N), the {@link MarkovNextQueryPredictor} (speculative drill-downs, plan §8.15) — and
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
    private final MarkovNextQueryPredictor nextQueryPredictor;
    private final long bucketSeconds;
    private final int topNQueries;

    private final Map<QueryHash, CanonicalQueryObject> canonicalRegistry = new ConcurrentHashMap<>();

    public WarmingOrchestrator(CacheBackendPort cacheBackend, SparkQueryExecutorPort sparkExecutor,
                               GapQueryRewriterPort gapQueryRewriter, WarmingQueue warmingQueue,
                               QueryPopularityTracker popularityTracker,
                               MarkovNextQueryPredictor nextQueryPredictor,
                               long bucketSeconds, int topNQueries) {
        this.cacheBackend = cacheBackend;
        this.sparkExecutor = sparkExecutor;
        this.gapQueryRewriter = gapQueryRewriter;
        this.warmingQueue = warmingQueue;
        this.popularityTracker = popularityTracker;
        this.nextQueryPredictor = nextQueryPredictor;
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

    /**
     * Speculative warming (plan §8.15): after a parent query, enqueue the likely next drill-downs so
     * they are warm before the user clicks. Only hashes whose canonical we know can be warmed.
     */
    public int enqueueSpeculativeNext(QueryHash currentQuery, int howMany) {
        int enqueued = 0;
        for (QueryHash predicted : nextQueryPredictor.predictNext(currentQuery, howMany)) {
            if (canonicalRegistry.containsKey(predicted)) {
                warmingQueue.vote(predicted);
                enqueued++;
            }
        }
        return enqueued;
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

        long currentBucketStart = Math.floorDiv(warmStartTimestampSeconds, bucketSeconds) * bucketSeconds;
        while (currentBucketStart <= warmEndTimestampSeconds) {
            long bucketEnd = Math.min(currentBucketStart + bucketSeconds - 1, warmEndTimestampSeconds);
            String key = CacheKeyFactory.buildBucketKey(queryHash, currentBucketStart, bucketSeconds);

            if (!forceOverwrite && isAlreadyWarmed(key)) {
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
