package com.cascada.cache.domain;

/**
 * Exact constants ported from {@code data_collector/src/core/smart_cache/domain.py}.
 *
 * <p>These values are part of the cache wire contract (key formats, fixed step) and must
 * not drift: a different bucket size or step would make every previously stored entry
 * unreadable. They are pinned here and asserted by the cache-correctness gate.
 */
public final class CacheConstants {

    private CacheConstants() {
    }

    public static final int SECONDS_PER_DAY = 86_400;

    /**
     * The universal fixed internal step at which time-series buckets are stored. The user's
     * requested display step is re-derived by resampling at merge time — never at store time —
     * which is what makes {@code hash(7-day query) == hash(1-day query)} (cache_hashing.py).
     */
    public static final int DEFAULT_CACHE_STEP_SECONDS = 300;

    public static final int SECONDS_PER_DAY_MINUS_ONE = 86_399;

    /** Bucket cache key namespace: {@code QC:V4:B<bucketSeconds>:<queryHash>:<bucketStartTs>}. */
    public static final String CACHE_KEY_PREFIX = "QC:V4";

    /** Query-tracker sorted-set of popularity: members = query hash, score = cumulative hits. */
    public static final String QUERY_TRACKER_TOP_SORTED_SET_KEY = "QT:V1:TOP";

    /** Per-query metadata hash prefix. Full key: {@code QT:V1:META:<queryHash>}. */
    public static final String QUERY_TRACKER_META_KEY_PREFIX = "QT:V1:META";

    public static final String QUERY_TRACKER_KEY_PREFIX = "QT:V1";
}
