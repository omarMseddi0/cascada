package com.cascada.cache.domain.key;

/** Stable key namespaces shared by cache-key construction and cache administration. */
public final class CacheKeyConstants {

    private CacheKeyConstants() {
    }

    /** Bucket cache key namespace: {@code QC:V4:B<bucketSeconds>:<queryHash>:<bucketStartTs>}. */
    public static final String CACHE_KEY_PREFIX = "QC:V4";

    /** Query-tracker sorted-set of popularity: members = query hash, score = cumulative hits. */
    public static final String QUERY_TRACKER_TOP_SORTED_SET_KEY = "QT:V1:TOP";

    /** Per-query metadata hash prefix. Full key: {@code QT:V1:META:<queryHash>}. */
    public static final String QUERY_TRACKER_META_KEY_PREFIX = "QT:V1:META";

    public static final String QUERY_TRACKER_KEY_PREFIX = "QT:V1";
}
