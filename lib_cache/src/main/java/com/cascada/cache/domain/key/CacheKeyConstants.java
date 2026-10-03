package com.cascada.cache.domain.key;

import java.util.regex.Pattern;

/** Stable key namespaces shared by cache-key construction and cache administration. */
public final class CacheKeyConstants {

    private CacheKeyConstants() {
    }

    /** Bucket cache key namespace: {@code QC:V4:B<bucketSeconds>:<queryHash>:<bucketStartTs>}. */
    public static final String CACHE_KEY_PREFIX = "QC:V4";

    private static final Pattern BUCKET_KEY = Pattern.compile(
            Pattern.quote(CACHE_KEY_PREFIX) + ":B[1-9][0-9]*:[^:]+:-?[0-9]+");

    /** Query-tracker sorted-set of popularity: members = query hash, score = cumulative hits. */
    public static final String QUERY_TRACKER_TOP_SORTED_SET_KEY = "QT:V1:TOP";

    /** Per-query metadata hash prefix. Full key: {@code QT:V1:META:<queryHash>}. */
    public static final String QUERY_TRACKER_META_KEY_PREFIX = "QT:V1:META";

    public static final String QUERY_TRACKER_KEY_PREFIX = "QT:V1";

    /** True only for a key in the cache bucket wire format. */
    public static boolean isBucketKey(String key) {
        return key != null && BUCKET_KEY.matcher(key).matches();
    }
}
