package com.cascada.cache.domain.key;

import com.cascada.identity.domain.QueryHash;

/**
 * Builds cache keys using the established bucket wire format:
 * {@code QC:V4:B<bucketSeconds>:<queryHash>:<bucketStartTs>}.
 */
public final class CacheKeyFactory {

    private CacheKeyFactory() {
    }

    /** Ported from {@code build_cache_key(query_hash, bucket_start_ts, bucket_seconds)}. */
    public static String buildBucketKey(QueryHash queryHash, long bucketStartTimestampSeconds, long bucketSeconds) {
        return CacheKeyConstants.CACHE_KEY_PREFIX
                + ":B" + bucketSeconds
                + ":" + queryHash.value()
                + ":" + bucketStartTimestampSeconds;
    }
}
