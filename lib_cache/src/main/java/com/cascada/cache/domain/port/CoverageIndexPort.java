package com.cascada.cache.domain.port;

import com.cascada.cache.domain.index.BucketCoverageBitmap;
import com.cascada.identity.domain.QueryHash;

import java.util.Optional;

/**
 * Outbound port for the coverage-bitmap index (plan Appendix J.1). One bitmap per
 * {@code (queryHash, bucketSeconds)} family answers "which buckets of this query are cached?"
 * in a single fetch, replacing the per-bucket pipelined EXISTS phase.
 *
 * <p>The index is advisory: when {@link #load} returns empty the engine falls back to the
 * authoritative {@link CacheBackendPort#existsForKeys} path, and a stale bit is corrected by the
 * engine's vanished-bucket guard. Writers ({@code store}/eviction paths) call
 * {@link #markCached}/{@link #markEvicted} to keep the bitmap warm.
 */
public interface CoverageIndexPort {

    /** The coverage bitmap for this query family, or empty if none is indexed yet. */
    Optional<BucketCoverageBitmap> load(QueryHash queryHash, long bucketSeconds);

    /** Record that one bucket of this family is now cached. */
    void markCached(QueryHash queryHash, long bucketSeconds, long bucketStartTimestampSeconds);

    /** Record that one bucket of this family was evicted/expired. */
    void markEvicted(QueryHash queryHash, long bucketSeconds, long bucketStartTimestampSeconds);
}
