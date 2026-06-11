package com.cascada.cache.adapter.index;

import com.cascada.cache.domain.index.BucketCoverageBitmap;
import com.cascada.cache.domain.port.CoverageIndexPort;
import com.cascada.identity.domain.QueryHash;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory {@link CoverageIndexPort} — the dev/test twin of the future Valkey-backed adapter (which
 * stores the same {@link BucketCoverageBitmap#toBytes()} words under {@code CV:B<seconds>:<hash>} and
 * mutates them with SETBIT). Bitmap mutations are serialized per family via synchronization on the
 * bitmap instance, matching the single-writer semantics a Valkey SETBIT gives for free.
 */
public final class InMemoryCoverageIndexAdapter implements CoverageIndexPort {

    private final Map<String, BucketCoverageBitmap> bitmapsByFamily = new ConcurrentHashMap<>();

    @Override
    public Optional<BucketCoverageBitmap> load(QueryHash queryHash, long bucketSeconds) {
        return Optional.ofNullable(bitmapsByFamily.get(familyKey(queryHash, bucketSeconds)));
    }

    @Override
    public void markCached(QueryHash queryHash, long bucketSeconds, long bucketStartTimestampSeconds) {
        BucketCoverageBitmap bitmap = bitmapsByFamily.computeIfAbsent(
                familyKey(queryHash, bucketSeconds), ignored -> new BucketCoverageBitmap(bucketSeconds));
        synchronized (bitmap) {
            bitmap.cover(bucketStartTimestampSeconds);
        }
    }

    @Override
    public void markEvicted(QueryHash queryHash, long bucketSeconds, long bucketStartTimestampSeconds) {
        BucketCoverageBitmap bitmap = bitmapsByFamily.get(familyKey(queryHash, bucketSeconds));
        if (bitmap != null) {
            synchronized (bitmap) {
                bitmap.uncover(bucketStartTimestampSeconds);
            }
        }
    }

    private String familyKey(QueryHash queryHash, long bucketSeconds) {
        return "CV:B" + bucketSeconds + ":" + queryHash.value();
    }
}
