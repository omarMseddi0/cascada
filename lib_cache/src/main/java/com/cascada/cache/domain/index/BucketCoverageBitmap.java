package com.cascada.cache.domain.index;

import java.util.BitSet;
import java.util.List;

/**
 * The Index Fabric's coverage bitmap (plan Appendix J.1): for one {@code (queryHash, bucketSeconds)}
 * family, bit <em>i</em> means "the bucket whose start is {@code i * bucketSeconds} is cached".
 *
 * <p>This collapses gap analysis from one {@code EXISTS} round trip per bucket (a 30-day query costs
 * 30 pipelined commands) into <strong>one</strong> small value fetch plus pure bit-math: the engine
 * loads the bitmap once and answers every presence question locally. A day-bucket bitmap covering the
 * whole epoch-to-2100 range is under 6 KB; in practice only the live span's words are allocated.
 *
 * <p>Consistency contract: the bitmap is a <em>hint</em>, never an authority.
 * <ul>
 *   <li>A stale <b>present</b> bit is caught downstream — the MGET returns no value and the engine's
 *       vanished-bucket guard re-fetches exactly that day from Spark.</li>
 *   <li>A stale <b>absent</b> bit only costs a redundant Spark gap fetch (correct, just slower).</li>
 * </ul>
 * So coverage drift can cost latency but never data. A periodic reconciliation sweep (re-deriving the
 * bitmap from the backend's real keys) bounds the drift window.
 *
 * <p>Framework-free domain value. {@link #toBytes()} / {@link #fromBytes(long, byte[])} give adapters
 * a stable wire form (the raw little-endian {@link BitSet} words) for storing the bitmap as a single
 * Valkey value ({@code CV:B<bucketSeconds>:<queryHash>}).
 */
public final class BucketCoverageBitmap {

    private final long bucketSeconds;
    private final BitSet coveredOrdinals;

    public BucketCoverageBitmap(long bucketSeconds) {
        this(bucketSeconds, new BitSet());
    }

    private BucketCoverageBitmap(long bucketSeconds, BitSet coveredOrdinals) {
        if (bucketSeconds <= 0) {
            throw new IllegalArgumentException("bucketSeconds must be > 0, but was: " + bucketSeconds);
        }
        this.bucketSeconds = bucketSeconds;
        this.coveredOrdinals = coveredOrdinals;
    }

    public static BucketCoverageBitmap fromBytes(long bucketSeconds, byte[] words) {
        return new BucketCoverageBitmap(bucketSeconds, BitSet.valueOf(words));
    }

    public byte[] toBytes() {
        return coveredOrdinals.toByteArray();
    }

    public long bucketSeconds() {
        return bucketSeconds;
    }

    /** True iff the bucket starting at {@code bucketStartTimestampSeconds} is marked cached. */
    public boolean isCovered(long bucketStartTimestampSeconds) {
        int ordinal = ordinalOf(bucketStartTimestampSeconds);
        return ordinal >= 0 && coveredOrdinals.get(ordinal);
    }

    /** Mark one bucket cached. Pre-epoch buckets are not indexable and are ignored (EXISTS fallback). */
    public void cover(long bucketStartTimestampSeconds) {
        int ordinal = ordinalOf(bucketStartTimestampSeconds);
        if (ordinal >= 0) {
            coveredOrdinals.set(ordinal);
        }
    }

    /** Mark one bucket evicted/expired. */
    public void uncover(long bucketStartTimestampSeconds) {
        int ordinal = ordinalOf(bucketStartTimestampSeconds);
        if (ordinal >= 0) {
            coveredOrdinals.clear(ordinal);
        }
    }

    /** Presence mask for an ordered list of bucket starts — the bitmap analogue of pipelined EXISTS. */
    public List<Boolean> presenceMask(List<Long> bucketStartTimestamps) {
        return bucketStartTimestamps.stream().map(this::isCovered).toList();
    }

    public int coveredCount() {
        return coveredOrdinals.cardinality();
    }

    /**
     * Bucket ordinal = floorDiv(start, bucketSeconds). Negative (pre-epoch) ordinals cannot live in a
     * {@link BitSet}; they report -1 and the caller treats the bucket as not covered, which degrades
     * to the always-correct EXISTS/Spark path.
     */
    private int ordinalOf(long bucketStartTimestampSeconds) {
        long ordinal = Math.floorDiv(bucketStartTimestampSeconds, bucketSeconds);
        return (ordinal < 0 || ordinal > Integer.MAX_VALUE) ? -1 : (int) ordinal;
    }
}
