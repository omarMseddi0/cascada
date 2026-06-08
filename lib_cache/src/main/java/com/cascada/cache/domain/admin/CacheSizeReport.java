package com.cascada.cache.domain.admin;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * The result of the "how much is in cache?" button (plan §8.17, system card §10). It is the sum of the
 * <em>stored blob bytes</em> across the backend — exactly the bytes a Valkey/Redis or Delta cold tier
 * holds on the wire/on disk, not a live JVM-heap estimate — so the number a customer sees matches what
 * they are actually paying to store.
 *
 * <p>The report is framework-free domain data: the backend adapter measures the bytes (Valkey via
 * {@code MEMORY USAGE} / {@code STRLEN}, the in-memory fake by array length, a Delta cold tier via the
 * Delta snapshot size) and hands the totals here. {@link #totalMegabytes()} is the headline figure the
 * admin console renders; {@link #bytesByTenant()} drives the per-tenant breakdown bar.
 */
public final class CacheSizeReport {

    private static final double BYTES_PER_MEGABYTE = 1024.0 * 1024.0;

    private final long totalBytes;
    private final long bucketCount;
    private final Map<String, Long> bytesByTenant;

    public CacheSizeReport(long totalBytes, long bucketCount, Map<String, Long> bytesByTenant) {
        if (totalBytes < 0 || bucketCount < 0) {
            throw new IllegalArgumentException("a cache size report cannot have negative totals");
        }
        this.totalBytes = totalBytes;
        this.bucketCount = bucketCount;
        // copy + sort so the breakdown renders deterministically and callers cannot mutate the report
        this.bytesByTenant = Map.copyOf(new TreeMap<>(Objects.requireNonNull(bytesByTenant, "bytesByTenant")));
    }

    /** An explicitly empty report (cold start, or after a full flush). */
    public static CacheSizeReport empty() {
        return new CacheSizeReport(0L, 0L, Map.of());
    }

    public long totalBytes() {
        return totalBytes;
    }

    /** The headline number the button shows, rounded to two decimals (e.g. {@code 41.27} MB). */
    public double totalMegabytes() {
        return Math.round(totalBytes / BYTES_PER_MEGABYTE * 100.0) / 100.0;
    }

    /** How many cached buckets back that size — useful for "average bucket size" diagnostics. */
    public long bucketCount() {
        return bucketCount;
    }

    /** Stored bytes per tenant key-prefix segment; the admin console renders this as a breakdown. */
    public Map<String, Long> bytesByTenant() {
        return bytesByTenant;
    }

    @Override
    public String toString() {
        return "CacheSizeReport{" + totalMegabytes() + " MB across " + bucketCount + " buckets, "
                + bytesByTenant.size() + " tenant(s)}";
    }
}
