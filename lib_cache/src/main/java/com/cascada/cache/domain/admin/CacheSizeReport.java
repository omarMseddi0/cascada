package com.cascada.cache.domain.admin;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

/** A stable storage-size snapshot, including byte and bucket totals by tenant. */
public record CacheSizeReport(long totalBytes, long bucketCount, Map<String, Long> bytesByTenant,
                              Map<String, Long> bucketCountByTenant) {

    private static final double BYTES_PER_MEGABYTE = 1024.0 * 1024.0;

    /** Compatibility constructor for callers that only have byte totals by tenant. */
    public CacheSizeReport(long totalBytes, long bucketCount, Map<String, Long> bytesByTenant) {
        this(totalBytes, bucketCount, bytesByTenant, Map.of());
    }

    public CacheSizeReport {
        if (totalBytes < 0 || bucketCount < 0) {
            throw new IllegalArgumentException("a cache size report cannot have negative totals");
        }
        SortedMap<String, Long> sortedBytes = new TreeMap<>(
                Map.copyOf(Objects.requireNonNull(bytesByTenant, "bytesByTenant")));
        SortedMap<String, Long> sortedBucketCounts = new TreeMap<>(
                Map.copyOf(Objects.requireNonNull(bucketCountByTenant, "bucketCountByTenant")));
        bytesByTenant = Collections.unmodifiableSortedMap(sortedBytes);
        bucketCountByTenant = Collections.unmodifiableSortedMap(sortedBucketCounts);
    }

    /** An explicitly empty report (cold start, or after a full flush). */
    public static CacheSizeReport empty() {
        return new CacheSizeReport(0L, 0L, Map.of(), Map.of());
    }

    /** The headline number the button shows, rounded to two decimals (e.g. {@code 41.27} MB). */
    public double totalMegabytes() {
        return Math.round(totalBytes / BYTES_PER_MEGABYTE * 100.0) / 100.0;
    }

    /** Compact diagnostic summary that omits the tenant-by-tenant map. */
    @Override
    public String toString() {
        return "CacheSizeReport{" + totalMegabytes() + " MB across " + bucketCount + " buckets, "
                + bytesByTenant.size() + " tenant(s)}";
    }
}
