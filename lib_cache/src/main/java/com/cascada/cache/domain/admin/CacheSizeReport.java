package com.cascada.cache.domain.admin;

/** A stable storage-size snapshot for all cached query buckets. */
public record CacheSizeReport(long totalBytes, long bucketCount) {

    private static final double BYTES_PER_MEGABYTE = 1024.0 * 1024.0;

    public CacheSizeReport {
        if (totalBytes < 0 || bucketCount < 0) {
            throw new IllegalArgumentException("a cache size report cannot have negative totals");
        }
    }

    /** An explicitly empty report (cold start, or after a full flush). */
    public static CacheSizeReport empty() {
        return new CacheSizeReport(0L, 0L);
    }

    /** The headline number the button shows, rounded to two decimals (e.g. {@code 41.27} MB). */
    public double totalMegabytes() {
        return Math.round(totalBytes / BYTES_PER_MEGABYTE * 100.0) / 100.0;
    }

    @Override
    public String toString() {
        return "CacheSizeReport{" + totalMegabytes() + " MB across " + bucketCount + " buckets}";
    }
}
