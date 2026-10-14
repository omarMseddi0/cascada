package com.cascada.cache.exception;

/** Raised before a request materializes or visits more buckets than the configured hard ceiling. */
public final class BucketEnumerationLimitExceededException extends IllegalArgumentException {

    public BucketEnumerationLimitExceededException(int maximumBuckets) {
        super("bucket plan exceeds the maximum of " + maximumBuckets + " buckets");
    }
}
