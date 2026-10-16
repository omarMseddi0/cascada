package com.cascada.cache.domain.time;

import java.math.BigInteger;

/** Bucket-boundary arithmetic shared by planning, warming, safety rules and the merge. */
public final class BucketMath {

    private BucketMath() {
    }

    /** Start of the {@code width}-second bucket containing {@code timestamp}; correct before 1970 too. */
    public static long floor(long timestamp, long width) {
        return Math.floorDiv(timestamp, width) * width;
    }

    /** {@link #floor(long, long)} without overflow, for windows that reach the edges of the long range. */
    public static BigInteger floor(BigInteger timestamp, BigInteger width) {
        BigInteger[] division = timestamp.divideAndRemainder(width);
        BigInteger quotient = division[0];
        if (division[1].signum() < 0) {
            quotient = quotient.subtract(BigInteger.ONE);
        }
        return quotient.multiply(width);
    }
}
