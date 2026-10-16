package com.cascada.cache.application.service.merge;

import java.math.BigDecimal;

/**
 * Spark's value semantics shared by the merge and the cube, so both group, order and bound values
 * exactly as Spark does.
 */
public final class SparkValues {

    /** Spark's maximum DECIMAL precision and scale. */
    public static final int MAX_DECIMAL_PRECISION = 38;

    private SparkValues() {
    }

    /** Spark's binary UTF-8 ordering is code-point order for valid Unicode strings. */
    public static int compareStrings(String leftString, String rightString) {
        int leftOffset = 0;
        int rightOffset = 0;
        while (leftOffset < leftString.length() && rightOffset < rightString.length()) {
            int leftCodePoint = leftString.codePointAt(leftOffset);
            int rightCodePoint = rightString.codePointAt(rightOffset);
            int compared = Integer.compare(leftCodePoint, rightCodePoint);
            if (compared != 0) {
                return compared;
            }
            leftOffset += Character.charCount(leftCodePoint);
            rightOffset += Character.charCount(rightCodePoint);
        }
        return Integer.compare(leftString.length() - leftOffset, rightString.length() - rightOffset);
    }

    /** Spark normalises grouping keys: {@code -0.0} groups with {@code 0.0} and every NaN is one NaN. */
    public static double canonicalDouble(double value) {
        if (Double.isNaN(value)) {
            return Double.NaN;
        }
        return value == 0.0d ? 0.0d : value;
    }

    /** Grouping identity of a DECIMAL key: {@code 1.0} and {@code 1.00} are the same group. */
    public static BigDecimal canonicalDecimal(BigDecimal value) {
        return value.stripTrailingZeros();
    }

    /** Rejects a DECIMAL value Spark could not represent (precision or scale above 38). */
    public static void requireDecimal(BigDecimal value, String column) {
        long integerDigits = (long) value.precision() - Math.min(0L, (long) value.scale());
        if (value.precision() > MAX_DECIMAL_PRECISION || integerDigits > MAX_DECIMAL_PRECISION
                || Math.abs((long) value.scale()) > MAX_DECIMAL_PRECISION) {
            throw new IllegalArgumentException("DECIMAL value of column '" + column
                    + "' exceeds Spark's maximum precision/scale of " + MAX_DECIMAL_PRECISION);
        }
    }

    /** DECIMAL SUM with Spark's bound applied to the result. */
    public static BigDecimal decimalSum(BigDecimal left, BigDecimal right, String column) {
        BigDecimal sum = left.add(right);
        requireDecimal(sum, column);
        return sum;
    }
}
