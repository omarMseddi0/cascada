package com.cascada.cache.domain.merge.columnar;

import com.cascada.cache.domain.merge.AggregateFunction;

import java.util.Arrays;

/**
 * The vectorized hash-aggregation operator every merge path routes through — the in-process
 * equivalent of Spark's {@code HashAggregateExec} / Presto's {@code GroupByHash}. Inputs are
 * columnar primitive arrays (dictionary codes for dimensions, {@code double[]} for measures,
 * an optional {@code long[]} time-bucket column); grouping runs over an open-addressing linear-probe
 * table of int slots, and measures combine into flat {@code double[]} accumulators indexed by dense
 * group id. The hot loop allocates nothing per row and never boxes.
 *
 * <p>Two operations, both O(rows):
 * <ul>
 *   <li>{@link #deduplicateExactRows}: the RC3 guard — drops rows byte-identical across bucket,
 *       dimensions and measures (double equality by {@link Double#doubleToLongBits}, matching
 *       {@code record}/{@code Map} equality of the previous row-object implementation);</li>
 *   <li>{@link #aggregate}: group-by (bucket, dimension codes) combining each measure with its
 *       {@link AggregateFunction}. A {@link Double#NaN} measure cell means "absent in this row"
 *       and does not contribute.</li>
 * </ul>
 */
public final class ColumnarHashAggregator {

    private static final int EMPTY_SLOT = -1;
    private static final long GOLDEN = 0x9E3779B97F4A7C15L;
    private static final long MIX = 0xC2B2AE3D27D4EB4FL;

    /**
     * Grouped output in dense-group-id order (first-seen order). Dimension codes are stored flat
     * with stride {@code dimensionCount}; {@code measureAccumulators[m][g]} is measure {@code m} of
     * group {@code g}, {@link Double#NaN} if no row contributed it.
     */
    public record GroupedResult(int groupCount, long[] groupBuckets, int[] groupDimensionCodes,
                                int dimensionCount, double[][] measureAccumulators) {

        public int dimensionCode(int group, int dimension) {
            return groupDimensionCodes[group * dimensionCount + dimension];
        }
    }

    /**
     * Exact-duplicate-row elimination (RC3): returns a keep-mask where the first occurrence of each
     * identical (bucket, dimensions, measures) row is {@code true} and every repeat is {@code false}.
     * {@code buckets} may be {@code null} when the input has no time column.
     */
    public boolean[] deduplicateExactRows(int rowCount, long[] buckets, int[][] dimensionCodes,
                                          double[][] measures) {
        boolean[] keep = new boolean[rowCount];
        int capacity = tableCapacityFor(rowCount);
        long mask = capacity - 1L;
        int[] slots = new int[capacity];
        Arrays.fill(slots, EMPTY_SLOT);

        for (int row = 0; row < rowCount; row++) {
            long hash = hashRow(buckets, dimensionCodes, measures, row);
            int index = (int) (hash & mask);
            boolean duplicate = false;
            while (slots[index] != EMPTY_SLOT) {
                int other = slots[index];
                if (rowsEqual(buckets, dimensionCodes, measures, row, other)) {
                    duplicate = true;
                    break;
                }
                index = (int) ((index + 1) & mask);
            }
            if (!duplicate) {
                slots[index] = row;
                keep[row] = true;
            }
        }
        return keep;
    }

    /**
     * Groups rows by (bucket, dimension codes) and combines measures. {@code groupBuckets} may be
     * {@code null} (no time column); {@code keepMask} may be {@code null} (keep all rows) or the
     * output of {@link #deduplicateExactRows}.
     */
    public GroupedResult aggregate(int rowCount, long[] groupBuckets, int[][] dimensionCodes,
                                   double[][] measures, AggregateFunction[] functions,
                                   boolean[] keepMask) {
        int dimensionCount = dimensionCodes.length;
        int measureCount = measures.length;

        int capacity = tableCapacityFor(rowCount);
        long mask = capacity - 1L;
        int[] slots = new int[capacity];
        Arrays.fill(slots, EMPTY_SLOT);

        int groupCapacity = Math.max(16, rowCount / 4);
        long[] outBuckets = groupBuckets == null ? null : new long[groupCapacity];
        int[] outCodes = new int[groupCapacity * dimensionCount];
        double[][] accumulators = new double[measureCount][groupCapacity];
        for (double[] accumulator : accumulators) {
            Arrays.fill(accumulator, Double.NaN);
        }
        int groupCount = 0;

        for (int row = 0; row < rowCount; row++) {
            if (keepMask != null && !keepMask[row]) {
                continue;
            }
            long bucket = groupBuckets == null ? 0L : groupBuckets[row];
            long hash = hashGroupKey(bucket, dimensionCodes, row);
            int index = (int) (hash & mask);
            int group = EMPTY_SLOT;
            while (slots[index] != EMPTY_SLOT) {
                int candidate = slots[index];
                if (groupKeyEquals(candidate, bucket, dimensionCodes, row, outBuckets, outCodes,
                        dimensionCount)) {
                    group = candidate;
                    break;
                }
                index = (int) ((index + 1) & mask);
            }
            if (group == EMPTY_SLOT) {
                if (groupCount == groupCapacity) {
                    int grown = groupCapacity * 2;
                    if (outBuckets != null) {
                        outBuckets = Arrays.copyOf(outBuckets, grown);
                    }
                    outCodes = Arrays.copyOf(outCodes, grown * dimensionCount);
                    for (int m = 0; m < measureCount; m++) {
                        double[] widened = Arrays.copyOf(accumulators[m], grown);
                        Arrays.fill(widened, groupCapacity, grown, Double.NaN);
                        accumulators[m] = widened;
                    }
                    groupCapacity = grown;
                }
                group = groupCount++;
                if (outBuckets != null) {
                    outBuckets[group] = bucket;
                }
                int base = group * dimensionCount;
                for (int d = 0; d < dimensionCount; d++) {
                    outCodes[base + d] = dimensionCodes[d][row];
                }
                slots[index] = group;
            }

            for (int m = 0; m < measureCount; m++) {
                double incoming = measures[m][row];
                if (Double.isNaN(incoming)) {
                    continue; // absent in this row
                }
                double current = accumulators[m][group];
                accumulators[m][group] = Double.isNaN(current)
                        ? incoming
                        : combine(functions[m], current, incoming);
            }
        }

        return new GroupedResult(groupCount,
                outBuckets == null ? null : Arrays.copyOf(outBuckets, groupCount),
                Arrays.copyOf(outCodes, groupCount * dimensionCount),
                dimensionCount,
                trimAccumulators(accumulators, groupCount));
    }

    // --- internals -------------------------------------------------------------------------------

    /** Enum-switch instead of a virtual {@code combine} call so the JIT keeps the loop monomorphic. */
    private static double combine(AggregateFunction function, double left, double right) {
        return switch (function) {
            case SUM, COUNT -> left + right;
            case MINIMUM -> Math.min(left, right);
            case MAXIMUM -> Math.max(left, right);
        };
    }

    private static long hashGroupKey(long bucket, int[][] dimensionCodes, int row) {
        long hash = bucket * GOLDEN;
        for (int[] column : dimensionCodes) {
            hash = (hash ^ column[row]) * MIX;
        }
        return finalizeHash(hash);
    }

    private static long hashRow(long[] buckets, int[][] dimensionCodes, double[][] measures, int row) {
        long hash = (buckets == null ? 0L : buckets[row]) * GOLDEN;
        for (int[] column : dimensionCodes) {
            hash = (hash ^ column[row]) * MIX;
        }
        for (double[] column : measures) {
            hash = (hash ^ Double.doubleToLongBits(column[row])) * MIX;
        }
        return finalizeHash(hash);
    }

    private static long finalizeHash(long hash) {
        hash ^= hash >>> 29;
        hash *= 0xBF58476D1CE4E5B9L;
        hash ^= hash >>> 32;
        return hash;
    }

    private static boolean rowsEqual(long[] buckets, int[][] dimensionCodes, double[][] measures,
                                     int row, int other) {
        if (buckets != null && buckets[row] != buckets[other]) {
            return false;
        }
        for (int[] column : dimensionCodes) {
            if (column[row] != column[other]) {
                return false;
            }
        }
        for (double[] column : measures) {
            if (Double.doubleToLongBits(column[row]) != Double.doubleToLongBits(column[other])) {
                return false;
            }
        }
        return true;
    }

    private static boolean groupKeyEquals(int group, long bucket, int[][] dimensionCodes, int row,
                                          long[] outBuckets, int[] outCodes, int dimensionCount) {
        if (outBuckets != null && outBuckets[group] != bucket) {
            return false;
        }
        int base = group * dimensionCount;
        for (int d = 0; d < dimensionCount; d++) {
            if (outCodes[base + d] != dimensionCodes[d][row]) {
                return false;
            }
        }
        return true;
    }

    private static double[][] trimAccumulators(double[][] accumulators, int groupCount) {
        double[][] trimmed = new double[accumulators.length][];
        for (int m = 0; m < accumulators.length; m++) {
            trimmed[m] = Arrays.copyOf(accumulators[m], groupCount);
        }
        return trimmed;
    }

    /** Power-of-two table size targeting a load factor below 0.75 even if every row is distinct. */
    private static int tableCapacityFor(int rowCount) {
        int minimum = Math.max(16, rowCount + (rowCount >> 1));
        return Integer.highestOneBit(minimum - 1) << 1;
    }
}
