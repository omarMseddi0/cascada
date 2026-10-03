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
 *   <li>{@link #deduplicateExactRows(int, long[], int[][], double[][])}: the RC3 guard — drops rows equal
 *       across bucket, dimensions and measures (double equality by {@link Double#doubleToLongBits},
 *       matching {@code record}/{@code Map} equality of the previous row-object implementation);</li>
 *   <li>{@link #aggregate(int, long[], int[][], double[][], AggregateFunction[], boolean[])}:
 *       group-by (bucket, dimension codes) combining each measure with its
 *       {@link AggregateFunction}. The compatibility overload treats a {@link Double#NaN} cell as
 *       absent; overloads with an explicit validity matrix preserve NaN as a real value.</li>
 * </ul>
 */
public final class ColumnarHashAggregator {

    private static final int EMPTY_SLOT = -1;
    private static final long GOLDEN = 0x9E3779B97F4A7C15L;
    private static final long MIX = 0xC2B2AE3D27D4EB4FL;

    /**
     * Grouped output in dense-group-id order (first-seen order). Dimension codes are stored flat
     * with stride {@code dimensionCount}; {@code measureAccumulators[measureIndex][groupIndex]} is
     * measure {@code measureIndex} of that group. {@code measurePresent[measureIndex][groupIndex]}
     * distinguishes a valid NaN from no contribution.
     */
    public record GroupedResult(int groupCount, long[] groupBuckets, int[] groupDimensionCodes,
                                int dimensionCount, double[][] measureAccumulators,
                                boolean[][] measurePresent) {

        /** Retains the original constructor and its NaN-as-absent convention for callers. */
        public GroupedResult(int groupCount, long[] groupBuckets, int[] groupDimensionCodes,
                             int dimensionCount, double[][] measureAccumulators) {
            this(groupCount, groupBuckets, groupDimensionCodes, dimensionCount, measureAccumulators,
                    inferPresence(measureAccumulators));
        }

        public int dimensionCode(int groupIndex, int dimensionIndex) {
            return groupDimensionCodes[groupIndex * dimensionCount + dimensionIndex];
        }

        private static boolean[][] inferPresence(double[][] measures) {
            boolean[][] present = new boolean[measures.length][];
            for (int measureIndex = 0; measureIndex < measures.length; measureIndex++) {
                present[measureIndex] = new boolean[measures[measureIndex].length];
                for (int groupIndex = 0; groupIndex < measures[measureIndex].length; groupIndex++) {
                    present[measureIndex][groupIndex] = !Double.isNaN(measures[measureIndex][groupIndex]);
                }
            }
            return present;
        }
    }

    /**
     * Exact-duplicate-row elimination (RC3) using the legacy NaN-as-missing convention. Returns a
     * keep-mask where the first occurrence of each equal (bucket, dimension, measure) row is {@code true}.
     * Use the validity overload when a measure can legitimately be NaN. {@code buckets} may be null
     * when there is no time column.
     */
    public boolean[] deduplicateExactRows(int rowCount, long[] buckets, int[][] dimensionCodes,
                                          double[][] measures) {
        return deduplicateExactRows(rowCount, buckets, dimensionCodes, measures, null);
    }

    /**
     * Exact-row deduplication with explicit measure-cell validity. A missing cell compares
     * differently from a valid NaN; the four-argument overload retains its legacy sentinel behavior.
     */
    public boolean[] deduplicateExactRows(int rowCount, long[] buckets, int[][] dimensionCodes,
                                          double[][] measures, boolean[][] measurePresent) {
        boolean[] keep = new boolean[rowCount];
        int capacity = tableCapacityFor(rowCount);
        long mask = capacity - 1L;
        int[] slots = new int[capacity];
        Arrays.fill(slots, EMPTY_SLOT);

        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            long hash = hashRow(buckets, dimensionCodes, measures, measurePresent, rowIndex);
            int slotIndex = (int) (hash & mask);
            boolean duplicate = false;
            while (slots[slotIndex] != EMPTY_SLOT) {
                int priorRowIndex = slots[slotIndex];
                if (rowsEqual(buckets, dimensionCodes, measures, measurePresent, rowIndex, priorRowIndex)) {
                    duplicate = true;
                    break;
                }
                slotIndex = (int) ((slotIndex + 1) & mask);
            }
            if (!duplicate) {
                slots[slotIndex] = rowIndex;
                keep[rowIndex] = true;
            }
        }
        return keep;
    }

    /**
     * Groups rows by (bucket, dimension codes) and combines measures using the legacy
     * NaN-as-missing convention. Use the explicit-validity overload when NaN is a real value.
     * {@code groupBuckets} may be {@code null} (no time column); {@code keepMask} may be {@code null}
     * (keep all rows) or the
     * output of {@link #deduplicateExactRows(int, long[], int[][], double[][])}.
     */
    public GroupedResult aggregate(int rowCount, long[] groupBuckets, int[][] dimensionCodes,
                                   double[][] measures, AggregateFunction[] functions,
                                   boolean[] keepMask) {
        return aggregate(rowCount, groupBuckets, dimensionCodes, measures, functions, keepMask, null);
    }

    /**
     * Groups rows using explicit measure-cell validity. When {@code measurePresent} is supplied,
     * valid NaNs participate in the selected aggregate and absent cells are ignored.
     */
    public GroupedResult aggregate(int rowCount, long[] groupBuckets, int[][] dimensionCodes,
                                   double[][] measures, AggregateFunction[] functions,
                                   boolean[] keepMask, boolean[][] measurePresent) {
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
        boolean[][] accumulatedPresent = new boolean[measureCount][groupCapacity];
        for (double[] accumulator : accumulators) {
            Arrays.fill(accumulator, Double.NaN);
        }
        int groupCount = 0;

        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            if (keepMask != null && !keepMask[rowIndex]) {
                continue;
            }
            long bucket = groupBuckets == null ? 0L : groupBuckets[rowIndex];
            long hash = hashGroupKey(bucket, dimensionCodes, rowIndex);
            int slotIndex = (int) (hash & mask);
            int groupIndex = EMPTY_SLOT;
            while (slots[slotIndex] != EMPTY_SLOT) {
                int candidateGroup = slots[slotIndex];
                if (groupKeyEquals(candidateGroup, bucket, dimensionCodes, rowIndex, outBuckets, outCodes,
                        dimensionCount)) {
                    groupIndex = candidateGroup;
                    break;
                }
                slotIndex = (int) ((slotIndex + 1) & mask);
            }
            if (groupIndex == EMPTY_SLOT) {
                if (groupCount == groupCapacity) {
                    int grown = groupCapacity * 2;
                    if (outBuckets != null) {
                        outBuckets = Arrays.copyOf(outBuckets, grown);
                    }
                    outCodes = Arrays.copyOf(outCodes, grown * dimensionCount);
                    for (int measureIndex = 0; measureIndex < measureCount; measureIndex++) {
                        double[] widened = Arrays.copyOf(accumulators[measureIndex], grown);
                        Arrays.fill(widened, groupCapacity, grown, Double.NaN);
                        accumulators[measureIndex] = widened;
                        accumulatedPresent[measureIndex] = Arrays.copyOf(accumulatedPresent[measureIndex], grown);
                    }
                    groupCapacity = grown;
                }
                groupIndex = groupCount++;
                if (outBuckets != null) {
                    outBuckets[groupIndex] = bucket;
                }
                int dimensionOffset = groupIndex * dimensionCount;
                for (int dimensionIndex = 0; dimensionIndex < dimensionCount; dimensionIndex++) {
                    outCodes[dimensionOffset + dimensionIndex] = dimensionCodes[dimensionIndex][rowIndex];
                }
                slots[slotIndex] = groupIndex;
            }

            for (int measureIndex = 0; measureIndex < measureCount; measureIndex++) {
                double incomingMeasure = measures[measureIndex][rowIndex];
                boolean incomingPresent = measurePresent == null
                        ? !Double.isNaN(incomingMeasure)
                        : measurePresent[measureIndex][rowIndex];
                if (!incomingPresent) {
                    continue; // absent in this row
                }
                if (!accumulatedPresent[measureIndex][groupIndex]) {
                    accumulators[measureIndex][groupIndex] = incomingMeasure;
                    accumulatedPresent[measureIndex][groupIndex] = true;
                } else {
                    accumulators[measureIndex][groupIndex] = combine(functions[measureIndex],
                            accumulators[measureIndex][groupIndex], incomingMeasure);
                }
            }
        }

        return new GroupedResult(groupCount,
                outBuckets == null ? null : Arrays.copyOf(outBuckets, groupCount),
                Arrays.copyOf(outCodes, groupCount * dimensionCount),
                dimensionCount,
                trimAccumulators(accumulators, groupCount),
                trimPresence(accumulatedPresent, groupCount));
    }

    // --- internals -------------------------------------------------------------------------------

    /** Enum-switch instead of a virtual {@code combine} call so the JIT keeps the loop monomorphic. */
    private static double combine(AggregateFunction function, double left, double right) {
        return switch (function) {
            case SUM, COUNT -> left + right;
            case MINIMUM -> {
                if (Double.isNaN(left)) yield right;
                if (Double.isNaN(right)) yield left;
                yield Math.min(left, right);
            }
            case MAXIMUM -> Double.isNaN(left) || Double.isNaN(right) ? Double.NaN : Math.max(left, right);
        };
    }

    private static long hashGroupKey(long bucket, int[][] dimensionCodes, int rowIndex) {
        long hash = bucket * GOLDEN;
        for (int[] dimensionColumn : dimensionCodes) {
            hash = (hash ^ dimensionColumn[rowIndex]) * MIX;
        }
        return finalizeHash(hash);
    }

    private static long hashRow(long[] buckets, int[][] dimensionCodes, double[][] measures,
                                boolean[][] measurePresent, int rowIndex) {
        long hash = (buckets == null ? 0L : buckets[rowIndex]) * GOLDEN;
        for (int[] dimensionColumn : dimensionCodes) {
            hash = (hash ^ dimensionColumn[rowIndex]) * MIX;
        }
        for (int measureIndex = 0; measureIndex < measures.length; measureIndex++) {
            if (measurePresent != null) {
                boolean measureIsPresent = measurePresent[measureIndex][rowIndex];
                hash = (hash ^ (measureIsPresent ? 1L : 0L)) * MIX;
                if (!measureIsPresent) {
                    continue;
                }
            }
            hash = (hash ^ Double.doubleToLongBits(measures[measureIndex][rowIndex])) * MIX;
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
                                     boolean[][] measurePresent, int rowIndex, int otherRowIndex) {
        if (buckets != null && buckets[rowIndex] != buckets[otherRowIndex]) {
            return false;
        }
        for (int[] dimensionColumn : dimensionCodes) {
            if (dimensionColumn[rowIndex] != dimensionColumn[otherRowIndex]) {
                return false;
            }
        }
        for (int measureIndex = 0; measureIndex < measures.length; measureIndex++) {
            if (measurePresent != null) {
                boolean rowValuePresent = measurePresent[measureIndex][rowIndex];
                boolean otherValuePresent = measurePresent[measureIndex][otherRowIndex];
                if (rowValuePresent != otherValuePresent) {
                    return false;
                }
                if (!rowValuePresent) {
                    continue;
                }
            }
            if (Double.doubleToLongBits(measures[measureIndex][rowIndex])
                    != Double.doubleToLongBits(measures[measureIndex][otherRowIndex])) {
                return false;
            }
        }
        return true;
    }

    private static boolean groupKeyEquals(int groupIndex, long bucket, int[][] dimensionCodes, int rowIndex,
                                          long[] outBuckets, int[] outCodes, int dimensionCount) {
        if (outBuckets != null && outBuckets[groupIndex] != bucket) {
            return false;
        }
        int dimensionOffset = groupIndex * dimensionCount;
        for (int dimensionIndex = 0; dimensionIndex < dimensionCount; dimensionIndex++) {
            if (outCodes[dimensionOffset + dimensionIndex] != dimensionCodes[dimensionIndex][rowIndex]) {
                return false;
            }
        }
        return true;
    }

    private static double[][] trimAccumulators(double[][] accumulators, int groupCount) {
        double[][] trimmed = new double[accumulators.length][];
        for (int measureIndex = 0; measureIndex < accumulators.length; measureIndex++) {
            trimmed[measureIndex] = Arrays.copyOf(accumulators[measureIndex], groupCount);
        }
        return trimmed;
    }

    private static boolean[][] trimPresence(boolean[][] presence, int groupCount) {
        boolean[][] trimmed = new boolean[presence.length][];
        for (int measureIndex = 0; measureIndex < presence.length; measureIndex++) {
            trimmed[measureIndex] = Arrays.copyOf(presence[measureIndex], groupCount);
        }
        return trimmed;
    }

    /** Power-of-two table size targeting a load factor below 0.75 even if every row is distinct. */
    private static int tableCapacityFor(int rowCount) {
        int minimum = Math.max(16, rowCount + (rowCount >> 1));
        return Integer.highestOneBit(minimum - 1) << 1;
    }
}
