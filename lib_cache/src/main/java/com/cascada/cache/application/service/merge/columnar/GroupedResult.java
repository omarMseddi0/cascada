package com.cascada.cache.application.service.merge.columnar;

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

    static boolean[][] inferPresence(double[][] measures) {
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
