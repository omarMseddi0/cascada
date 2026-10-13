package com.cascada.cache.domain.frame;

public final class FrameColumnReader {
    private final ColumnType type;
    private final long[] longs;
    private final double[] doubles;
    private final String[] strings;
    private final java.math.BigDecimal[] decimals;
    private final long[] validity;
    private final int length;

    FrameColumnReader(ColumnType type, Object values, long[] validity, int length) {
        this.type = type;
        longs = type == ColumnType.LONG ? (long[]) values : null;
        doubles = type == ColumnType.DOUBLE ? (double[]) values : null;
        strings = type == ColumnType.STRING ? (String[]) values : null;
        decimals = type == ColumnType.DECIMAL ? (java.math.BigDecimal[]) values : null;
        this.validity = validity;
        this.length = length;
    }

    public ColumnType type() { return type; }
    public boolean isNullAt(int row) {
        if (row < 0 || row >= length) throw new IndexOutOfBoundsException(row);
        return validity != null && (validity[row >>> 6] & (1L << (row & 63))) == 0;
    }
    // The scan checks kind and nullness before reading. Array access still checks row bounds.
    public long longValue(int row) { return longs[row]; }
    public double doubleValue(int row) { return doubles[row]; }
    public String stringValue(int row) { return strings[row]; }
    public java.math.BigDecimal decimalValue(int row) { return decimals[row]; }
}
