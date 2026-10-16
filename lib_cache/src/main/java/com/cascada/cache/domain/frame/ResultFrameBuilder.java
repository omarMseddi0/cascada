package com.cascada.cache.domain.frame;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ResultFrameBuilder {
    private final List<String> names = new ArrayList<>();
    private final Map<String, ColumnType> types = new LinkedHashMap<>();
    private Object[] columns;
    private long[][] present;
    private int capacity;
    private int rowCount;
    private int nextColumn;
    private int expectedRows = 16;
    private ColumnType[] indexedTypes;
    private boolean sharedSnapshot;

    /** Allocation hint; does not change the number of rows or limit subsequent growth. */
    public ResultFrameBuilder expectedRows(int rows) {
        if (rows < 0) throw new IllegalArgumentException("expected rows must not be negative");
        if (columns != null) throw new IllegalStateException("set expected rows before appending cells");
        expectedRows = Math.max(16, rows);
        return this;
    }

    public ResultFrameBuilder column(String name, ColumnType type) {
        if (columns != null) {
            throw new IllegalStateException("schema cannot change after rows have been appended");
        }
        if (types.putIfAbsent(name, type) != null) {
            throw new IllegalArgumentException("duplicate column '" + name + "'");
        }
        names.add(name);
        return this;
    }

    /** Compatibility path for callers at object-oriented boundaries. */
    public ResultFrameBuilder row(Map<String, Object> values) {
        for (String name : names) {
            appendValue(values.get(name));
        }
        return this;
    }

    /** Convenience typed row path; values must match the declared schema order. */
    public ResultFrameBuilder row(Object... values) {
        if (values.length != names.size()) {
            throw new IllegalArgumentException("expected " + names.size() + " values but got " + values.length);
        }
        for (Object value : values) {
            appendValue(value);
        }
        return this;
    }

    public ResultFrameBuilder appendLong(long value) {
        requireType(ColumnType.LONG);
        ensureCapacityForCell();
        ((long[]) columns[nextColumn])[rowCount] = value;
        advance();
        return this;
    }

    public ResultFrameBuilder appendDouble(double value) {
        requireType(ColumnType.DOUBLE);
        ensureCapacityForCell();
        ((double[]) columns[nextColumn])[rowCount] = value;
        advance();
        return this;
    }

    public ResultFrameBuilder appendString(String value) {
        requireType(ColumnType.STRING);
        ensureCapacityForCell();
        ((String[]) columns[nextColumn])[rowCount] = value;
        if (value == null) markNull();
        advance();
        return this;
    }

    public ResultFrameBuilder appendDecimal(java.math.BigDecimal value) {
        requireType(ColumnType.DECIMAL);
        ensureCapacityForCell();
        ((java.math.BigDecimal[]) columns[nextColumn])[rowCount] = value;
        if (value == null) markNull();
        advance();
        return this;
    }

    public ResultFrameBuilder appendNull() {
        ensureCapacityForCell();
        markNull();
        advance();
        return this;
    }

    /** Copies a typed cell without creating a row map or boxing primitive numeric values. */
    public ResultFrameBuilder appendCell(ResultFrame source, int row, int column) {
        if (source.isNullAt(row, column)) return appendNull();
        return switch (source.columnTypeAt(column)) {
            case LONG -> appendLong(source.longAt(row, column));
            case DOUBLE -> appendDouble(source.doubleAt(row, column));
            case STRING -> appendString(source.stringAt(row, column));
            case DECIMAL -> appendDecimal((java.math.BigDecimal) source.valueAt(row, column));
        };
    }

    public ResultFrame build() {
        if (nextColumn != 0) {
            throw new IllegalStateException("a partial row is pending");
        }
        initialize();
        if (capacity == rowCount) {
            // A precisely sized frame can retain these arrays. Further builder writes detach
            // them first, preserving the immutable snapshot without a full-frame build copy.
            sharedSnapshot = true;
            return new ResultFrame(names, types, columns.clone(), present.clone(), rowCount);
        }
        Object[] trimmedColumns = new Object[names.size()];
        long[][] trimmedPresent = new long[names.size()][];
        for (int column = 0; column < names.size(); column++) {
            trimmedColumns[column] = growColumn(columns[column], types.get(names.get(column)), rowCount);
            trimmedPresent[column] = present[column] == null ? null
                    : java.util.Arrays.copyOf(present[column], bitmapWords(rowCount));
        }
        return new ResultFrame(names, types, trimmedColumns, trimmedPresent, rowCount);
    }

    /** Appends a boxed value converted to the next column's type; {@code null} appends a NULL. */
    public ResultFrameBuilder appendValue(Object value) {
        if (value == null) {
            return appendNull();
        }
        initialize();
        return switch (indexedTypes[nextColumn]) {
            case LONG -> appendLong(((Number) value).longValue());
            case DOUBLE -> appendDouble(((Number) value).doubleValue());
            case STRING -> appendString(value.toString());
            case DECIMAL -> appendDecimal((java.math.BigDecimal) value);
        };
    }

    void requireType(ColumnType expected) {
        initialize();
        if (nextColumn >= names.size() || indexedTypes[nextColumn] != expected) {
            String actual = nextColumn >= names.size() ? "no column" : indexedTypes[nextColumn].toString();
            throw new IllegalStateException("expected next column to be " + expected + " but was " + actual);
        }
    }

    void ensureCapacityForCell() {
        initialize();
        if (rowCount < capacity && !sharedSnapshot) {
            return;
        }
        int grown = rowCount < capacity ? capacity : Math.max(16, Math.multiplyExact(capacity, 2));
        for (int column = 0; column < names.size(); column++) {
            ColumnType type = indexedTypes[column];
            columns[column] = growColumn(columns[column], type, grown);
            if (present[column] != null) {
                int oldWords = present[column].length;
                present[column] = java.util.Arrays.copyOf(present[column], bitmapWords(grown));
                java.util.Arrays.fill(present[column], oldWords, present[column].length, -1L);
            }
        }
        capacity = grown;
        sharedSnapshot = false;
    }

    void initialize() {
        if (columns != null) {
            return;
        }
        capacity = expectedRows;
        columns = new Object[names.size()];
        present = new long[names.size()][];
        indexedTypes = new ColumnType[names.size()];
        for (int column = 0; column < names.size(); column++) {
            indexedTypes[column] = types.get(names.get(column));
            columns[column] = newColumn(indexedTypes[column], capacity);

        }
    }

    void advance() {
        nextColumn++;
        if (nextColumn == names.size()) {
            nextColumn = 0;
            rowCount++;
        }
    }

    void markNull() {
        long[] bitmap = present[nextColumn];
        if (bitmap == null) {
            bitmap = new long[bitmapWords(capacity)];
            java.util.Arrays.fill(bitmap, -1L);
            present[nextColumn] = bitmap;
        }
        // Append-only rows are valid by default, including future rows after a growth.
        // Only nulls need a bit write; non-null cells never re-mark already-set validity bits.
        bitmap[rowCount >>> 6] &= ~(1L << (rowCount & 63));
    }

    /** A typed column array (long[], double[], String[] or BigDecimal[]) of {@code length} cells. */
    public static Object newColumn(ColumnType type, int length) {
        return switch (type) {
            case LONG -> new long[length];
            case DOUBLE -> new double[length];
            case STRING -> new String[length];
            case DECIMAL -> new java.math.BigDecimal[length];
        };
    }

    /** {@code source} copied into a typed column array of {@code length} cells. */
    public static Object growColumn(Object source, ColumnType type, int length) {
        return switch (type) {
            case LONG -> java.util.Arrays.copyOf((long[]) source, length);
            case DOUBLE -> java.util.Arrays.copyOf((double[]) source, length);
            case STRING -> java.util.Arrays.copyOf((String[]) source, length);
            case DECIMAL -> java.util.Arrays.copyOf((java.math.BigDecimal[]) source, length);
        };
    }

    private static int bitmapWords(int rows) {
        return (int) (((long) rows + 63L) >>> 6);
    }
}
