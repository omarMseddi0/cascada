package com.cascada.cache.domain.frame;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A compact, immutable, typed result table used by the cache.
 *
 * <p>Data is stored column-by-column in primitive {@code long[]} and {@code double[]} arrays (and
 * {@code String[]} for dimensions), with a packed null bitmap only for columns containing nulls. This is deliberately
 * unlike a {@code List<Map<String, Object>>}: a large cache merge can scan its input without
 * allocating a map per row, boxing a numeric cell, or hashing a column name for every access.
 * {@link #rows()} remains as a lazy compatibility view for API callers and tests; it is not the
 * aggregation path.
 */
public final class ResultFrame {

    private final List<String> columnNames;
    private final Map<String, ColumnType> columnTypes;
    private final Map<String, Integer> columnIndexes;
    private final ColumnType[] indexedTypes;
    private final Object[] columns;
    private final long[][] present;
    private final int rowCount;
    private final List<Map<String, Object>> rowsView;

    /** Compatibility constructor for boundary adapters that still receive row maps. */
    public ResultFrame(List<String> columnNames, Map<String, ColumnType> columnTypes,
                       List<Map<String, Object>> rows) {
        this(fromRows(columnNames, columnTypes, rows));
    }

    private ResultFrame(ResultFrame frame) {
        this.columnNames = frame.columnNames;
        this.columnTypes = frame.columnTypes;
        this.columnIndexes = frame.columnIndexes;
        this.indexedTypes = frame.indexedTypes;
        this.columns = frame.columns;
        this.present = frame.present;
        this.rowCount = frame.rowCount;
        this.rowsView = new FrameRowsView(this);
    }

    ResultFrame(List<String> names, Map<String, ColumnType> types, Object[] columns,
                        long[][] present, int rowCount) {
        this.columnNames = List.copyOf(names);
        this.columnTypes = Map.copyOf(new LinkedHashMap<>(types));
        Map<String, Integer> indexes = new LinkedHashMap<>();
        for (int index = 0; index < this.columnNames.size(); index++) {
            indexes.put(this.columnNames.get(index), index);
        }
        this.columnIndexes = Map.copyOf(indexes);
        this.indexedTypes = new ColumnType[this.columnNames.size()];
        for (int index = 0; index < indexedTypes.length; index++) {
            indexedTypes[index] = types.get(this.columnNames.get(index));
        }
        this.columns = columns;
        this.present = present;
        this.rowCount = rowCount;
        this.rowsView = new FrameRowsView(this);
    }

    public static ResultFrame empty() {
        return new ResultFrameBuilder().build();
    }

    public List<String> columnNames() {
        return columnNames;
    }

    public Map<String, ColumnType> columnTypes() {
        return columnTypes;
    }

    public ColumnType columnType(String name) {
        ColumnType type = columnTypes.get(name);
        if (type == null) {
            throw new IllegalArgumentException("no column named '" + name + "' in frame");
        }
        return type;
    }

    /** Returns the physical index of a named column; resolve this once before a hot row loop. */
    public int columnIndex(String name) {
        Integer index = columnIndexes.get(name);
        if (index == null) {
            throw new IllegalArgumentException("no column named '" + name + "' in frame");
        }
        return index;
    }

    public int rowCount() {
        return rowCount;
    }

    /** Declared type at a resolved physical column index. */
    public ColumnType columnTypeAt(int column) {
        return indexedTypes[column];
    }

    /** Resolves a read-only column once for an internal typed scan. No backing array is exposed. */
    public FrameColumnReader frameColumnReader(int column) {
        if (column < 0 || column >= columns.length) throw new IndexOutOfBoundsException(column);
        return new FrameColumnReader(indexedTypes[column], columns[column], present[column], rowCount);
    }

    /** Retained compatibility accessor; internal scans should use {@link #frameColumnReader(int)}. */
    public ColumnReader columnReader(int column) {
        return new ColumnReader(frameColumnReader(column));
    }

    public static final class ColumnReader {
        private final FrameColumnReader delegate;

        private ColumnReader(FrameColumnReader delegate) { this.delegate = delegate; }

        public ColumnType type() { return delegate.type(); }
        public boolean isNullAt(int row) { return delegate.isNullAt(row); }
        // The scan checks kind and nullness before reading. Array access still checks row bounds.
        public long longValue(int row) { return delegate.longValue(row); }
        public double doubleValue(int row) { return delegate.doubleValue(row); }
        public String stringValue(int row) { return delegate.stringValue(row); }
        public java.math.BigDecimal decimalValue(int row) { return delegate.decimalValue(row); }
    }

    public boolean isEmpty() {
        return rowCount == 0;
    }

    public boolean isNullAt(int row, int column) {
        checkCell(row, column);
        return !isPresent(row, column);
    }

    public long longAt(int row, int column) {
        checkType(column, ColumnType.LONG);
        checkPresent(row, column);
        return ((long[]) columns[column])[row];
    }

    public double doubleAt(int row, int column) {
        checkType(column, ColumnType.DOUBLE);
        checkPresent(row, column);
        return ((double[]) columns[column])[row];
    }

    public String stringAt(int row, int column) {
        checkType(column, ColumnType.STRING);
        checkPresent(row, column);
        return ((String[]) columns[column])[row];
    }

    /** String form without boxing; used when a numeric value is a grouping dimension. */
    public String stringValueAt(int row, int column) {
        checkCell(row, column);
        if (!isPresent(row, column)) {
            return null;
        }
        return switch (indexedTypes[column]) {
            case STRING -> ((String[]) columns[column])[row];
            case DECIMAL -> ((java.math.BigDecimal[]) columns[column])[row].toPlainString();
            case LONG -> Long.toString(((long[]) columns[column])[row]);
            case DOUBLE -> Double.toString(((double[]) columns[column])[row]);
        };
    }

    /** Nullable numeric measure with the aggregation engine's NaN-as-absent convention. */
    public double measureAt(int row, int column) {
        checkType(column, ColumnType.DOUBLE);
        checkCell(row, column);
        return isPresent(row, column) ? ((double[]) columns[column])[row] : Double.NaN;
    }

    /**
     * Lazy legacy row view. Calling this method does not materialize all rows, but each requested
     * row map is still an object-level adapter; high-throughput code must use indexed accessors.
     */
    public List<Map<String, Object>> rows() {
        return rowsView;
    }

    public Object valueAt(int row, int column) {
        checkCell(row, column);
        if (!isPresent(row, column)) {
            return null;
        }
        return switch (indexedTypes[column]) {
            case LONG -> ((long[]) columns[column])[row];
            case DOUBLE -> ((double[]) columns[column])[row];
            case STRING -> ((String[]) columns[column])[row];
            case DECIMAL -> ((java.math.BigDecimal[]) columns[column])[row];
        };
    }

    Object rowValue(int row, Object key) {
        Integer column = columnIndexes.get(key);
        return column == null ? null : valueAt(row, column);
    }

    private boolean isPresent(int row, int column) {
        long[] bitmap = present[column];
        return bitmap == null || (bitmap[row >>> 6] & (1L << (row & 63))) != 0;
    }

    private static int bitmapWords(int rows) {
        return (int) (((long) rows + 63L) >>> 6);
    }

    private void checkCell(int row, int column) {
        if (row < 0 || row >= rowCount || column < 0 || column >= columns.length) {
            throw new IndexOutOfBoundsException("cell (" + row + ", " + column + ") is outside frame");
        }
    }

    private void checkType(int column, ColumnType expected) {
        ColumnType actual = indexedTypes[column];
        if (actual != expected) {
            throw new IllegalArgumentException("column '" + columnNames.get(column) + "' is " + actual
                    + ", not " + expected);
        }
    }

    private void checkPresent(int row, int column) {
        checkCell(row, column);
        if (!isPresent(row, column)) {
            throw new IllegalStateException("column '" + columnNames.get(column) + "' is null at row " + row);
        }
    }

    private static ResultFrame fromRows(List<String> names, Map<String, ColumnType> types,
                                        List<Map<String, Object>> rows) {
        ResultFrameBuilder builder = new ResultFrameBuilder();
        for (String name : names) {
            builder.column(name, types.get(name));
        }
        for (Map<String, Object> row : rows) {
            builder.row(row);
        }
        return builder.build();
    }

    /** Typed column builder kept as a compatibility delegate for existing callers. */
    public static final class Builder {
        private final ResultFrameBuilder delegate = new ResultFrameBuilder();

        public Builder expectedRows(int rows) {
            delegate.expectedRows(rows);
            return this;
        }

        public Builder column(String name, ColumnType type) {
            delegate.column(name, type);
            return this;
        }

        public Builder row(Map<String, Object> values) {
            delegate.row(values);
            return this;
        }

        public Builder row(Object... values) {
            delegate.row(values);
            return this;
        }

        public Builder appendLong(long value) {
            delegate.appendLong(value);
            return this;
        }

        public Builder appendDouble(double value) {
            delegate.appendDouble(value);
            return this;
        }

        public Builder appendString(String value) {
            delegate.appendString(value);
            return this;
        }

        public Builder appendDecimal(java.math.BigDecimal value) {
            delegate.appendDecimal(value);
            return this;
        }

        public Builder appendNull() {
            delegate.appendNull();
            return this;
        }

        public Builder appendCell(ResultFrame source, int row, int column) {
            delegate.appendCell(source, row, column);
            return this;
        }

        public ResultFrame build() {
            return delegate.build();
        }
    }
    public static Builder builder() {
        return new Builder();
    }
}
