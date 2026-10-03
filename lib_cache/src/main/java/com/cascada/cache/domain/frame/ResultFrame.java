package com.cascada.cache.domain.frame;

import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        this.rowsView = new RowsView();
    }

    private ResultFrame(List<String> names, Map<String, ColumnType> types, Object[] columns,
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
        this.rowsView = new RowsView();
    }

    public static ResultFrame empty() {
        return new Builder().build();
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
    public ColumnReader columnReader(int column) {
        if (column < 0 || column >= columns.length) throw new IndexOutOfBoundsException(column);
        return new ColumnReader(indexedTypes[column], columns[column], present[column], rowCount);
    }

    public static final class ColumnReader {
        private final ColumnType type;
        private final long[] longs;
        private final double[] doubles;
        private final String[] strings;
        private final java.math.BigDecimal[] decimals;
        private final long[] validity;
        private final int length;

        private ColumnReader(ColumnType type, Object values, long[] validity, int length) {
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
        Builder builder = builder();
        for (String name : names) {
            builder.column(name, types.get(name));
        }
        for (Map<String, Object> row : rows) {
            builder.row(row);
        }
        return builder.build();
    }

    private final class RowsView extends AbstractList<Map<String, Object>> {
        @Override
        public Map<String, Object> get(int row) {
            if (row < 0 || row >= rowCount) {
                throw new IndexOutOfBoundsException(row);
            }
            return new RowView(row);
        }

        @Override
        public int size() {
            return rowCount;
        }
    }

    private final class RowView extends AbstractMap<String, Object> {
        private final int row;

        private RowView(int row) {
            this.row = row;
        }

        @Override
        public Object get(Object key) {
            Integer column = columnIndexes.get(key);
            return column == null ? null : valueAt(row, column);
        }

        @Override
        public boolean containsKey(Object key) {
            return columnIndexes.containsKey(key);
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            LinkedHashMap<String, Object> values = new LinkedHashMap<>(columnNames.size());
            for (int column = 0; column < columnNames.size(); column++) {
                values.put(columnNames.get(column), valueAt(row, column));
            }
            return values.entrySet();
        }
    }

    /** Typed column builder. Use append methods in internal/vectorized paths to avoid row maps. */
    public static final class Builder {
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
        public Builder expectedRows(int rows) {
            if (rows < 0) throw new IllegalArgumentException("expected rows must not be negative");
            if (columns != null) throw new IllegalStateException("set expected rows before appending cells");
            expectedRows = Math.max(16, rows);
            return this;
        }

        public Builder column(String name, ColumnType type) {
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
        public Builder row(Map<String, Object> values) {
            for (String name : names) {
                appendObject(values.get(name));
            }
            return this;
        }

        /** Convenience typed row path; values must match the declared schema order. */
        public Builder row(Object... values) {
            if (values.length != names.size()) {
                throw new IllegalArgumentException("expected " + names.size() + " values but got " + values.length);
            }
            for (Object value : values) {
                appendObject(value);
            }
            return this;
        }

        public Builder appendLong(long value) {
            requireType(ColumnType.LONG);
            ensureCapacityForCell();
            ((long[]) columns[nextColumn])[rowCount] = value;
            advance();
            return this;
        }

        public Builder appendDouble(double value) {
            requireType(ColumnType.DOUBLE);
            ensureCapacityForCell();
            ((double[]) columns[nextColumn])[rowCount] = value;
            advance();
            return this;
        }

        public Builder appendString(String value) {
            requireType(ColumnType.STRING);
            ensureCapacityForCell();
            ((String[]) columns[nextColumn])[rowCount] = value;
            if (value == null) markNull();
            advance();
            return this;
        }

        public Builder appendDecimal(java.math.BigDecimal value) {
            requireType(ColumnType.DECIMAL);
            ensureCapacityForCell();
            ((java.math.BigDecimal[]) columns[nextColumn])[rowCount] = value;
            if (value == null) markNull();
            advance();
            return this;
        }

        public Builder appendNull() {
            ensureCapacityForCell();
            markNull();
            advance();
            return this;
        }

        /** Copies a typed cell without creating a row map or boxing primitive numeric values. */
        public Builder appendCell(ResultFrame source, int row, int column) {
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
                trimmedColumns[column] = copyColumn(columns[column], types.get(names.get(column)), rowCount);
                trimmedPresent[column] = present[column] == null ? null
                        : java.util.Arrays.copyOf(present[column], bitmapWords(rowCount));
            }
            return new ResultFrame(names, types, trimmedColumns, trimmedPresent, rowCount);
        }

        private void appendObject(Object value) {
            if (value == null) {
                appendNull();
                return;
            }
            initialize();
            switch (indexedTypes[nextColumn]) {
                case LONG -> appendLong(((Number) value).longValue());
                case DOUBLE -> appendDouble(((Number) value).doubleValue());
                case STRING -> appendString(value.toString());
                case DECIMAL -> appendDecimal((java.math.BigDecimal) value);
            }
        }

        private void requireType(ColumnType expected) {
            initialize();
            if (nextColumn >= names.size() || indexedTypes[nextColumn] != expected) {
                String actual = nextColumn >= names.size() ? "no column" : indexedTypes[nextColumn].toString();
                throw new IllegalStateException("expected next column to be " + expected + " but was " + actual);
            }
        }

        private void ensureCapacityForCell() {
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

        private void initialize() {
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

        private void advance() {
            nextColumn++;
            if (nextColumn == names.size()) {
                nextColumn = 0;
                rowCount++;
            }
        }

        private void markNull() {
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

        private static Object newColumn(ColumnType type, int length) {
            return switch (type) {
                case LONG -> new long[length];
                case DOUBLE -> new double[length];
                case STRING -> new String[length];
                case DECIMAL -> new java.math.BigDecimal[length];
            };
        }

        private static Object growColumn(Object source, ColumnType type, int length) {
            return switch (type) {
                case LONG -> java.util.Arrays.copyOf((long[]) source, length);
                case DOUBLE -> java.util.Arrays.copyOf((double[]) source, length);
                case STRING -> java.util.Arrays.copyOf((String[]) source, length);
                case DECIMAL -> java.util.Arrays.copyOf((java.math.BigDecimal[]) source, length);
            };
        }

        private static Object copyColumn(Object source, ColumnType type, int length) {
            return switch (type) {
                case LONG -> java.util.Arrays.copyOf((long[]) source, length);
                case DOUBLE -> java.util.Arrays.copyOf((double[]) source, length);
                case STRING -> java.util.Arrays.copyOf((String[]) source, length);
                case DECIMAL -> java.util.Arrays.copyOf((java.math.BigDecimal[]) source, length);
            };
        }
    }

    public static Builder builder() {
        return new Builder();
    }
}
