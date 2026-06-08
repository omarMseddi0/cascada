package com.cascada.cache.domain.frame;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A small, framework-free columnar result table — the unit the cache stores, fetches, and merges.
 * It is the portable stand-in for a pandas/Tablesaw {@code DataFrame} (the substrate the merge math
 * runs on), kept dependency-free so the correctness gate can run without a dataframe library.
 *
 * <p>The schema (ordered column names + their {@link ColumnType}) travels with the frame so the
 * Arrow/portable serializer can round-trip it losslessly. Internally the data is row-oriented, which
 * is what the merge strategies consume.
 */
public final class ResultFrame {

    private final List<String> columnNames;
    private final Map<String, ColumnType> columnTypes;
    private final List<Map<String, Object>> rows;

    public ResultFrame(List<String> columnNames, Map<String, ColumnType> columnTypes,
                       List<Map<String, Object>> rows) {
        this.columnNames = List.copyOf(columnNames);
        this.columnTypes = new LinkedHashMap<>(columnTypes);
        this.rows = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            this.rows.add(new LinkedHashMap<>(row));
        }
    }

    public static ResultFrame empty() {
        return new ResultFrame(List.of(), Map.of(), List.of());
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

    public List<Map<String, Object>> rows() {
        return List.copyOf(rows);
    }

    public int rowCount() {
        return rows.size();
    }

    public boolean isEmpty() {
        return rows.isEmpty();
    }

    /** A builder that enforces the schema so callers cannot produce a ragged frame. */
    public static final class Builder {
        private final List<String> columnNames = new ArrayList<>();
        private final Map<String, ColumnType> columnTypes = new LinkedHashMap<>();
        private final List<Map<String, Object>> rows = new ArrayList<>();

        public Builder column(String name, ColumnType type) {
            columnNames.add(name);
            columnTypes.put(name, type);
            return this;
        }

        public Builder row(Map<String, Object> values) {
            Map<String, Object> ordered = new LinkedHashMap<>();
            for (String column : columnNames) {
                ordered.put(column, values.get(column));
            }
            rows.add(ordered);
            return this;
        }

        public ResultFrame build() {
            return new ResultFrame(columnNames, columnTypes, rows);
        }
    }

    public static Builder builder() {
        return new Builder();
    }
}
