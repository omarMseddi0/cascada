package com.cascada.cache.adapter.execution;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.port.SparkQueryExecutorPort;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An in-process OLAP executor behind {@link SparkQueryExecutorPort}, backed by **DuckDB** (embedded,
 * no cluster). It is the fast local substitute for the Spark + Gluten/Velox adapter: the engine hands
 * it a physical SQL string and gets a {@link ResultFrame} back, exactly like the Spark port, so the
 * cache's gap-fill and roll-up paths run unchanged — only the executor swaps.
 *
 * <p>Why DuckDB behind this port: it is columnar and vectorised (so the aggregation that row-oriented
 * Java is slow at — see the 1BRC Java-dataframe benchmark — happens in native vectorised code), and it
 * speaks Arrow natively, so a cached bucket serialized by {@link com.cascada.cache.adapter.serialization.ArrowResultFrameSerializer}
 * can be handed to DuckDB zero-copy. For "is this subsuming cache entry coarser than my query? roll it
 * up" or "fill this small Spark gap locally", DuckDB answers in microseconds without a JVM-heap
 * group-by and without a Spark round-trip. The port boundary means production can still inject the real
 * Spark adapter for cluster-scale gaps; this one is for in-process speed (plan §8.16 tiering, the
 * "swap DuckDB in-process behind the same port" path).
 *
 * <p>Type mapping mirrors the cache's three-type domain: SQL integral types → {@link ColumnType#LONG},
 * floating/decimal → {@link ColumnType#DOUBLE}, everything else → {@link ColumnType#STRING}. The
 * connection is opened once and reused; {@link #close()} releases it.
 */
public final class DuckDbInProcessQueryExecutor implements SparkQueryExecutorPort, AutoCloseable {

    private final Connection connection;

    /** Opens an in-memory DuckDB instance (no file, nothing to clean up). */
    public DuckDbInProcessQueryExecutor() {
        this("jdbc:duckdb:");
    }

    /**
     * @param jdbcUrl a DuckDB JDBC URL — {@code jdbc:duckdb:} for in-memory, or
     *     {@code jdbc:duckdb:/path/to/file.db} for a persistent local store (an NVMe warm tier).
     */
    public DuckDbInProcessQueryExecutor(String jdbcUrl) {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
            this.connection = DriverManager.getConnection(jdbcUrl);
        } catch (ClassNotFoundException | SQLException failure) {
            throw new IllegalStateException("could not open an in-process DuckDB connection", failure);
        }
    }

    @Override
    public ResultFrame execute(String physicalSql) {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(physicalSql)) {
            return toResultFrame(resultSet);
        } catch (SQLException failure) {
            throw new IllegalStateException("DuckDB failed to execute physical SQL: " + physicalSql, failure);
        }
    }

    /**
     * Runs a DDL/DML statement (CREATE/INSERT/COPY) that returns no result set — used to register or
     * load a local table into this DuckDB instance (the warm-tier bootstrap). The read path
     * ({@link #execute}) stays query-only, matching the {@link SparkQueryExecutorPort} contract.
     *
     * @return the affected-row count DuckDB reports (0 for DDL)
     */
    public int executeUpdate(String ddlOrDml) {
        try (Statement statement = connection.createStatement()) {
            return statement.executeUpdate(ddlOrDml);
        } catch (SQLException failure) {
            throw new IllegalStateException("DuckDB failed to execute statement: " + ddlOrDml, failure);
        }
    }

    private ResultFrame toResultFrame(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        ResultFrame.Builder builder = ResultFrame.builder();
        ColumnType[] columnTypes = new ColumnType[columnCount + 1];
        String[] columnLabels = new String[columnCount + 1];
        for (int column = 1; column <= columnCount; column++) {
            ColumnType type = mapSqlType(metaData.getColumnType(column));
            String label = metaData.getColumnLabel(column);
            columnTypes[column] = type;
            columnLabels[column] = label;
            builder.column(label, type);
        }

        while (resultSet.next()) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int column = 1; column <= columnCount; column++) {
                row.put(columnLabels[column], readCell(resultSet, column, columnTypes[column]));
            }
            builder.row(row);
        }
        return builder.build();
    }

    private Object readCell(ResultSet resultSet, int column, ColumnType type) throws SQLException {
        Object raw = resultSet.getObject(column);
        if (raw == null || resultSet.wasNull()) {
            return null;
        }
        return switch (type) {
            case LONG -> resultSet.getLong(column);
            case DOUBLE -> resultSet.getDouble(column);
            case STRING -> resultSet.getString(column);
        };
    }

    private ColumnType mapSqlType(int sqlType) {
        return switch (sqlType) {
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT -> ColumnType.LONG;
            case Types.REAL, Types.FLOAT, Types.DOUBLE, Types.DECIMAL, Types.NUMERIC -> ColumnType.DOUBLE;
            default -> ColumnType.STRING;
        };
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException ignored) {
            // closing a connection that already failed is not actionable
        }
    }
}
