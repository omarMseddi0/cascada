package com.cascada.spark.execution;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.port.SparkQueryExecutorPort;
import com.cascada.spark.config.SparkSessionConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The production {@link SparkQueryExecutorPort}: runs a physical SQL string on a Spark 3.5.x session
 * with Delta Lake enabled, and maps the resulting {@code Dataset<Row>} into the cache's framework-free
 * {@link ResultFrame}. Ported from {@code SparkSessionManager.get_session} + the query path of
 * {@code query_engine.py} (the {@code SparkQueryEngine} that calls {@code spark.sql(sql)}).
 *
 * <p><b>Local and cluster are the same code.</b> The {@link SparkSessionConfig} decides the master
 * ({@code local[*]} or {@code k8s://...}) and the {@code spark.*} properties; this class builds the
 * session and executes against it identically either way. A {@code SELECT ... FROM delta.`/path`} reads
 * a Delta table directly because the config always installs the Delta extensions + catalog.
 *
 * <p>Spark 3.5.6 officially targets Java 8/11/17. In production it runs in the pinned Spark+Gluten+Velox
 * image, so the Spark runtime is {@code provided}. This adapter therefore compiles here but its live
 * session is exercised in the cluster image / a JDK-17 integration job, not the JDK-22 unit build —
 * which is why the config builder (pure) carries the unit-test coverage and this class is a thin,
 * well-typed bridge.
 */
public final class SparkDeltaQueryExecutor implements SparkQueryExecutorPort, AutoCloseable {

    /**
     * Hard ceiling on rows materialised into the engine's JVM per query. The cache path only ever
     * pulls pre-aggregated frames (group-by cardinality × buckets — thousands of rows), so a result
     * beyond this is a runaway raw scan that would otherwise OOM the driver via collect.
     */
    private static final int DEFAULT_MAX_RESULT_ROWS = 1_000_000;

    private final SparkSession sparkSession;
    private final boolean ownsSession;
    private final int maxResultRows;

    /** Build (or get) a SparkSession from the resolved config — the single place a session is created. */
    public SparkDeltaQueryExecutor(SparkSessionConfig config) {
        this(config, DEFAULT_MAX_RESULT_ROWS);
    }

    /** As above, with an explicit driver-side row ceiling. */
    public SparkDeltaQueryExecutor(SparkSessionConfig config, int maxResultRows) {
        if (maxResultRows <= 0) {
            throw new IllegalArgumentException("maxResultRows must be > 0, but was: " + maxResultRows);
        }
        SparkSession.Builder builder = SparkSession.builder()
                .appName(config.appName())
                .master(config.master());
        for (Map.Entry<String, String> property : config.sparkProperties().entrySet()) {
            builder = builder.config(property.getKey(), property.getValue());
        }
        this.sparkSession = builder.getOrCreate();
        this.ownsSession = true;
        this.maxResultRows = maxResultRows;
    }

    /**
     * For tests / advanced wiring: wrap an already-built session (e.g. a shared one). {@link #close()}
     * will NOT stop a session passed in this way — only the caller that created it should stop it.
     */
    public SparkDeltaQueryExecutor(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
        this.ownsSession = false;
        this.maxResultRows = DEFAULT_MAX_RESULT_ROWS;
    }

    @Override
    public ResultFrame execute(String physicalSql) {
        Dataset<Row> dataset = sparkSession.sql(physicalSql);
        return toResultFrame(dataset);
    }

    /** Map a Spark result into a {@link ResultFrame}; visible for the cluster integration test. */
    ResultFrame toResultFrame(Dataset<Row> dataset) {
        StructType schema = dataset.schema();
        StructField[] fields = schema.fields();

        ResultFrame.Builder builder = ResultFrame.builder();
        ColumnType[] columnTypes = new ColumnType[fields.length];
        for (int index = 0; index < fields.length; index++) {
            ColumnType type = mapSparkType(fields[index].dataType());
            columnTypes[index] = type;
            builder.column(fields[index].name(), type);
        }

        // toLocalIterator streams partitions one at a time instead of materialising the whole
        // result in the driver at once (collectAsList), and the row ceiling turns a runaway raw
        // scan into a clear error instead of an OOM-killed engine.
        java.util.Iterator<Row> rows = dataset.toLocalIterator();
        int rowCount = 0;
        while (rows.hasNext()) {
            if (rowCount >= maxResultRows) {
                throw new IllegalStateException("query result exceeds the " + maxResultRows
                        + "-row driver ceiling; add a LIMIT or aggregate further");
            }
            Row row = rows.next();
            Map<String, Object> values = new LinkedHashMap<>();
            for (int index = 0; index < fields.length; index++) {
                values.put(fields[index].name(), readCell(row, index, columnTypes[index]));
            }
            builder.row(values);
            rowCount++;
        }
        return builder.build();
    }

    private Object readCell(Row row, int index, ColumnType type) {
        if (row.isNullAt(index)) {
            return null;
        }
        return switch (type) {
            case LONG -> ((Number) row.get(index)).longValue();
            case DOUBLE -> ((Number) row.get(index)).doubleValue();
            case STRING -> String.valueOf(row.get(index));
        };
    }

    private ColumnType mapSparkType(DataType dataType) {
        if (dataType.equals(DataTypes.ByteType) || dataType.equals(DataTypes.ShortType)
                || dataType.equals(DataTypes.IntegerType) || dataType.equals(DataTypes.LongType)) {
            return ColumnType.LONG;
        }
        if (dataType.equals(DataTypes.FloatType) || dataType.equals(DataTypes.DoubleType)
                || dataType instanceof org.apache.spark.sql.types.DecimalType) {
            return ColumnType.DOUBLE;
        }
        return ColumnType.STRING;
    }

    @Override
    public void close() {
        if (ownsSession) {
            sparkSession.stop();
        }
    }
}
