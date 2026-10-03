package com.cascada.spark.adapter.out.spark;

import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.application.port.out.QueryExecutorPort;
import com.cascada.spark.domain.SparkSessionConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The production {@link QueryExecutorPort}: runs a physical SQL string on a Spark 3.5.x session
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
public final class SparkDeltaQueryExecutor implements QueryExecutorPort, AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(SparkDeltaQueryExecutor.class.getName());

    /**
     * Hard ceiling on rows materialised into the engine's JVM per query. The cache path only ever
     * pulls pre-aggregated frames (group-by cardinality × buckets — thousands of rows), so a result
     * beyond this is rejected before the adapter builds the full {@link ResultFrame}. Spark may still
     * materialize the largest partition while providing {@code toLocalIterator()}.
     */
    private static final int DEFAULT_MAX_RESULT_ROWS = 1_000_000;
    private static final SessionOwnershipCoordinator SESSION_OWNERSHIP = new SessionOwnershipCoordinator();

    private final SparkSession sparkSession;
    private final boolean ownsSession;
    private final SparkResultFrameMapper resultFrameMapper;

    /** Build (or get) a SparkSession from the resolved config — the single place a session is created. */
    public SparkDeltaQueryExecutor(SparkSessionConfig config) {
        this(config, DEFAULT_MAX_RESULT_ROWS);
    }

    /** As above, with an explicit driver-side row ceiling. */
    public SparkDeltaQueryExecutor(SparkSessionConfig config, int maxResultRows) {
        this.resultFrameMapper = new SparkResultFrameMapper(maxResultRows);
        // getOrCreate may reuse a session or create one over somebody else's SparkContext.
        // Serialize our own construction so concurrent executors cannot both claim a shared context.
        SparkSession.Builder builder = SparkSession.builder()
                .appName(config.appName())
                .master(config.master());
        for (Map.Entry<String, String> property : config.sparkProperties().entrySet()) {
            builder = builder.config(property.getKey(), property.getValue());
        }
        SessionLease<SparkSession> lease = SESSION_OWNERSHIP.acquire(
                () -> org.apache.spark.SparkContext$.MODULE$.getActive().isDefined(), builder::getOrCreate);
        this.sparkSession = lease.session();
        this.ownsSession = lease.ownsContext();
    }

    /**
     * For tests / advanced wiring: wrap an already-built session (e.g. a shared one). {@link #close()}
     * will NOT stop a session passed in this way — only the caller that created it should stop it.
     */
    public SparkDeltaQueryExecutor(SparkSession sparkSession) {
        this.sparkSession = Objects.requireNonNull(sparkSession, "sparkSession");
        this.ownsSession = false;
        this.resultFrameMapper = new SparkResultFrameMapper(DEFAULT_MAX_RESULT_ROWS);
    }

    @Override
    public ResultFrame execute(String physicalSql) {
        boolean monitor = LOGGER.isLoggable(Level.FINE);
        long started = monitor ? System.nanoTime() : 0L;
        Dataset<Row> dataset = sparkSession.sql(physicalSql);
        long planned = monitor ? System.nanoTime() : 0L;
        ResultFrame result = toResultFrame(dataset);
        if (monitor) {
            long finished = System.nanoTime();
            LOGGER.fine(() -> "Spark query: setup_ms=" + (planned - started) / 1_000_000.0
                    + " execute_and_map_ms=" + (finished - planned) / 1_000_000.0
                    + " result_rows=" + result.rowCount() + " result_columns=" + result.columnNames().size());
        }
        return result;
    }

    /** Map a Spark result into a {@link ResultFrame}; visible for the cluster integration test. */
    ResultFrame toResultFrame(Dataset<Row> dataset) {
        // toLocalIterator streams through the result; Spark may materialize its largest partition.
        return resultFrameMapper.map(dataset.schema(), dataset.toLocalIterator());
    }

    @Override
    public void close() {
        if (ownsSession) {
            sparkSession.stop();
        }
    }
}
