package com.cascada.spark.adapter.out.spark;

import com.cascada.cache.application.execution.QueryCancellation;
import com.cascada.cache.application.execution.QueryExecutionScope;
import com.cascada.cache.application.port.out.QueryExecutorPort;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.spark.domain.SparkSessionConfig;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/** Runs physical SQL on the shared Spark/Delta session and exposes the cache's primitive frame contract.
 * Spark 4.1.1 and its native runtime are supplied by the Java 21 driver/executor image.
 */
public final class SparkDeltaQueryExecutor implements QueryExecutorPort, AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(SparkDeltaQueryExecutor.class.getName());

    private static final SessionOwnershipCoordinator SESSION_OWNERSHIP = new SessionOwnershipCoordinator();

    private final SparkSession sparkSession;
    private final boolean ownsSession;
    private final SparkResultFrameMapper resultFrameMapper;

    /** Build (or get) a SparkSession from the resolved config — the single place a session is created. */
    public SparkDeltaQueryExecutor(SparkSessionConfig config) {
        GlutenRuntimeValidator.validate(config);
        this.resultFrameMapper = new SparkResultFrameMapper();
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
        this.resultFrameMapper = new SparkResultFrameMapper();
    }

    @Override
    public ResultFrame execute(String physicalSql) {
        return QueryExecutionScope.current().map(cancellation -> executeCancellable(physicalSql, cancellation))
                .orElseGet(() -> executeSql(physicalSql));
    }

    private ResultFrame executeCancellable(String physicalSql, QueryCancellation cancellation) {
        cancellation.requireActive();
        var context = sparkSession.sparkContext();
        String previousGroup = context.getLocalProperty("spark.jobGroup.id");
        String previousDescription = context.getLocalProperty("spark.job.description");
        String previousInterruptPolicy = context.getLocalProperty("spark.job.interruptOnCancel");
        context.setJobGroup(cancellation.executionId(), "Cascada query", true);
        try (var registration = cancellation.onCancel(() -> context.cancelJobGroupAndFutureJobs(cancellation.executionId()))) {
            cancellation.requireActive();
            return executeSql(physicalSql);
        } finally {
            context.setLocalProperty("spark.jobGroup.id", previousGroup);
            context.setLocalProperty("spark.job.description", previousDescription);
            context.setLocalProperty("spark.job.interruptOnCancel", previousInterruptPolicy);
        }
    }

    private ResultFrame executeSql(String physicalSql) {
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
