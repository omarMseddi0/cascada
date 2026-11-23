package com.cascada.spark.adapter.out.spark;

import com.cascada.spark.adapter.out.model.spark.SessionLease;
import com.cascada.cache.application.execution.QueryCancellation;
import com.cascada.cache.application.execution.QueryExecutionScope;
import com.cascada.cache.application.port.out.PinnedQueryExecutorPort;
import com.cascada.cache.domain.model.cube.ExactCubeSource;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.spark.domain.SparkSessionConfig;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.execution.SQLExecution;
import scala.Option;

/** Runs physical SQL on the shared Spark/Delta session and exposes the cache's primitive frame contract.
 * Spark 4.1.1 and its native runtime are supplied by the Java 21 driver/executor image.
 */
public final class SparkDeltaQueryExecutor implements PinnedQueryExecutorPort, AutoCloseable {

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
    public Optional<ExactCubeSource> bind(String registeredPath, String accessScope) {
        QueryCancellation.requireCurrentActive();
        var log = DeltaLog.forTable(sparkSession, registeredPath);
        Snapshot snapshot = log.update(false, Option.empty(), Option.empty());
        if (snapshot.version() < 0) return Optional.empty();
        log.protocolRead(snapshot.protocol());
        Map<String, String> types = new HashMap<>();
        var foldedNames = new HashSet<String>();
        for (var field : snapshot.schema().fields()) {
            if (!foldedNames.add(field.name().toLowerCase(Locale.ROOT))) return Optional.empty();
            String type = field.dataType().catalogString();
            if (type.equals("string") && !field.dataType().equals(DataTypes.StringType)) {
                type = "collated string";
            }
            types.put(field.name(), type);
        }
        return Optional.of(new ExactCubeSource(registeredPath, snapshot.metadata().id(), snapshot.version(),
                snapshot.schema().json(), semantics(), accessScope, types));
    }

    private Snapshot snapshot(String path) {
        return DeltaLog.forTable(sparkSession, path).update(false, Option.empty(), Option.empty());
    }

    private String semantics() {
        var conf = sparkSession.conf();
        return String.join("/", sparkSession.version(), conf.get("spark.sql.ansi.enabled"),
                conf.get("spark.sql.caseSensitive"), conf.get("spark.sql.session.timeZone"),
                conf.get("spark.sql.parser.escapedStringLiterals"),
                conf.get("spark.sql.groupByAliases"), conf.get("spark.sql.orderByOrdinal"));
    }

    @Override
    public ResultFrame executePinned(ExactCubeSource binding, String physicalSql) {
        ensureSameTable(binding);
        ResultFrame answer = execute(physicalSql);
        ensureSameTable(binding);
        return answer;
    }

    private void ensureSameTable(ExactCubeSource binding) {
        QueryCancellation.requireCurrentActive();
        Snapshot current = snapshot(binding.path());
        if (current.version() < binding.version() || !current.metadata().id().equals(binding.tableId())
                || !semantics().equals(binding.semantics())) {
            throw new IllegalStateException("Delta table or Spark semantics changed after admission");
        }
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
        // One job over every partition in parallel (toLocalIterator ran one job per partition in sequence),
        // collected as internal rows so no external Row object or boxed cell is built per value. The
        // execution id is what Dataset.collect() opens too: it keeps the query visible in the SQL UI/metrics.
        QueryExecution queryExecution = ((org.apache.spark.sql.classic.Dataset<Row>) dataset).queryExecution();
        InternalRow[] rows = SQLExecution.withNewExecutionId(queryExecution, Option.apply("cascada-collect"),
                () -> queryExecution.executedPlan().executeCollect());
        return resultFrameMapper.map(dataset.schema(), rows);
    }

    @Override
    public void close() {
        if (ownsSession) {
            sparkSession.stop();
        }
    }
}
