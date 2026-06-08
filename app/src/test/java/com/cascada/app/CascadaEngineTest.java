package com.cascada.app;

import com.cascada.cache.adapter.backend.InMemoryBlobCacheBackendAdapter;
import com.cascada.cache.adapter.serialization.ArrowResultFrameSerializer;
import com.cascada.cache.application.ExecuteCachedQueryUseCase;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.port.SparkQueryExecutorPort;
import com.cascada.sql.translate.RegisteredTable;
import com.cascada.sql.translate.TableCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end wiring test for the composition root: a logical SQL string flows through
 * translate → canonicalize → safety → cache engine → executor and produces a result. Proves the four
 * libraries are wired together correctly without a Spark cluster.
 *
 * <p>The executor here is a tiny in-test fake (the {@code DirectComputeOracle} pattern) rather than a
 * real engine, because what this test verifies is the <em>wiring</em>: that the logical query is
 * translated to physical Delta SQL (table → {@code delta.`/path`}, time bucketed) and reaches the
 * executor. The fake also lets us assert the translation output precisely. DuckDB/Spark executor
 * correctness is covered by their own adapter tests; the real Delta read is a cluster integration test.
 */
class CascadaEngineTest {

    /** Captures the physical SQL the engine produced and returns a canned aggregated frame. */
    private static final class CapturingExecutor implements SparkQueryExecutorPort {
        private String lastPhysicalSql;

        @Override
        public ResultFrame execute(String physicalSql) {
            this.lastPhysicalSql = physicalSql;
            return ResultFrame.builder()
                    .column("appName", ColumnType.STRING)
                    .column("total_bytes", ColumnType.DOUBLE)
                    .row(Map.of("appName", "netflix", "total_bytes", 150.0))
                    .row(Map.of("appName", "youtube", "total_bytes", 20.0))
                    .build();
        }
    }

    private CapturingExecutor executor;
    private CascadaEngine engine;

    @BeforeEach
    void wireEngine() {
        executor = new CapturingExecutor();
        TableCatalog catalog = new TableCatalog().register(RegisteredTable.of(
                "traffic", "/tmp/traffic",
                Map.of("ts", "ts", "appName", "appName", "bytes", "bytes"), "ts"));

        engine = CascadaEngine.builder()
                .executor(executor)
                .cacheBackend(new InMemoryBlobCacheBackendAdapter(new ArrowResultFrameSerializer()))
                .tableCatalog(catalog)
                .build();
    }

    @Test
    void translatesLogicalSqlToPhysicalDeltaSqlAndReachesTheExecutor() {
        String logicalSql = "SELECT appName, SUM(bytes) AS total_bytes FROM traffic "
                + "WHERE ts >= 0 AND ts <= 9999 GROUP BY appName ORDER BY appName";

        engine.query(logicalSql);

        // The logical table name was resolved to its physical Delta path.
        assertThat(executor.lastPhysicalSql).contains("delta.`/tmp/traffic`");
        assertThat(executor.lastPhysicalSql).containsIgnoringCase("GROUP BY appName");
    }

    @Test
    void returnsTheExecutorResultThroughTheFullPipeline() {
        String logicalSql = "SELECT appName, SUM(bytes) AS total_bytes FROM traffic "
                + "WHERE ts >= 0 AND ts <= 9999 GROUP BY appName ORDER BY appName";

        ExecuteCachedQueryUseCase.Result result = engine.query(logicalSql);
        ResultFrame frame = result.frame();

        assertThat(frame.columnNames()).contains("appName", "total_bytes");
        Map<String, Double> totalByApp = new HashMap<>();
        for (Map<String, Object> row : frame.rows()) {
            totalByApp.put((String) row.get("appName"), ((Number) row.get("total_bytes")).doubleValue());
        }
        assertThat(totalByApp.get("netflix")).isEqualTo(150.0);
        assertThat(totalByApp.get("youtube")).isEqualTo(20.0);
    }
}
