package com.cascada.cache.adapter.execution;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the in-process DuckDB executor satisfies {@link com.cascada.cache.domain.port.SparkQueryExecutorPort}:
 * it runs real physical SQL (including the GROUP BY aggregation the cache's gap-fill produces),
 * maps DuckDB's column types onto the cache's three-type domain, and surfaces nulls — so it is a true
 * drop-in for the Spark adapter at the port boundary.
 */
class DuckDbInProcessQueryExecutorTest {

    private DuckDbInProcessQueryExecutor executor;

    @BeforeEach
    void openExecutorAndSeedData() {
        executor = new DuckDbInProcessQueryExecutor();
        // Seed a tiny table via the DDL/DML helper (the read path stays query-only).
        executor.executeUpdate("CREATE TABLE traffic (appName VARCHAR, latency DOUBLE)");
        executor.executeUpdate("INSERT INTO traffic VALUES "
                + "('netflix', 100.0), ('netflix', 50.0), ('youtube', 20.0), ('hbo', NULL)");
    }

    @AfterEach
    void closeExecutor() {
        executor.close();
    }

    @Test
    void runsAGroupByAggregationAndReturnsACorrectResultFrame() {
        ResultFrame frame = executor.execute(
                "SELECT appName, SUM(latency) AS sum_latency, COUNT(latency) AS count_latency "
                        + "FROM traffic GROUP BY appName ORDER BY appName");

        assertThat(frame.columnNames()).containsExactly("appName", "sum_latency", "count_latency");
        assertThat(frame.columnType("appName")).isEqualTo(ColumnType.STRING);
        assertThat(frame.columnType("sum_latency")).isEqualTo(ColumnType.DOUBLE);
        assertThat(frame.columnType("count_latency")).isEqualTo(ColumnType.LONG);

        Map<String, Double> sumByApp = new HashMap<>();
        Map<String, Long> countByApp = new HashMap<>();
        for (Map<String, Object> row : frame.rows()) {
            Object sum = row.get("sum_latency");
            if (sum != null) {
                sumByApp.put((String) row.get("appName"), ((Number) sum).doubleValue());
            }
            countByApp.put((String) row.get("appName"), ((Number) row.get("count_latency")).longValue());
        }
        assertThat(sumByApp.get("netflix")).isEqualTo(150.0);
        assertThat(sumByApp.get("youtube")).isEqualTo(20.0);
        assertThat(countByApp.get("netflix")).isEqualTo(2L);
        // hbo's only latency is NULL -> COUNT(latency) = 0, SUM(latency) = NULL.
        assertThat(countByApp.get("hbo")).isEqualTo(0L);
    }

    @Test
    void surfacesSqlNullsAsJavaNulls() {
        ResultFrame frame = executor.execute(
                "SELECT appName, SUM(latency) AS sum_latency FROM traffic "
                        + "WHERE appName = 'hbo' GROUP BY appName");
        assertThat(frame.rowCount()).isEqualTo(1);
        assertThat(frame.rows().get(0).get("sum_latency")).isNull();
    }

    @Test
    void returnsAnEmptyFrameForAZeroRowResult() {
        ResultFrame frame = executor.execute("SELECT appName FROM traffic WHERE appName = 'nobody'");
        assertThat(frame.rowCount()).isZero();
        assertThat(frame.columnNames()).containsExactly("appName");
    }
}
