package com.cascada.app.bootstrap;

import com.cascada.spark.adapter.out.spark.SparkDeltaQueryExecutor;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@EnabledIfSystemProperty(named = "cascada.sql.benchmark", matches = "true")
public class SparkSqlBenchmarkTest {

    private static final String TABLE = "benchmark_events";

    public static void main(String[] args) throws Exception {
        new SparkSqlBenchmarkTest().collectsUncachedSqlAgainstAnExistingDeltaSnapshot();
    }

    @Test
    void collectsUncachedSqlAgainstAnExistingDeltaSnapshot() throws Exception {
        String deltaPath = System.getenv("CASCADA_BENCHMARK_DELTA_PATH");
        assertThat(deltaPath).as("CASCADA_BENCHMARK_DELTA_PATH").isNotBlank();
        long version = Long.parseLong(System.getenv("CASCADA_BENCHMARK_DELTA_VERSION"));
        boolean nativeMode = Boolean.getBoolean("cascada.sql.benchmark.native");
        int threads = Integer.getInteger("cascada.sql.benchmark.threads", 6);
        int trials = Integer.getInteger("cascada.sql.benchmark.trials", 7);
        assertThat(threads).isPositive();
        assertThat(trials).isPositive();

        var deployment = LocalSparkFixtures.deploymentProperties(nativeMode);
        assertThat(deployment).as("Spark settings from SPARK_CONFIG_PATH").isNotEmpty();
        var configBuilder = LocalSparkFixtures.localConfig()
                .master("local[" + threads + "]")
                .appName("cascada-sql-benchmark")
                .withGroup(deployment)
                .withEnvironment(key -> "CASCADA_GLUTEN_ENABLED".equals(key)
                        ? Boolean.toString(nativeMode) : null);
        String glutenJar = System.getenv("CASCADA_BENCHMARK_GLUTEN_JAR");
        if (nativeMode) {
            assertThat(glutenJar).as("CASCADA_BENCHMARK_GLUTEN_JAR").isNotBlank();
            configBuilder.withProperty("spark.executor.extraClassPath", glutenJar);
        }
        var config = configBuilder.build();
        try (var executor = new SparkDeltaQueryExecutor(config)) {
            SparkSession spark = SparkSession.active();
            spark.sparkContext().setLogLevel("WARN");
            String plugins = spark.sparkContext().getConf().get("spark.plugins", "");
            assertThat(plugins.contains("org.apache.gluten.GlutenPlugin")).isEqualTo(nativeMode);

            spark.read().option("versionAsOf", version).format("delta").load(deltaPath)
                    .createOrReplaceTempView(TABLE);
            assertThat(spark.catalog().isCached(TABLE)).isFalse();

            if (nativeMode) {
                var origin = Class.forName("org.apache.gluten.GlutenPlugin")
                        .getProtectionDomain().getCodeSource().getLocation().toURI();
                assertThat(Path.of(origin).toRealPath()).isEqualTo(Path.of(glutenJar).toRealPath());
            } else {
                assertThatThrownBy(() -> Class.forName("org.apache.gluten.GlutenPlugin"))
                        .as("vanilla Spark classpath").isInstanceOf(ClassNotFoundException.class);
            }
            var json = new ObjectMapper();
            for (var query : queries().entrySet()) {
                String sql = query.getValue();
                String digest = LocalSparkFixtures.digest(spark.sql(sql).collectAsList());
                double[] millis = new double[trials];
                String schema = null;
                List<String> operators = List.of();
                for (int trial = 0; trial < trials; trial++) {
                    long start = System.nanoTime();
                    var dataset = spark.sql(sql);
                    List<Row> rows = dataset.collectAsList();
                    millis[trial] = (System.nanoTime() - start) / 1_000_000.0;

                    assertThat(LocalSparkFixtures.digest(rows)).as(query.getKey() + " repeat result").isEqualTo(digest);
                    schema = dataset.schema().json();
                    operators = LocalSparkFixtures.executedPlanClasses(dataset.queryExecution().executedPlan());
                    assertThat(operators).noneMatch(name -> name.contains("InMemoryTableScan"));
                    if (!nativeMode) {
                        assertThat(operators).noneMatch(name -> name.startsWith("org.apache.gluten."));
                    }
                }
                Arrays.sort(millis);
                long nativeScans = operators.stream().filter(name -> name.endsWith("DeltaScanTransformer")).count();
                if (nativeMode) {
                    assertThat(nativeScans).as(query.getKey() + " native Delta scans").isPositive();
                }
                System.out.println("SQL_BENCHMARK " + json.writeValueAsString(Map.of(
                        "case", query.getKey(), "mode", nativeMode ? "gluten" : "vanilla",
                        "digest", digest, "schema", schema, "nativeDeltaScans", nativeScans,
                        "medianMs", (millis[(trials - 1) / 2] + millis[trials / 2]) / 2,
                        "p95Ms", millis[(int) Math.ceil(trials * 0.95) - 1])));
            }
        }
    }

    private Map<String, String> queries() {
        var queries = new LinkedHashMap<String, String>();
        queries.put("scalar", """
                SELECT COUNT(*) AS n, SUM(amount) AS total, MIN(ts) AS first_ts FROM benchmark_events
                """);
        queries.put("groups", """
                SELECT region, device, SUM(amount) AS total, COUNT(amount) AS present,
                       AVG(CAST(amount AS DECIMAL(20,2))) AS mean
                FROM benchmark_events
                GROUP BY region, device ORDER BY region NULLS FIRST, device NULLS FIRST
                """);
        queries.put("distinct", """
                SELECT region, COUNT(DISTINCT amount) AS n
                FROM benchmark_events
                GROUP BY region ORDER BY region NULLS FIRST
                """);
        queries.put("cte-join", """
                WITH totals AS (SELECT region, SUM(amount) AS total FROM benchmark_events GROUP BY region),
                counts AS (SELECT region, COUNT(*) AS n FROM benchmark_events WHERE amount > 0 GROUP BY region)
                SELECT t.region, t.total, c.n
                FROM totals t LEFT JOIN counts c ON t.region <=> c.region
                ORDER BY t.region NULLS FIRST
                """);
        queries.put("window", """
                WITH totals AS (
                    SELECT region, device, SUM(amount) AS total
                    FROM benchmark_events GROUP BY region, device
                ), ranked AS (
                    SELECT region, device, total,
                           ROW_NUMBER() OVER (PARTITION BY region ORDER BY total DESC, device NULLS FIRST) AS pos
                    FROM totals
                )
                SELECT region, device, total, pos FROM ranked WHERE pos <= 3 ORDER BY region NULLS FIRST, pos
                """);
        return queries;
    }
}
