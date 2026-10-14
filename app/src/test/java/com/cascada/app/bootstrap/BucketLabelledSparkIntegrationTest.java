package com.cascada.app.bootstrap;

import com.cascada.cache.adapter.out.cache.InMemoryBlobCacheBackendAdapter;
import com.cascada.cache.adapter.out.serialization.ArrowResultFrameSerializer;
import com.cascada.cache.adapter.out.sketch.DataSketchesDistinctSketchAdapter;
import com.cascada.cache.application.config.CacheExecutionConfiguration;
import com.cascada.cache.application.port.out.QueryExecutorPort;
import com.cascada.cache.application.service.CacheExecutionEngine;
import com.cascada.cache.application.service.hashing.QueryHashGenerator;
import com.cascada.cache.domain.admin.CacheScope;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.key.CacheKeyFactory;
import com.cascada.cache.domain.query.CanonicalQueryObject;
import com.cascada.identity.domain.QueryHash;
import com.cascada.spark.adapter.out.spark.SparkDeltaQueryExecutor;
import com.cascada.sql.adapter.out.calcite.CalciteCanonicalObjectFactory;
import com.cascada.sql.adapter.out.calcite.GapQueryRewriterAdapter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/** Bucket-labelled cold fills, partial refills and HLL sketches against real Spark SQL. */
@EnabledIfSystemProperty(named = "cascada.spark.integration", matches = "true")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BucketLabelledSparkIntegrationTest {
    private static final long DAY = 86_400L, BASE = 1_759_968_000L;
    private static final String WINDOW = "ts >= " + (BASE + 5_000) + " AND ts <= " + (BASE + 5 * DAY - 100);

    private SparkSession spark;
    private QueryExecutorPort spark(AtomicInteger calls) {
        SparkDeltaQueryExecutor executor = new SparkDeltaQueryExecutor(spark);
        return new QueryExecutorPort() {
            @Override
            public ResultFrame execute(String sql) {
                calls.incrementAndGet();
                return executor.execute(sql);
            }

            @Override
            public ResultFrame project(ResultFrame input, java.util.Map<String, String> types, List<String> projections) {
                return executor.project(input, types, projections);
            }
        };
    }
    private final AtomicInteger sparkCalls = new AtomicInteger();
    private final InMemoryBlobCacheBackendAdapter backend =
            new InMemoryBlobCacheBackendAdapter(new ArrowResultFrameSerializer());
    private final CalciteCanonicalObjectFactory canonicalizer = new CalciteCanonicalObjectFactory();
    private CacheExecutionEngine engine;

    @BeforeAll
    void startSpark() {
        spark = SparkSession.builder().master("local[4]").appName("cascada-bucket-labelled")
                .config("spark.ui.enabled", "false").config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1").config("spark.sql.shuffle.partitions", "8")
                .config("spark.sql.session.timeZone", "UTC").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        spark.sql("""
                SELECT id,
                       %d + pmod(id * 7919, %d) AS ts,
                       CASE WHEN id %% 17 = 0 THEN NULL ELSE concat('r', CAST(id %% 4 AS STRING)) END AS region,
                       CASE WHEN id %% 11 = 0 THEN NULL ELSE id %% 1009 - 400 END AS amount,
                       CASE WHEN id %% 23 = 0 THEN '' ELSE CAST(id %% 30011 AS STRING) END AS user_id
                FROM range(0, 300000, 1, 8)
                """.formatted(BASE, 6 * DAY)).createOrReplaceTempView("events");
        engine = new CacheExecutionEngine(backend, spark(sparkCalls), new GapQueryRewriterAdapter("ts", DAY),
                new CacheExecutionConfiguration(DAY, 300, "ts"), null, null, java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor(),
                new DataSketchesDistinctSketchAdapter());
    }

    @AfterAll
    void stopSpark() {
        if (spark != null) spark.stop();
    }

    @BeforeEach
    void emptyCache() {
        backend.flush(CacheScope.everything());
        sparkCalls.set(0);
    }

    @Test
    void timeSeriesColdHitAndPartialAnswersEqualDirectSpark() {
        String sql = "SELECT CAST(FLOOR(ts / 3600) * 3600 AS BIGINT) AS ts, region, SUM(amount) AS total, "
                + "COUNT(*) AS n FROM events WHERE " + WINDOW
                + " GROUP BY CAST(FLOOR(ts / 3600) * 3600 AS BIGINT), region";
        List<String> direct = sorted(new SparkDeltaQueryExecutor(spark).execute(sql));

        assertThat(sorted(run(sql))).isEqualTo(direct);
        assertThat(sparkCalls.get()).as("one labelled Spark job fills the cold window").isEqualTo(1);

        sparkCalls.set(0);
        assertThat(sorted(run(sql))).isEqualTo(direct);
        assertThat(sparkCalls.get()).as("only the head and tail edges are recomputed").isEqualTo(1);

        CanonicalQueryObject canonical = canonicalizer.canonicalize(sql);
        QueryHash hash = new QueryHashGenerator().generateQueryHash(canonical, 300);
        backend.flush(CacheScope.forKeyPrefix(CacheKeyFactory.buildBucketKey(hash, BASE + 2 * DAY, DAY)));
        assertThat(sorted(run(sql))).as("partial hit refills the evicted bucket").isEqualTo(direct);
        assertThat(sorted(run(sql))).isEqualTo(direct);
    }

    @Test
    void globalAggregateColdAndHitAnswersEqualDirectSpark() {
        String sql = "SELECT COUNT(*) AS n, SUM(amount) AS total, MIN(amount) AS lo FROM events WHERE " + WINDOW;
        List<String> direct = sorted(new SparkDeltaQueryExecutor(spark).execute(sql));

        assertThat(sorted(run(sql))).isEqualTo(direct);
        assertThat(sorted(run(sql))).isEqualTo(direct);
    }

    @Test
    void approximateDistinctCountIsAccurateAndIndependentOfCacheState() {
        String sql = "SELECT region, APPROX_COUNT_DISTINCT(user_id) AS users FROM events WHERE " + WINDOW
                + " GROUP BY region";
        ResultFrame exact = new SparkDeltaQueryExecutor(spark).execute(
                "SELECT region, COUNT(DISTINCT user_id) AS users FROM events WHERE " + WINDOW + " GROUP BY region");

        List<String> cold = sorted(run(sql));
        List<String> hit = sorted(run(sql));
        assertThat(hit).isEqualTo(cold);

        ResultFrame answer = run(sql);
        for (int row = 0; row < exact.rowCount(); row++) {
            Object region = exact.valueAt(row, 0);
            long expected = exact.longAt(row, 1);
            for (int candidate = 0; candidate < answer.rowCount(); candidate++) {
                if (java.util.Objects.equals(answer.valueAt(candidate, 0), region)) {
                    assertThat(answer.longAt(candidate, answer.columnIndex("users")))
                            .isCloseTo(expected, within(Math.max(2L, expected / 20)));
                }
            }
        }
    }

    @Test
    void averagesFromSumAndCountIngredientsEqualSparkAvgExactly() {
        String sql = "SELECT region, AVG(amount) AS mean_amount, AVG(CAST(amount AS DECIMAL(18,2))) AS mean_decimal, "
                + "AVG(CAST(amount AS DECIMAL(10,3)) / 7) AS mean_fraction FROM events WHERE " + WINDOW + " GROUP BY region";
        List<String> direct = sorted(new SparkDeltaQueryExecutor(spark).execute(sql));

        assertThat(sorted(run(sql))).as("cold: direct path also fetches SUM/COUNT").isEqualTo(direct);
        assertThat(sorted(run(sql))).as("hit: buckets hold SUM/COUNT, AVG computed at read").isEqualTo(direct);
        String series = "SELECT CAST(FLOOR(ts / 3600) * 3600 AS BIGINT) AS ts, AVG(CAST(amount AS DECIMAL(18,2))) AS mean "
                + "FROM events WHERE " + WINDOW + " GROUP BY CAST(FLOOR(ts / 3600) * 3600 AS BIGINT)";
        List<String> directSeries = sorted(new SparkDeltaQueryExecutor(spark).execute(series));
        assertThat(sorted(run(series))).isEqualTo(directSeries);
        assertThat(sorted(run(series))).isEqualTo(directSeries);
    }

    @Test
    void expressionsOverAggregatesAndUnaliasedAveragesEqualSparkExactly() {
        String sql = "SELECT region, AVG(amount), SUM(amount) / COUNT(*) AS per_row, MAX(amount) - MIN(amount), "
                + "ROUND(100.0 * SUM(CAST(amount AS DECIMAL(18,2))) / SUM(ABS(amount)), 3) AS share, "
                + "CASE WHEN COUNT(*) > 0 THEN SUM(amount) ELSE 0 END AS guarded "
                + "FROM events WHERE " + WINDOW + " GROUP BY region";
        List<String> direct = sorted(new SparkDeltaQueryExecutor(spark).execute(sql));

        assertThat(sorted(run(sql))).as("cold").isEqualTo(direct);
        assertThat(sorted(run(sql))).as("cache hit").isEqualTo(direct);
    }

    @Test
    void parallelCollectionReturnsEveryRowOfAManyPartitionScan() {
        ResultFrame rows = new SparkDeltaQueryExecutor(spark).execute("SELECT id FROM range(0, 250000, 1, 16)");
        assertThat(rows.rowCount()).isEqualTo(250_000);
        assertThat(rows.longAt(249_999, 0)).isEqualTo(249_999L);
    }

    private ResultFrame run(String sql) {
        CanonicalQueryObject canonical = canonicalizer.canonicalize(sql);
        return engine.execute(canonical, new QueryHashGenerator().generateQueryHash(canonical, 300));
    }

    private static List<String> sorted(ResultFrame frame) {
        List<String> rows = new ArrayList<>();
        for (int row = 0; row < frame.rowCount(); row++) {
            List<String> cells = new ArrayList<>();
            for (int column = 0; column < frame.columnNames().size(); column++) {
                cells.add(frame.columnNames().get(column) + "=" + frame.valueAt(row, column));
            }
            rows.add(String.join(",", cells));
        }
        rows.sort(null);
        return rows;
    }
}
