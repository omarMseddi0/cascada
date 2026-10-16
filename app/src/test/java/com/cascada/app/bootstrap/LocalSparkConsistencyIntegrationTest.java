package com.cascada.app.bootstrap;

import com.cascada.cache.adapter.out.cache.InMemoryBlobCacheBackendAdapter;
import com.cascada.cache.adapter.out.index.InMemoryCoverageIndexAdapter;
import com.cascada.cache.adapter.out.serialization.PortableFrameSerializer;
import com.cascada.cache.adapter.out.sketch.HyperLogLogDistinctCounter;
import com.cascada.cache.adapter.out.sketch.KllQuantileEstimator;
import com.cascada.cache.application.config.CacheExecutionConfiguration;
import com.cascada.cache.application.port.out.QueryExecutorPort;
import com.cascada.cache.application.service.*;
import com.cascada.cache.application.service.cube.CubeShapeCatalog;
import com.cascada.cache.application.service.cube.CubeSubsumptionPlanner;
import com.cascada.cache.application.service.cube.CubeConsistencyVerifier;
import com.cascada.cache.domain.cube.CachedShapeEntry;
import com.cascada.cache.domain.cube.QueryShape;
import com.cascada.cache.application.service.hashing.QueryHashGenerator;
import com.cascada.cache.application.service.safety.SafetyRuleRegistry;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.key.CacheKeyFactory;
import com.cascada.cache.domain.safety.CacheConfiguration;
import com.cascada.cache.domain.query.CanonicalQueryObject;
import com.cascada.cache.domain.time.GapPlan;
import com.cascada.cache.domain.time.TimeRange;
import com.cascada.sql.adapter.out.calcite.CalciteCanonicalObjectFactory;
import com.cascada.sql.adapter.out.calcite.GapQueryRewriterAdapter;
import com.cascada.sql.adapter.out.calcite.LogicalToPhysicalSqlTranslator;
import com.cascada.sql.domain.RegisteredTable;
import com.cascada.sql.domain.TableCatalog;
import com.cascada.spark.adapter.out.spark.SparkDeltaQueryExecutor;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import static org.assertj.core.api.Assertions.*;

/** Differential execution against independent, unmodified Spark SQL; no Java aggregate oracle. */
@EnabledIfSystemProperty(named = "cascada.spark.integration", matches = "true")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LocalSparkConsistencyIntegrationTest {
    private static final long DAY = 86400, BASE = 1759968000L, END = BASE + 7 * DAY - 1;
    private SparkSession spark;
    private java.nio.file.Path warehouse;
    private SparkDeltaQueryExecutor physicalAdapter;
    private QueryExecutorPort direct;
    private final java.util.concurrent.atomic.AtomicInteger physicalQueries = new java.util.concurrent.atomic.AtomicInteger();
    private final CalciteCanonicalObjectFactory canonicalizer = new CalciteCanonicalObjectFactory();
    private final GapQueryRewriterAdapter rewriter = new GapQueryRewriterAdapter("ts", DAY);
    private final QueryHashGenerator hashing = new QueryHashGenerator();

    @BeforeAll void startRealSpark() throws Exception {
        warehouse = java.nio.file.Files.createTempDirectory("cascada-consistency-warehouse-");
        boolean nativeMode = Boolean.parseBoolean(System.getenv("CASCADA_GLUTEN_ENABLED"));
        String master = System.getenv().getOrDefault("SPARK_MASTER","local[2]");
        if (nativeMode) {
            var config = com.cascada.spark.application.configuration.SparkSessionConfigBuilder.forLocal()
                    .master(master).withEnvironment(System::getenv)
                    .withProperty("spark.sql.warehouse.dir", warehouse.toUri().toString())
                    .withProperty("spark.ui.enabled", "false").withProperty("spark.driver.host", "127.0.0.1")
                    .withProperty("spark.driver.bindAddress", "127.0.0.1")
                    .withProperty("spark.sql.shuffle.partitions", "2").withProperty("spark.default.parallelism", "2")
                    .withProperty("spark.sql.session.timeZone", "UTC")
                    .withProperty("spark.sql.ansi.enabled", "false")
                    .withProperty("spark.gluten.sql.native.writer.enabled", "false")
                    .withProperty("spark.databricks.delta.snapshotPartitions", "2")
                    .withProperty("spark.driver.maxResultSize", "128m").build();
            physicalAdapter = new SparkDeltaQueryExecutor(config);
            spark = SparkSession.active();
        } else spark = SparkSession.builder().master(master).appName("cascada-epoch-sql-consistency")
                .config("spark.sql.warehouse.dir", warehouse.toUri().toString())
                .config("spark.ui.enabled", "false").config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1").config("spark.sql.shuffle.partitions", "2")
                .config("spark.default.parallelism", "2").config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.ansi.enabled", "false")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.databricks.delta.snapshotPartitions", "2")
                .config("spark.driver.maxResultSize", "128m").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        if (System.getProperty("os.name").startsWith("Windows")) {
            spark.sparkContext().hadoopConfiguration().set("fs.file.impl", NioLocalFileSystem.class.getName());
            spark.sparkContext().hadoopConfiguration().setBoolean("fs.file.impl.disable.cache", true);
        }
        if (physicalAdapter == null) physicalAdapter = new SparkDeltaQueryExecutor(spark);
        direct = sql -> { physicalQueries.incrementAndGet(); return physicalAdapter.execute(sql); };
        long rows = Long.getLong("cascada.consistency.rows", 1_000_000L);
        if (rows < 10000) throw new IllegalArgumentException("Use at least 10000 rows for the distribution checks");
        // Different populations per day, repeated users across days, nullable groups/measurements,
        // exact integers above 2^53, signed decimals and epoch SECOND boundaries, including midnight.
        var generated = spark.sql("""
                SELECT id, %d + pmod(id * 7919, %d) AS ts,
                    CASE WHEN id %% 19 = 0 THEN NULL ELSE concat('r', CAST(id %% 5 AS STRING)) END AS region,
                    concat('d', CAST(id %% 3 AS STRING)) AS device,
                    CASE WHEN id %% 11 = 0 THEN NULL ELSE CAST(id %% 1009 - 400 AS BIGINT) END AS amount,
                    CASE WHEN id %% 13 = 0 THEN NULL ELSE CAST((id %% 997) * 0.125 AS DOUBLE) END AS latency,
                    CAST((id %% 101 - 50) * 0.127 AS DECIMAL(20,3)) AS price,
                    9007199254740993L + id %% 10 AS huge,
                    id %% 50021 AS user_id, CAST(NULL AS DOUBLE) AS absent
                FROM range(0, %d, 1, %d)
                """.formatted(BASE, 7 * DAY, rows, Math.max(2, (rows + 249999) / 250000)));
        boolean deltaFixture = !System.getProperty("os.name").startsWith("Windows");
        if (deltaFixture) {
            String deltaPath = warehouse.resolve("events").toUri().toString();
            generated.write().format("delta").mode("overwrite").save(deltaPath);
            assertThat(java.nio.file.Files.isDirectory(warehouse.resolve("events/_delta_log"))).isTrue();
            spark.read().format("delta").load(deltaPath).persist(StorageLevel.MEMORY_AND_DISK())
                    .createOrReplaceTempView("events");
        } else generated.persist(StorageLevel.MEMORY_AND_DISK()).createOrReplaceTempView("events");
        assertThat(spark.table("events").count()).isEqualTo(rows);
        spark.sql("SELECT ts, CASE WHEN id % 3 = 0 THEN '😀' WHEN id % 3 = 1 THEN '\uE000' ELSE 'z' END AS region, "
                + "device, amount FROM events").createOrReplaceTempView("unicode_events");
        spark.sql("SELECT ts - " + (BASE + 3 * DAY) + " AS ts, region, amount FROM events")
                .createOrReplaceTempView("negative_events");
        System.out.println("CONSISTENCY_RUNTIME Spark=" + spark.version() + " rows=" + rows
                + " master=" + spark.sparkContext().master() + " heap=" + Runtime.getRuntime().maxMemory()
                + " deltaFixture=" + deltaFixture + " native=" + nativeMode);
    }

    @AfterAll void stop() throws Exception {
        System.out.println("CONSISTENCY_PHYSICAL_SQL_EXECUTIONS " + physicalQueries.get());
        if (physicalAdapter != null) physicalAdapter.close();
        if (spark != null) spark.stop();
        if (warehouse != null) deleteFixture(warehouse);
    }

    private class Harness {
        final List<String> executed = Collections.synchronizedList(new ArrayList<>());
        final InMemoryBlobCacheBackendAdapter cache = new InMemoryBlobCacheBackendAdapter(new PortableFrameSerializer());
        final InMemoryCoverageIndexAdapter coverage = new InMemoryCoverageIndexAdapter();
        final QueryExecutorPort tracked = sql -> { executed.add(sql); return direct.execute(sql); };
        final CacheExecutionEngine engine;
        final ExecuteLogicalQueryService service;
        Harness(boolean cube) {
            engine = new CacheExecutionEngine(cache, tracked, rewriter, CacheExecutionConfiguration.defaults(),
                    coverage, cube ? new CubeShapeCatalog() : null, Runnable::run);
            var cached = new ExecuteCachedQueryService(SafetyRuleRegistry.defaultRegistry(), CacheConfiguration.defaults(),
                    hashing, engine, tracked);
            service = new ExecuteLogicalQueryService(sql -> sql, canonicalizer, cached, tracked);
        }
        void seed(String sql, List<Long> starts) {
            var canonical = canonicalizer.canonicalize(sql);
            assertThat(SafetyRuleRegistry.defaultRegistry().evaluate(canonical, CacheConfiguration.defaults()).isBypass())
                    .as("seed query must be mergeable").isFalse();
            var hash = hashing.generateQueryHash(canonical, 300);
            for (long start : starts) {
                var partial = direct.execute(rewriter.buildStorageQuery(canonical.physicalSql(),
                        new GapPlan(Optional.empty(), List.of(start), Optional.empty()), 300));
                cache.store(CacheKeyFactory.buildBucketKey(hash, start, DAY), partial, java.time.Duration.ofDays(1));
                coverage.markCached(hash, DAY, start);
            }
        }
    }

    record Scenario(String name, String sql, boolean mergeable) {}

    @TestFactory Stream<DynamicTest> aggregateMatrix() {
        List<Scenario> queries = new ArrayList<>();
        String measures = "SUM(amount) AS total, COUNT(*) AS n, COUNT(amount) AS present, "
                + "MIN(amount) AS lo, MAX(amount) AS hi, SUM(price) AS money, MAX(huge) AS exact_large, "
                + "SUM(latency) AS floating_total, COUNT(latency) AS floating_count, "
                + "MIN(latency) AS first_latency, MAX(latency) AS last_latency";
        for (String predicate : List.of("", " AND (region = 'r1' OR device = 'd2')",
                " AND amount IS NOT NULL AND amount >= -100", " AND region IS NULL",
                " AND id < 0", " AND NOT (device = 'd1')")) {
            for (int step : List.of(300, 600, 3600, 90000)) {
                String bucket = "FLOOR(ts / " + step + ") * " + step;
                String sql = "SELECT " + bucket + " AS ts, region, device, " + measures
                        + " FROM events WHERE ts >= " + BASE + " AND ts <= " + END + predicate
                        + " GROUP BY " + bucket + ", region, device ORDER BY ts, region NULLS LAST, device";
                queries.add(new Scenario("timeseries-step-" + step + "-filter-" + queries.size(), sql, true));
            }
        }
        for (String selection : List.of(measures, "AVG(latency) AS mean, COUNT(latency) AS present",
                "SUM(absent) AS total, COUNT(absent) AS present, MIN(absent) AS lo, MAX(absent) AS hi")) {
            queries.add(new Scenario("global-" + queries.size(), "SELECT region, " + selection
                    + " FROM events WHERE ts >= " + BASE + " AND ts <= " + END + " GROUP BY region",
                    true));
        }
        for (String order : List.of("total DESC NULLS LAST, region ASC NULLS FIRST",
                "present ASC, region DESC NULLS LAST", "region DESC NULLS FIRST")) {
            queries.add(new Scenario("top-k-" + queries.size(), "SELECT region, SUM(amount) AS total, COUNT(*) AS present"
                    + " FROM events WHERE ts >= " + BASE + " AND ts <= " + END
                    + " GROUP BY region ORDER BY " + order + " LIMIT 3", true));
        }
        queries.add(new Scenario("head-body-tail", "SELECT FLOOR(ts/600)*600 AS ts, region, SUM(amount) AS total, COUNT(*) AS n"
                + " FROM events WHERE ts > " + (BASE + 123) + " AND ts < " + (END - 177)
                + " GROUP BY FLOOR(ts/600)*600, region ORDER BY ts, region", true));
        queries.add(new Scenario("between", "SELECT region, SUM(amount) AS total FROM events WHERE ts BETWEEN "
                + BASE + " AND " + END + " GROUP BY region ORDER BY total DESC, region", true));
        queries.add(new Scenario("reversed-bounds", "SELECT region, COUNT(*) AS n FROM events WHERE " + BASE
                + " <= ts AND " + END + " >= ts GROUP BY region ORDER BY n, region", true));
        queries.add(new Scenario("empty-scalar", "SELECT SUM(amount) AS total, COUNT(*) AS n, MIN(amount) AS lo"
                + " FROM events WHERE ts >= " + BASE + " AND ts <= " + END + " AND id < 0", true));
        queries.add(new Scenario("raw-epoch-seconds", "SELECT ts, region, SUM(amount) AS total, COUNT(*) AS n"
                + " FROM events WHERE ts >= " + BASE + " AND ts <= " + END
                + " AND id < 5000 GROUP BY ts, region ORDER BY ts, region", true));
        queries.add(new Scenario("unicode-string-order", "SELECT region, SUM(amount) AS total, COUNT(*) AS n"
                + " FROM unicode_events WHERE ts >= " + BASE + " AND ts <= " + END
                + " GROUP BY region ORDER BY region", true));
        queries.add(new Scenario("unicode-top-k", "SELECT region, SUM(amount) AS total, COUNT(*) AS n"
                + " FROM unicode_events WHERE ts >= " + BASE + " AND ts <= " + END
                + " GROUP BY region ORDER BY region DESC LIMIT 2", true));
        queries.add(new Scenario("unicode-string-min-max", "SELECT device, MIN(region) AS first_region, MAX(region) AS last_region, COUNT(*) AS n"
                + " FROM unicode_events WHERE ts >= " + BASE + " AND ts <= " + END
                + " GROUP BY device ORDER BY device", true));
        return queries.stream().filter(q -> q.name.contains(System.getProperty("cascada.consistency.filter", "")))
                .map(q -> DynamicTest.dynamicTest(q.name, () -> checkPaths(q)));
    }

    private void checkPaths(Scenario q) {
        ResultFrame expected = direct.execute(q.sql);
        Harness cold = new Harness(false);
        var first = cold.service.query(q.sql);
        assertThat(first.servedThroughCache()).as(q.name + " eligibility").isEqualTo(q.mergeable);
        same(first.frame(), expected, q.sql, q.name + ":cold");
        cold.executed.clear();
        same(cold.service.query(q.sql).frame(), expected, q.sql, q.name + ":repeat");
        var canonical = canonicalizer.canonicalize(q.sql);
        if (q.mergeable && canonical.metadata().isTimeSeries() && q.name.startsWith("timeseries"))
            assertThat(cold.executed).as("a read-only cold path remains a miss until warming").hasSize(1);
        if (q.mergeable) {
            Harness partial = new Harness(false);
            partial.seed(q.sql, List.of(BASE + DAY, BASE + 3 * DAY, BASE + 5 * DAY));
            same(partial.service.query(q.sql).frame(), expected, q.sql, q.name + ":disjoint-partial");
            assertThat(partial.executed).as("partial must execute a rewritten gap").hasSize(1);
            assertThat(partial.executed.get(0)).doesNotContain("LIMIT");
            Harness all = new Harness(false);
            List<Long> complete = new ArrayList<>();
            long firstDay = q.name.equals("head-body-tail") ? BASE + DAY : BASE;
            long lastDay = q.name.equals("head-body-tail") ? BASE + 5 * DAY : BASE + 6 * DAY;
            for (long day = firstDay; day <= lastDay; day += DAY) complete.add(day);
            all.seed(q.sql, complete);
            same(all.service.query(q.sql).frame(), expected, q.sql, q.name + ":all-body");
            assertThat(all.executed).hasSize(q.name.equals("head-body-tail") ? 1 : 0);
        }
        System.out.println("CONSISTENCY_PASS " + q.name);
    }

    @TestFactory Stream<DynamicTest> complexSqlBypassesRemainAuthoritative() {
        String bounds = "ts >= " + BASE + " AND ts <= " + END;
        List<String> sqls = new ArrayList<>(List.of(
                "SELECT region, COUNT(DISTINCT user_id) AS users FROM events WHERE " + bounds + " GROUP BY region ORDER BY region",
                "SELECT region, SUM(amount) AS total FROM events WHERE " + bounds + " GROUP BY region HAVING SUM(amount) > 10000 ORDER BY total DESC",
                "SELECT region, AVG(latency) AS mean FROM events WHERE " + bounds + " GROUP BY region ORDER BY mean DESC",
                "SELECT region, SUM(amount) + COUNT(*) AS score FROM events WHERE " + bounds + " GROUP BY region ORDER BY score DESC",
                "SELECT region, MAX(amount)-MIN(amount) AS span FROM events WHERE " + bounds + " GROUP BY region ORDER BY span DESC",
                "SELECT region, approx_count_distinct(user_id) AS users FROM events WHERE " + bounds + " GROUP BY region",
                "SELECT DISTINCT region, device FROM events WHERE " + bounds,
                "SELECT region, SUM(amount) AS total FROM events WHERE " + bounds + " GROUP BY region ORDER BY total DESC LIMIT 3 OFFSET 1",
                "SELECT region, SUM(amount) AS total FROM events WHERE " + bounds + " GROUP BY region ORDER BY SUM(amount) DESC",
                "SELECT FLOOR(ts/450)*450 AS ts, COUNT(*) AS n FROM events WHERE " + bounds + " GROUP BY FLOOR(ts/450)*450",
                "SELECT e.region, SUM(e.amount) AS total FROM events e JOIN (SELECT 'd1' AS device) d ON e.device = d.device WHERE e." + bounds.replace(" AND ts", " AND e.ts") + " GROUP BY e.region",
                "SELECT region, SUM(amount) AS total FROM (SELECT ts, region, amount FROM events WHERE " + bounds + ") x GROUP BY region",
                "SELECT region, SUM(amount) AS total FROM events WHERE " + bounds + " AND user_id IN (SELECT user_id FROM events WHERE device = 'd1' GROUP BY user_id) GROUP BY region",
                "SELECT region, SUM(amount) AS total FROM events WHERE " + bounds + " GROUP BY region UNION ALL SELECT region, COUNT(*) AS total FROM events WHERE " + bounds + " GROUP BY region",
                "SELECT region, SUM(amount) AS total, dense_rank() OVER (ORDER BY SUM(amount) DESC) AS rank FROM events WHERE " + bounds + " GROUP BY region"));
        String huge = "WITH filtered AS (SELECT ts, region, device, user_id, amount, latency FROM events WHERE " + bounds
                + "), daily AS (SELECT FLOOR(ts/86400)*86400 AS day, region, device, SUM(amount) AS total, COUNT(*) AS n, "
                + "COUNT(DISTINCT user_id) AS users, AVG(latency) AS mean FROM filtered GROUP BY FLOOR(ts/86400)*86400, region, device), "
                + "regions AS (SELECT region, SUM(total) AS total, SUM(n) AS n FROM daily GROUP BY region HAVING SUM(n) > 10), "
                + "ranked AS (SELECT region, total, n, ROW_NUMBER() OVER (ORDER BY total DESC, region) AS pos FROM regions) "
                + "SELECT r.region, r.total, r.n, r.pos, (SELECT COUNT(*) FROM daily) AS groups "
                + "FROM ranked r WHERE pos <= 4 AND EXISTS (SELECT 1 FROM daily d WHERE d.region = r.region) ORDER BY total DESC, region";
        sqls.add(huge);
        sqls.add("""
                WITH base AS (
                  SELECT ts, region, device, user_id, amount, latency FROM events WHERE %s
                ), by_slot AS (
                  SELECT FLOOR(ts/3600)*3600 AS slot, region, device,
                         SUM(amount) AS total, COUNT(amount) AS present, COUNT(*) AS n,
                         MIN(amount) AS lo, MAX(amount) AS hi, AVG(latency) AS mean,
                         COUNT(DISTINCT user_id) AS users
                  FROM base GROUP BY FLOOR(ts/3600)*3600, region, device
                ), region_totals AS (
                  SELECT region, SUM(total) AS total, SUM(n) AS n FROM by_slot GROUP BY region
                ), device_totals AS (
                  SELECT device, SUM(total) AS total, SUM(n) AS n FROM by_slot GROUP BY device
                ), qualified AS (
                  SELECT s.*, r.total AS region_total, d.total AS device_total,
                         CASE WHEN s.present = 0 THEN NULL ELSE s.total / s.present END AS unit_mean
                  FROM by_slot s
                  LEFT JOIN region_totals r ON s.region = r.region
                  JOIN device_totals d ON s.device = d.device
                  WHERE s.n >= (SELECT MIN(n) FROM by_slot)
                ), ranked AS (
                  SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY total DESC, slot, device) AS pos,
                         SUM(total) OVER (PARTITION BY region ORDER BY slot, device
                                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
                  FROM qualified
                ), head AS (
                  SELECT region, device, total, running_total, users, mean, unit_mean
                  FROM ranked WHERE pos <= 2
                ), tail AS (
                  SELECT region, device, total, running_total, users, mean, unit_mean
                  FROM ranked WHERE pos > 2 AND pos <= 4
                ), combined AS (
                  SELECT * FROM head UNION ALL SELECT * FROM tail
                )
                SELECT region, device, SUM(total) AS total, MAX(running_total) AS running_peak,
                       SUM(users) AS sum_slot_users, AVG(mean) AS mean_of_slots, AVG(unit_mean) AS unit_mean
                FROM combined
                WHERE device IN (SELECT device FROM device_totals WHERE n > 100)
                GROUP BY region, device HAVING COUNT(*) >= 1
                ORDER BY total DESC NULLS LAST, region NULLS FIRST, device LIMIT 10
                """.formatted(bounds));
        return java.util.stream.IntStream.range(0, sqls.size()).mapToObj(i -> DynamicTest.dynamicTest("complex-" + i, () -> {
            String sql = sqls.get(i);
            if (i == 3 || i == 4) {
                String unordered = sql.substring(0, sql.indexOf(" ORDER BY"));
                checkPaths(new Scenario("cached-composite-" + i, unordered, true));
                return;
            }
            if (i == 12) {
                checkPaths(new Scenario("uncorrelated-IN-subquery", sql, true));
                return;
            }
            if (i == 2) {
                // AVG is cached as SUM and COUNT ingredients and divided after the merge.
                // The query orders by its last column only, so tied rows have no defined order:
                // compare the rows as a set and check that the ordered column descends.
                Harness h = new Harness(true);
                var result = h.service.query(sql);
                assertThat(result.servedThroughCache()).isTrue();
                String unordered = sql.substring(0, sql.indexOf(" ORDER BY"));
                ResultFrame expected = direct.execute(sql);
                same(result.frame(), expected, unordered, "complex-cached");
                same(h.service.query(sql).frame(), expected, unordered, "complex-cached-repeat");
                assertDescendingLastColumn(result.frame());
                return;
            }
            // Spark 4.2 itself fails to canonicalize this correlated CTE against InMemoryRelation
            // (NoSuchElementException id#9L). Run both independent executions without Spark table caching.
            if (i >= 15) spark.catalog().uncacheTable("events");
            try {
            Harness h = new Harness(true);
            var expected = direct.execute(sql);
            var result = h.service.query(sql);
            assertThat(result.servedThroughCache()).as("unsafe query must bypass").isFalse();
            assertThat(h.executed).containsExactly(sql);
            same(result.frame(), expected, sql, "complex-" + i);
            same(h.service.query(sql).frame(), expected, sql, "complex-repeat-" + i);
            System.out.println("CONSISTENCY_PASS complex-" + i);
            } finally { if (i >= 15) spark.catalog().cacheTable("events"); }
        }));
    }

    @Test void cubeFilterDownAndExactReuseUseRealSqlFrames() {
        Harness h = new Harness(true);
        String source = "SELECT region, device, SUM(amount) AS total, COUNT(*) AS n, MIN(amount) AS lo, MAX(amount) AS hi "
                + "FROM events WHERE ts >= " + BASE + " AND ts <= " + END + " GROUP BY region, device";
        same(h.service.query(source).frame(), direct.execute(source), source, "cube-source");
        h.executed.clear();
        same(h.service.query(source).frame(), direct.execute(source), source, "cube-exact");
        assertThat(h.executed).isEmpty();
        for (String filter : List.of("device = 'd1'", "region IS NULL", "device IN ('d1', 'd2')", "region <> 'r1'")) {
            String sql = source.replace(" GROUP BY", " AND " + filter + " GROUP BY");
            h.executed.clear();
            same(h.service.query(sql).frame(), direct.execute(sql), sql, "cube-filter-" + filter);
            if (filter.equals("device = 'd1'") || filter.startsWith("device IN"))
                assertThat(h.executed).as("supported filter-down must really reuse cube").isEmpty();
            else assertThat(h.executed).as("unsupported cube predicate must fall back").hasSize(1);
        }
        // The production catalog conservatively refuses changes of projection/dimension shape.
        String rollup = "SELECT region, SUM(amount) AS total, COUNT(*) AS n, MIN(amount) AS lo, MAX(amount) AS hi "
                + "FROM events WHERE ts >= " + BASE + " AND ts <= " + END + " GROUP BY region";
        h.executed.clear();
        same(h.service.query(rollup).frame(), direct.execute(rollup), rollup, "cube-projection-boundary");
        assertThat(h.executed).hasSize(1);
        System.out.println("CONSISTENCY_PASS cube-exact-filter-projection-boundaries");
    }

    @Test void hllAndKllMergeSparkProducedDayPartitionsAgainstSparkTruth() {
        List<HyperLogLogDistinctCounter> hlls = new ArrayList<>();
        List<HyperLogLogDistinctCounter> stringHlls = new ArrayList<>();
        List<KllQuantileEstimator> klls = new ArrayList<>();
        for (int day = 0; day < 7; day++) {
            hlls.add(new HyperLogLogDistinctCounter()); stringHlls.add(new HyperLogLogDistinctCounter());
            klls.add(new KllQuantileEstimator());
        }
        var rows = spark.sql("SELECT ts, user_id, latency, CAST(user_id AS STRING) AS user_string FROM events").toLocalIterator();
        long values = 0;
        while (rows.hasNext()) {
            var row = rows.next();
            int day = (int) ((row.getLong(0) - BASE) / DAY);
            hlls.get(day).add(row.getLong(1));
            stringHlls.get(day).add(row.getString(3));
            if (!row.isNullAt(2)) { klls.get(day).add(row.getDouble(2)); values++; }
        }
        double truth = direct.execute("SELECT COUNT(DISTINCT user_id) AS n FROM events").longAt(0, 0);
        double estimate = HyperLogLogDistinctCounter.unionOf(hlls).estimateDistinctCount();
        assertThat(Math.abs(estimate - truth) / truth).isLessThan(0.05);
        // A one-input union may retain HIP, while a repeated union switches to the composite
        // estimator. Test the true cardinality/error contract, not identical getEstimate() bits.
        double dayTruth = direct.execute("SELECT COUNT(DISTINCT user_id) AS n FROM events WHERE ts < " + (BASE + DAY)).longAt(0, 0);
        double singleEstimate = HyperLogLogDistinctCounter.unionOf(List.of(hlls.get(0))).estimateDistinctCount();
        double duplicateEstimate = HyperLogLogDistinctCounter.unionOf(List.of(hlls.get(0), hlls.get(0))).estimateDistinctCount();
        assertThat(Math.abs(singleEstimate - dayTruth) / dayTruth).isLessThan(0.05);
        assertThat(Math.abs(duplicateEstimate - dayTruth) / dayTruth).isLessThan(0.05);
        var reversed = new ArrayList<>(hlls);
        Collections.reverse(reversed);
        assertThat(HyperLogLogDistinctCounter.unionOf(reversed).estimateDistinctCount()).isEqualTo(estimate);
        double stringTruth = direct.execute("SELECT COUNT(DISTINCT CAST(user_id AS STRING)) AS n FROM events").longAt(0, 0);
        stringHlls.forEach(counter -> counter.add((String) null));
        double stringEstimate = HyperLogLogDistinctCounter.unionOf(stringHlls).estimateDistinctCount();
        assertThat(Math.abs(stringEstimate - stringTruth) / stringTruth).isLessThan(0.05);
        assertThat(HyperLogLogDistinctCounter.unionOf(List.of()).estimateDistinctCount()).isZero();
        KllQuantileEstimator merged = KllQuantileEstimator.mergeOf(klls);
        double maximumRankError = 0;
        for (double rank : List.of(0.0, 0.5, 0.95, 0.99, 1.0)) {
            double value = merged.quantile(rank);
            var count = direct.execute("SELECT COUNT(*) AS n FROM events WHERE latency <= " + value).longAt(0, 0);
            double error = Math.abs((double) count / values - rank);
            maximumRankError = Math.max(maximumRankError, error);
            assertThat(error).as("KLL empirical Spark rank " + rank).isLessThan(0.025);
        }
        System.out.println("CONSISTENCY_PASS sketches HLL_truth=" + truth + " estimate=" + estimate
                + " string_estimate=" + stringEstimate + " single_estimate=" + singleEstimate + " duplicate_estimate=" + duplicateEstimate
                + " KLL_values=" + values + " max_rank_error=" + maximumRankError);
    }

    @Test void hllStringEdgeCasesAgreeWithSparkDistinctNullAndEmptySemantics() {
        String source = "SELECT CAST(NULL AS STRING) AS value UNION ALL SELECT '' UNION ALL SELECT '' "
                + "UNION ALL SELECT 'a' UNION ALL SELECT decode(unhex('00'), 'UTF-8')";
        var first = new HyperLogLogDistinctCounter();
        var second = new HyperLogLogDistinctCounter();
        var rows = spark.sql(source).collectAsList();
        for (int row = 0; row < rows.size(); row++) {
            String value = rows.get(row).isNullAt(0) ? null : rows.get(row).getString(0);
            (row % 2 == 0 ? first : second).add(value);
        }
        long truth = direct.execute("SELECT COUNT(DISTINCT value) AS n FROM (" + source + ") values_table").longAt(0, 0);
        assertThat(truth).isEqualTo(3);
        double estimate = HyperLogLogDistinctCounter.unionOf(List.of(first, second)).estimateDistinctCount();
        assertThat(estimate).as("NULL is excluded, empty string and NUL are distinct values").isCloseTo(truth, within(0.001));
        System.out.println("CONSISTENCY_PASS HLL-null-empty-NUL truth=" + truth + " estimate=" + estimate);
    }

    @TestFactory Stream<DynamicTest> cubeComponentRollupsAgainstIndependentSpark() {
        String bounds = " FROM events WHERE ts >= " + BASE + " AND ts <= " + END;
        String measures = "SUM(amount) AS `SUM(amount)`, COUNT(amount) AS `COUNT(amount)`, "
                + "MIN(amount) AS `MIN(amount)`, MAX(amount) AS `MAX(amount)`, SUM(price) AS `SUM(price)`, "
                + "MAX(huge) AS `MAX(huge)`";
        var aggregateSet = Set.of("SUM(amount)", "COUNT(amount)", "MIN(amount)", "MAX(amount)", "SUM(price)", "MAX(huge)");
        var candidate = new CachedShapeEntry(new QueryShape(Set.of("region", "device"), Set.of(), aggregateSet),
                direct.execute("SELECT region, device, " + measures + bounds + " GROUP BY region, device"));
        List<DynamicTest> cases = new ArrayList<>();
        for (String dimension : List.of("region", "device", "")) {
            for (String filter : List.of("", "device = 'd1'", "device IN ('d1','d2')")) {
                cases.add(DynamicTest.dynamicTest("cube-rollup-" + dimension + "-" + filter, () -> {
                    var shape = new QueryShape(dimension.isEmpty() ? Set.of() : Set.of(dimension),
                            filter.isEmpty() ? Set.of() : Set.of(filter), aggregateSet);
                    var planner = new CubeSubsumptionPlanner();
                    assertThat(planner.subsumes(candidate.shape(), shape)).isTrue();
                    var answer = planner.rollUpAndFilterDown(candidate, shape);
                    String sql = "SELECT " + (dimension.isEmpty() ? "" : dimension + ", ") + measures + bounds
                            + (filter.isEmpty() ? "" : " AND " + filter)
                            + (dimension.isEmpty() ? "" : " GROUP BY " + dimension);
                    same(answer, direct.execute(sql), sql, "component-rollup");
                    assertThat(new CubeConsistencyVerifier().verifyRollUp(candidate, shape, answer).isConsistent()).isTrue();
                    System.out.println("CONSISTENCY_PASS cube-component " + dimension + " " + filter);
                }));
            }
        }
        return cases.stream();
    }

    @Test void negativeEpochSecondsAndStaleCoveragePreserveSparkAnswers() {
        String sql = "SELECT FLOOR(ts/600)*600 AS ts, region, SUM(amount) AS total, COUNT(*) AS n "
                + "FROM negative_events WHERE ts >= " + (-3 * DAY + 123) + " AND ts <= " + (4 * DAY - 178)
                + " GROUP BY FLOOR(ts/600)*600, region ORDER BY ts, region";
        Harness h = new Harness(false);
        h.seed(sql, List.of(-2 * DAY, 0L, 2 * DAY));
        var canonical = canonicalizer.canonicalize(sql);
        var hash = hashing.generateQueryHash(canonical, 300);
        // Mark absent days present, reproducing a coverage index that outlived evicted cache blobs.
        for (long day = -2 * DAY; day <= 2 * DAY; day += DAY) h.coverage.markCached(hash, DAY, day);
        same(h.service.query(sql).frame(), direct.execute(sql), sql, "negative-epoch-stale-coverage");
        assertThat(h.executed).as("one head/tail fetch plus one vanished-day recovery").hasSize(2);
        System.out.println("CONSISTENCY_PASS negative-epochs-stale-coverage");
    }

    @TestFactory Stream<DynamicTest> fixedStepIngredientsResampleToUserSteps() {
        return Stream.of(600, 3600, 90000).map(step -> DynamicTest.dynamicTest("resample-300-to-" + step, () -> {
            String storedSql = "SELECT FLOOR(ts/300)*300 AS ts, region, SUM(amount) AS total, COUNT(*) AS n "
                    + "FROM events WHERE ts >= " + BASE + " AND ts <= " + END
                    + " GROUP BY FLOOR(ts/300)*300, region ORDER BY ts, region";
            var stored = canonicalizer.canonicalize(storedSql);
            var m = stored.metadata();
            var metadata = new com.cascada.cache.domain.query.QueryMetadata(true, m.stalenessToleranceMillis(),
                    m.originalAggregates(), m.compositeAliases(), Optional.of(step), false, m.aggregateSpecs(), m.measureAggregates());
            var request = new CanonicalQueryObject(stored.hashComponents(), stored.timeRange(), stored.postProcessing(),
                    metadata, storedSql, stored.sourceSignature(), stored.projectionSignature(), stored.logicSignature());
            Harness h = new Harness(false);
            h.seed(storedSql, List.of(BASE, BASE + 2 * DAY, BASE + 4 * DAY));
            var result = h.engine.execute(request, hashing.generateQueryHash(request, 300));
            String truth = storedSql.replace("/300", "/" + step).replace("*300", "*" + step);
            same(result, direct.execute(truth), truth, "fixed-step-resample");
            assertThat(h.executed).hasSize(1);
            System.out.println("CONSISTENCY_PASS fixed-step-resample " + step);
        }));
    }

    @Test void recordsAnExecutedSparkPhysicalPlan() {
        var query = spark.sql("SELECT region, SUM(amount) AS total, COUNT(*) AS n FROM events WHERE ts >= "
                + BASE + " AND ts <= " + END + " GROUP BY region ORDER BY total DESC, region");
        assertThat(query.collectAsList()).isNotEmpty();
        String plan = query.queryExecution().executedPlan().toString();
        assertThat(plan).contains("HashAggregate", "Sort");
        System.out.println("CONSISTENCY_EXECUTED_PLAN\n" + plan);
    }

    @Test void cubeSourceWindowAndProjectionBoundariesDoNotReuseWrongAnswers() {
        Harness h = new Harness(true);
        String sql = "SELECT region, SUM(amount) AS total, COUNT(*) AS n FROM events WHERE ts >= " + BASE
                + " AND ts <= " + END + " GROUP BY region";
        h.service.query(sql);
        spark.sql("SELECT ts, region, amount + 17 AS amount FROM events").createOrReplaceTempView("other_events");
        for (String changed : List.of(sql.replace("events", "other_events"),
                sql.replace("AS total", "AS renamed"), sql.replace(Long.toString(END), Long.toString(END - 777)))) {
            h.executed.clear();
            same(h.service.query(changed).frame(), direct.execute(changed), changed, "cube-identity-boundary");
            assertThat(h.executed).hasSize(1);
        }
        System.out.println("CONSISTENCY_PASS cube-source-window-alias-boundaries");
    }

    @Test void registeredLogicalTableTranslatorResolvesComplexStructuresByScope() {
        var catalog = new TableCatalog().register(RegisteredTable.of("traffic", "/unused-local-test-path",
                Map.of("ts", "ts", "region", "C_region", "amount", "C_amount"), "ts"));
        var translator = new LogicalToPhysicalSqlTranslator(300, catalog);
        for (String sql : List.of(
                "WITH x AS (SELECT region, amount FROM traffic) SELECT region, SUM(amount) FROM x GROUP BY region",
                "SELECT region, SUM(amount) FROM (SELECT * FROM traffic) x GROUP BY region",
                "SELECT a.region, SUM(a.amount) FROM traffic a JOIN traffic b ON a.region=b.region GROUP BY a.region",
                "SELECT region, SUM(amount) FROM traffic WHERE region IN (SELECT region FROM traffic) GROUP BY region")) {
            String translated = translator.translateToPhysicalSql(sql);
            assertThat(translated).contains("delta.`/unused-local-test-path`").doesNotContain("FROM traffic");
            assertThatCode(() -> com.cascada.sql.adapter.out.calcite.CalciteSql.parseQuery(translated))
                    .doesNotThrowAnyException();
        }
        System.out.println("CONSISTENCY_PASS logical-translation-complex-scopes");
    }

    @TestFactory Stream<DynamicTest> realSparkWarmingOnlyStoresCompleteBuckets() {
        return Stream.of(false, true).map(timeSeries -> DynamicTest.dynamicTest("warming-" + timeSeries, () -> {
            Harness h = new Harness(false);
            String groups = timeSeries ? "FLOOR(ts/600)*600, region" : "region";
            String selection = timeSeries ? "FLOOR(ts/600)*600 AS ts, region" : "region";
            String observed = "SELECT " + selection + ", SUM(amount) AS total, COUNT(*) AS n FROM events WHERE ts >= "
                    + (BASE + DAY) + " AND ts <= " + (BASE + 5 * DAY - 1) + " GROUP BY " + groups;
            var canonical = canonicalizer.canonicalize(observed);
            var hash = hashing.generateQueryHash(canonical, 300);
            var warmer = new WarmingOrchestrator(h.cache, h.tracked, rewriter,
                    new com.cascada.cache.application.service.warming.WarmingQueue(),
                    new com.cascada.cache.adapter.out.tracking.QueryPopularityTracker(), h.coverage, DAY, 10);
            var first = warmer.warmSinglePattern(hash, canonical, BASE + 123, BASE + 3 * DAY + 177, false);
            assertThat(first.bucketsWarmed()).isEqualTo(3);
            assertThat(h.executed).hasSize(3);
            assertThat(h.cache.existsForKeys(List.of(CacheKeyFactory.buildBucketKey(hash, BASE + 3 * DAY, DAY))))
                    .containsExactly(false);
            String request = observed.replace(Long.toString(BASE + DAY), Long.toString(BASE))
                    .replace(Long.toString(BASE + 5 * DAY - 1), Long.toString(BASE + 3 * DAY - 1));
            h.executed.clear();
            same(h.service.query(request).frame(), direct.execute(request), request, "warmed-Spark-buckets");
            assertThat(h.executed).isEmpty();
            var repeated = warmer.warmSinglePattern(hash, canonical, BASE + 123, BASE + 3 * DAY + 177, false);
            assertThat(repeated.bucketsWarmed()).isZero();
            assertThat(repeated.bucketsSkipped()).isEqualTo(3);
            assertThat(h.executed).isEmpty();
            var forced = warmer.warmSinglePattern(hash, canonical, BASE + 123, BASE + 3 * DAY + 177, true);
            assertThat(forced.bucketsWarmed()).isEqualTo(3);
            assertThat(h.executed).hasSize(3);
            System.out.println("CONSISTENCY_PASS real-Spark-warming " + timeSeries);
        }));
    }

    @TestFactory Stream<DynamicTest> averageIngredientsMatchSparkIncludingAllNullGroups() {
        return Stream.of("latency", "absent").map(field -> DynamicTest.dynamicTest("avg-ingredients-" + field, () -> {
            String source = "SELECT region, SUM(" + field + ") AS `SUM(" + field + ")`, COUNT(" + field
                    + ") AS `COUNT(" + field + ")` FROM events WHERE ts >= " + BASE + " AND ts <= " + END + " GROUP BY region";
            var parsed = canonicalizer.canonicalize(source);
            var canonical = new CanonicalQueryObject(parsed.hashComponents(), parsed.timeRange(), parsed.postProcessing(),
                    parsed.metadata().withOriginalAggregates(List.of("AVG(" + field + ") AS mean")),
                    source, parsed.sourceSignature(), parsed.projectionSignature(), parsed.logicSignature());
            List<ResultFrame> partials = new ArrayList<>();
            for (long day = BASE; day <= END; day += DAY) partials.add(direct.execute(rewriter.buildGapQuery(source,
                    new GapPlan(Optional.empty(), List.of(day), Optional.empty()))));
            var merge = new FrameMergeService(300, "ts");
            String truthSql = "SELECT region, AVG(" + field + ") AS mean FROM events WHERE ts >= " + BASE
                    + " AND ts <= " + END + " GROUP BY region";
            var truth = direct.execute(truthSql);
            same(merge.mergeAndReconstruct(partials, canonical), truth, truthSql, "avg-batch-" + field);
            var incremental = merge.incremental(canonical);
            partials.forEach(incremental::add);
            same(incremental.finish(), truth, truthSql, "avg-incremental-" + field);
            var candidate = new CachedShapeEntry(new QueryShape(Set.of("region"), Set.of(),
                    Set.of("SUM(" + field + ")", "COUNT(" + field + ")")), direct.execute(source));
            var query = new QueryShape(Set.of(), Set.of(), Set.of("AVG(" + field + ")"));
            var planner = new CubeSubsumptionPlanner();
            assertThat(planner.subsumes(candidate.shape(), query)).isTrue();
            var answer = planner.rollUpAndFilterDown(candidate, query);
            String scalarTruth = "SELECT AVG(" + field + ") AS `AVG(" + field + ")` FROM events WHERE ts >= " + BASE + " AND ts <= " + END;
            same(answer, direct.execute(scalarTruth), scalarTruth, "avg-cube-" + field);
            assertThat(new CubeConsistencyVerifier().verifyRollUp(candidate, query, answer).isConsistent()).isTrue();
            System.out.println("CONSISTENCY_PASS average-ingredients " + field);
        }));
    }

    @Test void translatedLogicalSqlExecutesAgainstRealLocalDeltaFiles() throws Exception {
        // Linux validation uses the real Delta writer; the NIO v0 fixture is only for Windows,
        // where Hadoop's winutils dependency otherwise prevents local test writes.
        var directory = java.nio.file.Files.createTempDirectory("cascada-delta-consistency-");
        var file = directory.resolve("part-00000.parquet");
        var schema = org.apache.parquet.schema.MessageTypeParser.parseMessageType("""
                message events { required int64 ts; optional binary C_region (STRING);
                  optional binary C_device (STRING); optional int64 C_amount; optional double C_latency; }
                """);
        var groups = new org.apache.parquet.example.data.simple.SimpleGroupFactory(schema);
        try {
            if (!System.getProperty("os.name").startsWith("Windows")) {
                spark.sql("SELECT ts, region AS C_region, device AS C_device, amount AS C_amount, latency AS C_latency FROM events")
                        .write().format("delta").mode("overwrite").save(directory.toUri().toString());
            } else {
                try (var writer = org.apache.parquet.hadoop.example.ExampleParquetWriter.builder(new LocalParquetOutput(file))
                        .withConf(new org.apache.hadoop.conf.Configuration()).withType(schema)
                        .withRowGroupSize(8 * 1024 * 1024).build()) {
                    var rows = spark.sql("SELECT ts, region, device, amount, latency FROM events").toLocalIterator();
                    while (rows.hasNext()) {
                        var row = rows.next();
                        var group = groups.newGroup().append("ts", row.getLong(0));
                        for (int column = 1; column < 5; column++) {
                            if (row.isNullAt(column)) continue;
                            if (column < 3) group.add(column, row.getString(column));
                            else if (column == 3) group.add(column, row.getLong(column));
                            else group.add(column, row.getDouble(column));
                        }
                        writer.write(group);
                    }
                }
                var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                var deltaSchema = new org.apache.spark.sql.types.StructType()
                        .add("ts", "long", false).add("C_region", "string").add("C_device", "string")
                        .add("C_amount", "long").add("C_latency", "double");
                var log = java.nio.file.Files.createDirectory(directory.resolve("_delta_log"));
                String content = mapper.writeValueAsString(Map.of("protocol", Map.of("minReaderVersion", 1, "minWriterVersion", 2)))
                        + "\n" + mapper.writeValueAsString(Map.of("metaData", Map.of("id", UUID.randomUUID().toString(),
                                "format", Map.of("provider", "parquet", "options", Map.of()), "schemaString", deltaSchema.json(),
                                "partitionColumns", List.of(), "configuration", Map.of(), "createdTime", 0L)))
                        + "\n" + mapper.writeValueAsString(Map.of("add", Map.of("path", file.getFileName().toString(),
                                "partitionValues", Map.of(), "size", java.nio.file.Files.size(file), "modificationTime", 0L, "dataChange", true))) + "\n";
                java.nio.file.Files.writeString(log.resolve("00000000000000000000.json"), content);
                }
            var catalog = new TableCatalog().register(RegisteredTable.of("traffic", directory.toUri().toString(),
                    Map.of("ts", "ts", "region", "C_region", "device", "C_device", "amount", "C_amount", "latency", "C_latency"), "ts"));
            var translator = new LogicalToPhysicalSqlTranslator(300, catalog);
            Harness h = new Harness(false);
            var service = new ExecuteLogicalQueryService(translator, canonicalizer,
                    new ExecuteCachedQueryService(SafetyRuleRegistry.defaultRegistry(), CacheConfiguration.defaults(), hashing, h.engine, h.tracked), h.tracked);
            for (String projection : List.of("region, SUM(amount) AS total, COUNT(*) AS n", "region, AVG(latency) AS mean",
                    "ts, region, SUM(amount) AS total, COUNT(*) AS n")) {
                boolean timeSeries = projection.startsWith("ts,");
                String grouped = timeSeries ? "ts, region" : "region";
                String logical = "SELECT " + projection + " FROM traffic WHERE ts >= " + BASE + " AND ts <= " + END
                        + " GROUP BY " + grouped + " ORDER BY " + grouped;
                String physical = translator.translateToPhysicalSql(logical);
                assertThat(physical).contains("delta.`", "C_region");
                String expectedSql = logical.replace("traffic", "events").replace("SELECT region,", "SELECT region AS C_region,");
                if (timeSeries) expectedSql = "SELECT CAST(FLOOR(ts/300)*300 AS BIGINT) AS ts, region AS C_region, "
                        + "SUM(amount) AS total, COUNT(*) AS n FROM events WHERE ts >= " + BASE + " AND ts <= " + END
                        + " GROUP BY CAST(FLOOR(ts/300)*300 AS BIGINT), region ORDER BY ts, C_region";
                same(service.query(logical).frame(), direct.execute(expectedSql), physical, "translated-delta");
                if (timeSeries) {
                    h.executed.clear();
                    same(service.query(logical).frame(), direct.execute(expectedSql), physical, "translated-delta-all-cache");
                    assertThat(h.executed).isEmpty();
                }
                if (!projection.contains("AVG")) {
                    Harness partial = new Harness(false);
                    partial.seed(physical, List.of(BASE + DAY, BASE + 3 * DAY));
                    var partialService = new ExecuteLogicalQueryService(translator, canonicalizer,
                            new ExecuteCachedQueryService(SafetyRuleRegistry.defaultRegistry(), CacheConfiguration.defaults(),
                                    hashing, partial.engine, partial.tracked), partial.tracked);
                    same(partialService.query(logical).frame(), direct.execute(expectedSql), physical, "translated-delta-partial");
                    assertThat(partial.executed).hasSize(1);
                }
            }
            System.out.println("CONSISTENCY_PASS real-delta-log-parquet-logical-physical");
        } finally {
            // Only this exact, freshly created fixture directory is traversed and removed.
            deleteFixture(directory);
        }
    }

    private static void deleteFixture(java.nio.file.Path directory) throws java.io.IOException {
        var resolved = directory.toAbsolutePath().normalize();
        var temp = java.nio.file.Path.of(System.getProperty("java.io.tmpdir")).toAbsolutePath().normalize();
        if (!resolved.getParent().equals(temp) || !resolved.getFileName().toString().startsWith("cascada-"))
            throw new IllegalArgumentException("Refusing to remove an unexpected fixture path: " + resolved);
        try (var paths = java.nio.file.Files.walk(resolved)) {
            for (var path : paths.sorted(Comparator.reverseOrder()).toList()) java.nio.file.Files.deleteIfExists(path);
        }
    }

    /** Only test fixture IO: Spark reads real bytes; native Windows ACL helpers are not needed. */
    public static class NioLocalFileSystem extends org.apache.hadoop.fs.RawLocalFileSystem {
        public NioLocalFileSystem() {}
        private java.nio.file.Path local(org.apache.hadoop.fs.Path path) {
            return java.nio.file.Path.of(makeQualified(path).toUri());
        }
        @Override public org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path path) throws java.io.IOException {
            var file = local(path);
            if (!java.nio.file.Files.exists(file)) throw new java.io.FileNotFoundException(file.toString());
            var attributes = java.nio.file.Files.readAttributes(file, java.nio.file.attribute.BasicFileAttributes.class);
            return new org.apache.hadoop.fs.FileStatus(attributes.size(), attributes.isDirectory(), 1,
                    getDefaultBlockSize(path), attributes.lastModifiedTime().toMillis(), makeQualified(path));
        }
        @Override public org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path path) throws java.io.IOException {
            if (!java.nio.file.Files.isDirectory(local(path))) return new org.apache.hadoop.fs.FileStatus[]{getFileStatus(path)};
            List<org.apache.hadoop.fs.FileStatus> result = new ArrayList<>();
            try (var children = java.nio.file.Files.list(local(path))) {
                for (var child : children.toList()) result.add(getFileStatus(new org.apache.hadoop.fs.Path(child.toUri())));
            }
            return result.toArray(org.apache.hadoop.fs.FileStatus[]::new);
        }
        @Override public boolean mkdirs(org.apache.hadoop.fs.Path path, org.apache.hadoop.fs.permission.FsPermission permission) throws java.io.IOException {
            java.nio.file.Files.createDirectories(local(path));
            return true;
        }
        @Override public void setPermission(org.apache.hadoop.fs.Path path, org.apache.hadoop.fs.permission.FsPermission permission) {}
    }

    private record LocalParquetOutput(java.nio.file.Path path) implements org.apache.parquet.io.OutputFile {
        @Override public org.apache.parquet.io.PositionOutputStream create(long hint) throws java.io.IOException {
            return stream(java.nio.file.StandardOpenOption.CREATE_NEW);
        }
        @Override public org.apache.parquet.io.PositionOutputStream createOrOverwrite(long hint) throws java.io.IOException {
            return stream(java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.TRUNCATE_EXISTING);
        }
        private org.apache.parquet.io.PositionOutputStream stream(java.nio.file.OpenOption... options) throws java.io.IOException {
            var output = new java.io.BufferedOutputStream(java.nio.file.Files.newOutputStream(path, options));
            return new org.apache.parquet.io.PositionOutputStream() {
                long position;
                @Override public long getPos() { return position; }
                @Override public void write(int value) throws java.io.IOException { output.write(value); position++; }
                @Override public void write(byte[] bytes, int offset, int length) throws java.io.IOException { output.write(bytes, offset, length); position += length; }
                @Override public void flush() throws java.io.IOException { output.flush(); }
                @Override public void close() throws java.io.IOException { output.close(); }
            };
        }
        @Override public boolean supportsBlockSize() { return false; }
        @Override public long defaultBlockSize() { return 0; }
    }

    private static void assertDescendingLastColumn(ResultFrame frame) {
        int column = frame.columnNames().size() - 1;
        for (int row = 1; row < frame.rowCount(); row++) {
            double previous = ((Number) frame.valueAt(row - 1, column)).doubleValue();
            double current = ((Number) frame.valueAt(row, column)).doubleValue();
            assertThat(current).as("row " + row + " of the ORDER BY ... DESC column").isLessThanOrEqualTo(previous);
        }
    }

    private void same(ResultFrame actual, ResultFrame expected, String sql, String context) {
        assertThat(actual.columnNames()).as(context + " columns").isEqualTo(expected.columnNames());
        assertThat(actual.columnTypes()).as(context + " types").isEqualTo(expected.columnTypes());
        List<List<Object>> a = cells(actual), b = cells(expected);
        if (!sql.toUpperCase(Locale.ROOT).contains("ORDER BY")) {
            Comparator<List<Object>> comparator = Comparator.comparing(Object::toString);
            a.sort(comparator); b.sort(comparator);
        }
        assertThat(a).as(context + " values SQL=" + sql).isEqualTo(b);
    }

    private List<List<Object>> cells(ResultFrame frame) {
        List<List<Object>> result = new ArrayList<>();
        for (int row = 0; row < frame.rowCount(); row++) {
            List<Object> cells = new ArrayList<>();
            for (int col = 0; col < frame.columnNames().size(); col++) {
                Object value = frame.valueAt(row, col);
                if (value instanceof Double d && Double.isFinite(d)) value = BigDecimal.valueOf(d).setScale(8, java.math.RoundingMode.HALF_UP);
                else if (value instanceof BigDecimal decimal) value = decimal.stripTrailingZeros();
                cells.add(value);
            }
            result.add(cells);
        }
        return result;
    }
}
