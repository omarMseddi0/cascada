package com.cascada.app.bootstrap;

import com.cascada.app.config.CascadaEngineFactory;
import com.cascada.app.adapter.out.execution.LazyQueryExecutor;
import com.cascada.app.adapter.in.api.QueryHttpServer;
import com.cascada.app.adapter.in.api.QueryResponseEncoder;
import com.cascada.app.model.config.ServerSettings;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import com.cascada.app.model.config.ApplicationSettings;
import com.cascada.app.model.config.EngineSettings;
import com.cascada.cache.domain.model.cube.ExactCubeFragment;
import com.cascada.sql.adapter.out.calcite.CalciteCanonicalObjectFactory;
import com.cascada.sql.adapter.out.calcite.LogicalToPhysicalSqlTranslator;
import com.cascada.sql.domain.TableCatalog;
import com.cascada.sql.domain.model.RegisteredTable;
import java.util.Set;
import com.cascada.cache.application.model.config.CacheExecutionConfiguration;
import com.cascada.cache.application.model.query.QueryResult;
import com.cascada.cache.application.port.out.PinnedQueryExecutorPort;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.model.cube.ExactCubeSource;
import com.cascada.cache.fixture.ValkeyTestDatabase;
import com.cascada.spark.adapter.out.spark.SparkDeltaQueryExecutor;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkThrowable;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises the production logical-entry composition root with independent pinned Spark SQL. */
@EnabledIfEnvironmentVariable(named = ValkeyTestDatabase.ENVIRONMENT_VARIABLE, matches = ".+")
@EnabledIfSystemProperty(named = "cascada.spark.integration", matches = "true")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.MethodName.class)
class ExactCubeDifferentialIntegrationTest {
    private static final long DAY = 86_400L;
    private static final String SCOPE = "qualification";
    private static final long SEED = Long.getLong("cascada.exact.fixture.seed", 147_811L);
    private static final String SOURCE = "fact_data";
    private static final int HIGH = 9;
    private static final ServerSettings TEST_SERVER = new ServerSettings(0, 0, Duration.ofSeconds(60));

    @RegisterExtension final ValkeyTestDatabase valkey = new ValkeyTestDatabase();
    @TempDir static Path scratch;

    private SparkSession spark;
    private SparkDeltaQueryExecutor oracle;
    private String path;
    private TrackingExecutor tracked;
    private String fixtureSql;
    private final LongAdder sparkJobs = new LongAdder();
    private final LongAdder inputBytes = new LongAdder();

    @BeforeAll void setup() {
        spark = LocalSparkFixtures.localDeltaSession().master("local[2]").appName("exact-cube-differential")
                .config("spark.local.dir", scratch.resolve("spark").toString())
                .config("spark.sql.warehouse.dir", scratch.resolve("warehouse").toString())
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.databricks.delta.snapshotPartitions", "2")
                .config("spark.sql.shuffle.partitions", "2").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        spark.sparkContext().addSparkListener(new SparkListener() {
            @Override public void onJobStart(SparkListenerJobStart event) {
                sparkJobs.increment();
            }

            @Override public void onTaskEnd(SparkListenerTaskEnd event) {
                if (event.taskMetrics() != null) inputBytes.add(event.taskMetrics().inputMetrics().bytesRead());
            }
        });
        oracle = new SparkDeltaQueryExecutor(spark.newSession());
        tracked = new TrackingExecutor(new SparkDeltaQueryExecutor(spark));
        path = scratch.resolve("facts").toUri().toString();
        long generatedRows = Long.getLong("cascada.exact.fixture.rows", 64);
        fixtureSql = """
                SELECT CAST(ts AS BIGINT) AS ts, region, device, CAST(amount AS INT) AS amount
                FROM VALUES
                (0, NULL, 'x', -1), (0, 'all-null', 'x', NULL), (0, 'zero', 'x', 0),
                (518399, 'merge-winner', 'x', 20), (0, 'local-winner', 'y', 35),
                (518400, 'merge-winner', 'y', 51), (518400, 'all-null', 'y', NULL),
                (777599, NULL, 'x', 0), (777600, 'outside', 'x', 900),
                (777599, 'late-only', 'x', 2), (777599, 'late-only', 'x', 2),
                (777599, '😀', 'x', 0), (777599, '', 'x', 0)
                AS rows(ts, region, device, amount)
                UNION ALL
                SELECT pmod(id * 7919 + %d, %d),
                       CASE WHEN %d > 10000 AND pmod(id, 10) < 8 THEN 'skew'
                            ELSE concat('r', CAST(pmod(id, 13) AS STRING)) END,
                       concat('d', CAST(pmod(id, 5) AS STRING)),
                       CASE WHEN pmod(id, 11) = 0 THEN NULL ELSE CAST(pmod(id, 101) - 50 AS INT) END
                FROM range(0, %d)
                """.formatted(SEED, HIGH * DAY, generatedRows, generatedRows);
        spark.sql(fixtureSql).write().format("delta").partitionBy("device").save(path);
    }

    @AfterAll void shutdown() {
        if (spark != null) spark.stop();
    }

    @Test void qualifiesThreeHundredDistinctSqlOperatorCompositions() {
        qualifySqlCompositions(CubeQualificationSql.reusable(SOURCE), CubeQualificationSql.outsideCube(SOURCE));
    }

    @Test void qualifiesAdditionalCubeBackedSqlCompositions() {
        var base = CubeQualificationSql.reusable(SOURCE);
        var additional = CubeQualificationSql.additionalReusable(SOURCE);
        assertThat(additional).doesNotContainAnyElementsOf(base);
        assertThat((base.size() + additional.size()) * CubeQualificationSql.consumers(SOURCE).size())
                .isEqualTo(300);
        qualifySqlCompositions(additional, List.of());
    }

    private void qualifySqlCompositions(List<String> reusable, List<String> outside) {
        var consumers = CubeQualificationSql.consumers(SOURCE);
        var statements = new java.util.LinkedHashSet<String>();
        var failures = new ArrayList<Throwable>();
        int[] passed = new int[2];
        try (var factory = factory(SCOPE, true)) {
            var entry = factory.executeLogicalQueryUseCase();
            warm(factory, aggregate("AVG", "region, device", 0, HIGH, ""), 0, HIGH);
            for (int route = 0; route < passed.length; route++) {
                var producers = route == 0 ? reusable : outside;
                String expectedRoute = route == 0 ? "exact-cube" : "existing";
                for (String producer : producers) {
                    for (String consumer : consumers) {
                        String sql = "WITH cube_input AS (" + producer + ") " + consumer;
                        assertThat(statements.add(sql)).as(sql).isTrue();
                        tracked.calls.clear();
                        try {
                            var result = entry.query(sql);
                            check(sql, 0, consumer.contains("LIMIT 5 OFFSET 1"), result, expectedRoute);
                            if (route == 0) {
                                assertThat(result.exactCubeTrace().fragments()).isNotEmpty();
                                assertThat(result.exactCubeTrace().residualSql()).isEmpty();
                                assertThat(tracked.calls).containsExactly(result.exactCubeTrace().outerSql());
                            }
                            passed[route]++;
                        } catch (Exception | AssertionError failure) {
                            saveMismatch(sql, 0, null, failure);
                            failures.add(new AssertionError(sql, failure));
                        }
                    }
                }
            }
        }
        assertThat(statements).hasSize((reusable.size() + outside.size()) * consumers.size());
        System.out.printf("CUBE_SQL_CORPUS distinct=%d reused=%d declined=%d failed=%d%n",
                statements.size(), passed[0], passed[1], failures.size());
        if (!failures.isEmpty()) {
            var summary = new AssertionError(failures.size() + " SQL qualification failures");
            failures.forEach(summary::addSuppressed);
            throw summary;
        }
    }

    @Test void usesTheOriginalAverageBucketForBothOrdinaryAndNestedSql() {
        String producer = aggregate("AVG", "region, device", 0, HIGH, "");
        String consumer = "WITH sliced AS (" + aggregate("AVG", "region", 0, HIGH, " AND device = 'x'")
                + ") SELECT region, value FROM sliced ORDER BY region NULLS FIRST";
        try (var factory = factory(SCOPE, true); var inspector = RedisClient.create(valkey.uri());
             var connection = inspector.connect()) {
            var entry = factory.executeLogicalQueryUseCase();
            tracked.calls.clear();
            check(producer, 0, false, entry.query(producer), "existing");
            var commands = connection.sync();
            assertThat(commands.keys("QC:V4:exact:v1:frame:*")).isEmpty();
            var keys = warm(factory, producer, 0, HIGH);
            assertThat(commands.keys("QC:V4:B*")).containsExactlyInAnyOrderElementsOf(keys);
            tracked.calls.clear();
            long bindsBefore = tracked.bindCalls.sum();
            var direct = entry.query(producer);
            check(producer, 0, false, direct, "same-query-cache");
            assertThat(direct.servedThroughCache()).isTrue();
            assertThat(tracked.bindCalls.sum()).isEqualTo(bindsBefore);
            assertThat(tracked.calls).isEmpty();

            var result = entry.query(consumer);
            assertThat(tracked.bindCalls.sum()).isEqualTo(bindsBefore + 1);
            check(consumer, 0, true, result, "exact-cube");
            assertThat(result.exactCubeTrace().fragments()).extracting(ExactCubeFragment::bucketKey)
                    .containsExactlyInAnyOrderElementsOf(keys);
            assertThat(result.exactCubeTrace().residualSql()).isEmpty();
            assertThat(tracked.calls).containsExactly(result.exactCubeTrace().outerSql());
            assertThat(commands.keys("QC:V4:B*")).containsExactlyInAnyOrderElementsOf(keys);
            assertThat(commands.keys("QC:V4:exact:v1:frame:*")).isEmpty();

            var normal = factory(SCOPE, false);
            try (normal) {
                tracked.calls.clear();
                check(producer, 0, false, normal.executeLogicalQueryUseCase().query(producer), null);
                assertThat(tracked.calls).isEmpty();
            }
        }
    }

    @Test void incompleteSameQueryCoverageStillComposesUsingAnotherShape() {
        String query = aggregate("AVG", "region", 0, 2, "");
        try (var factory = factory(SCOPE, true)) {
            String directBucket = warm(factory, query, 0, 1).getFirst();
            String otherShape = warm(factory, aggregate("AVG", "region, device", 0, 2, ""), 1, 2).getFirst();

            tracked.calls.clear();
            long bindsBefore = tracked.bindCalls.sum();
            var result = factory.executeLogicalQueryUseCase().query(query);
            check(query, 0, false, result, "exact-cube");
            assertThat(result.exactCubeTrace().fragments()).extracting(ExactCubeFragment::bucketKey)
                    .containsExactlyInAnyOrder(directBucket, otherShape);
            assertThat(result.exactCubeTrace().residualSql()).isEmpty();
            assertThat(tracked.bindCalls.sum()).isEqualTo(bindsBefore + 1);
            assertThat(tracked.calls).isEmpty();
        }
    }

    @Test void twentyDaysCombineSameQueryBucketsTwoBroaderShapesAndDisjointSparkGaps() {
        String original = path;
        path = scratch.resolve("twenty-day-facts").toUri().toString();
        spark.sql("""
                SELECT pmod(id, 20) * 86400L AS ts,
                       CASE WHEN pmod(id, 13) = 0 THEN NULL ELSE concat('region-', pmod(id, 7)) END AS region,
                       CASE pmod(id DIV 20, 3) WHEN 0 THEN 'x' WHEN 1 THEN 'y' ELSE NULL END AS device,
                       pmod(id, 11) AS shop,
                       CASE WHEN pmod(id, 17) = 0 THEN NULL ELSE CAST(pmod(id, 31) - 15 AS INT) END AS amount
                FROM range(600)
                """).write().format("delta").save(path);
        String filter = " AND device = 'x'";
        String query = aggregate("AVG", "region", 0, 20, filter);
        try (var factory = factory(SCOPE, true)) {
            var expectedKeys = new ArrayList<>(warm(factory, query, 0, 5));
            expectedKeys.addAll(warm(factory, aggregate("AVG", "region, device", 5, 8, ""), 5, 8));
            expectedKeys.addAll(warm(factory, aggregate("AVG", "region, device", 9, 12, filter), 9, 12));
            expectedKeys.addAll(warm(factory, aggregate("AVG", "region, device, shop", 13, 16, ""), 13, 16));
            // These buckets have data for the final gap but have already aggregated away device.
            var incompatibleKeys = warm(factory, aggregate("AVG", "region, shop", 16, 20, ""), 16, 20);

            tracked.calls.clear();
            var answer = factory.executeLogicalQueryUseCase().query(query);
            check(query, 0, false, answer, "exact-cube");
            var trace = answer.exactCubeTrace();
            assertThat(trace.fragments()).extracting(ExactCubeFragment::bucketKey)
                    .containsExactlyInAnyOrderElementsOf(expectedKeys)
                    .doesNotContainAnyElementsOf(incompatibleKeys);
            assertThat(trace.residualSql()).hasSize(3);
            long[][] expectedGaps = {{8, 9}, {12, 13}, {16, 20}};
            for (int i = 0; i < expectedGaps.length; i++) {
                assertThat(trace.residualSql().get(i)).contains("VERSION AS OF 0", "SUM(`amount`)", "COUNT(`amount`)",
                        "`ts` >= " + expectedGaps[i][0] * DAY, "`ts` < " + expectedGaps[i][1] * DAY,
                        "device = 'x'");
            }
            assertThat(tracked.calls).containsExactlyElementsOf(trace.residualSql());
            System.out.printf(java.util.Locale.ROOT,
                    "TWENTY_DAY_CUBE cachedBuckets=%d residuals=%d planningMs=%.3f%n",
                    trace.fragments().size(), trace.residualSql().size(), trace.planningNanos() / 1_000_000.0);
        } finally {
            path = original;
        }
    }

    @Test void ignoresOldBucketProvenanceAfterTheOrdinaryKeyIsOverwritten() {
        String producer = aggregate("AVG", "region, device", 0, 1, "");
        String consumer = aggregate("AVG", "region", 0, 1, "");
        try (var factory = factory(SCOPE, true)) {
            String key = warm(factory, producer, 0, 1).getFirst();
            var entry = factory.executeLogicalQueryUseCase();
            check(consumer, 0, false, entry.query(consumer), "exact-cube");
            var backend = factory.cacheBackendForShutdown();
            backend.store(key, ResultFrame.builder().column("corrupted", ColumnType.LONG).row(1L).build(), Duration.ofHours(1));
            tracked.calls.clear();
            var result = entry.query(consumer);
            check(consumer, 0, false, result, "pinned-spark");
            assertThat(tracked.calls).singleElement().asString().contains("VERSION AS OF 0");
            assertThat(backend.multiGet(List.of(key)).getFirst()).hasValueSatisfying(frame ->
                    assertThat(frame.columnNames()).containsExactly("corrupted"));
        }
    }

    @Test void emptyOrdinaryBucketsPreserveGroupedAndScalarSqlSemantics() {
        String grouped = aggregate("AVG", "region", 12, 13, "");
        String scalar = "SELECT AVG(amount) AS mean FROM " + SOURCE + " WHERE ts >= " + 12 * DAY
                + " AND ts < " + 13 * DAY;
        try (var factory = factory(SCOPE, true)) {
            warm(factory, aggregate("AVG", "region, device", 12, 13, ""), 12, 13);
            var entry = factory.executeLogicalQueryUseCase();
            tracked.calls.clear();
            var groupedResult = entry.query(grouped);
            check(grouped, 0, false, groupedResult, "exact-cube");
            assertThat(groupedResult.frame().rowCount()).isZero();
            var scalarResult = entry.query(scalar);
            check(scalar, 0, false, scalarResult, "exact-cube");
            assertThat(scalarResult.frame().rowCount()).isEqualTo(1);
            assertThat(scalarResult.frame().isNullAt(0, 0)).isTrue();
            assertThat(tracked.calls).isEmpty();
        }
    }

    @Test void cancelledCompositionLeavesOrdinaryBucketsReadable() {
        String producer = aggregate("AVG", "region, device", 0, 1, "");
        String consumer = aggregate("AVG", "region", 0, 1, "");
        try (var factory = factory(SCOPE, true)) {
            var keys = warm(factory, producer, 0, 1);
            var entry = factory.executeLogicalQueryUseCase();
            var cancellation = new com.cascada.cache.application.execution.QueryCancellation();
            tracked.afterBind.set(cancellation::cancel);
            try (var scope = new com.cascada.cache.application.execution.QueryExecutionScope(cancellation)) {
                org.assertj.core.api.Assertions.assertThatThrownBy(() -> entry.query(consumer))
                        .isInstanceOf(java.util.concurrent.CancellationException.class);
            }
            check(consumer, 0, false, entry.query(consumer), "exact-cube");
            assertThat(factory.cacheBackendForShutdown().existsForKeys(keys)).containsExactly(true);
        }
    }

    @Test void normalBucketCompositionPinsTheResidualAcrossAnExternalWrite() {
        String original = path;
        path = scratch.resolve("bucket-race").toUri().toString();
        spark.sql(fixtureSql).write().format("delta").save(path);
        String query = aggregate("AVG", "region", 0, HIGH, "") + " ORDER BY region NULLS FIRST";
        try (var factory = factory(SCOPE, true)) {
            warm(factory, aggregate("AVG", "region, device", 0, 6, ""), 0, 6);
            tracked.beforePinned.set(() -> spark.newSession().sql("INSERT INTO delta.`" + path
                    + "` VALUES (518400L, 'external-row', 'writer', 700)").collectAsList());
            tracked.calls.clear();
            var result = factory.executeLogicalQueryUseCase().query(query);
            check(query, 0, true, result, "exact-cube");
            var trace = result.exactCubeTrace();
            assertThat(trace.fragments()).hasSize(6);
            assertThat(trace.residualSql()).singleElement().asString()
                    .contains("VERSION AS OF 0", "SUM(`amount`)", "COUNT(`amount`)", "`ts` >= 518400");
            assertThat(tracked.calls).containsExactly(trace.residualSql().getFirst());
            check(query, 1, true, factory.executeLogicalQueryUseCase().query(query), "existing");
        } finally {
            path = original;
        }
    }

    @Test void seededCompoundDeclinesStillMatchPinnedSpark() {
        org.junit.jupiter.api.Assumptions.assumeTrue(Long.getLong("cascada.exact.fixture.rows", 64) <= 10000,
                "exhaustive join corpus uses the tiny fixture");
        var bounds = "ts >= 0 AND ts < " + HIGH * DAY;
        var small = "SELECT region, amount FROM " + SOURCE + " WHERE " + bounds + " AND ts < " + 2 * DAY;
        var cases = new ArrayList<>(List.of(
                "SELECT a.region, COUNT(*) AS n FROM (" + small + ") a LEFT JOIN (" + small
                        + ") b ON a.region <=> b.region WHERE b.amount IS NULL GROUP BY a.region",
                "SELECT region, amount FROM (" + small + " UNION ALL " + small + ") q EXCEPT ALL " + small,
                "SELECT region, SUM(CAST(amount AS DECIMAL(20, 3))) AS total FROM " + SOURCE
                        + " WHERE " + bounds + " GROUP BY ROLLUP(region, device)",
                "SELECT region, SUM(amount) AS value FROM " + SOURCE + " WHERE " + bounds
                        + " GROUP BY region ORDER BY value DESC, region LIMIT 5 OFFSET 1",
                "SELECT region, amount FROM (" + small + " UNION ALL " + small + ") q INTERSECT ALL " + small,
                "SELECT a.region, b.region AS other_region, COUNT(*) AS n FROM (" + small + ") a FULL JOIN ("
                        + small + ") b ON a.region = b.region AND b.amount > 0 GROUP BY a.region, b.region",
                "SELECT f.region, SUM(f.amount) AS total FROM " + SOURCE + " f WHERE f.region NOT IN "
                        + "(SELECT d.region FROM " + SOURCE + " d WHERE d.device = f.device) GROUP BY f.region",
                "SELECT f.region, SUM(f.amount) AS total FROM " + SOURCE + " f WHERE NOT EXISTS "
                        + "(SELECT 1 FROM " + SOURCE + " d WHERE d.device = f.device AND d.region = f.region) GROUP BY f.region",
                "SELECT region, device, GROUPING_ID(region, device) AS gid, COUNT(amount) AS n FROM " + SOURCE
                        + " WHERE " + bounds + " GROUP BY GROUPING SETS ((region, device), (region), ())",
                "SELECT device, amount, SUM(amount) OVER (PARTITION BY device ORDER BY amount NULLS LAST "
                        + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total, "
                        + "LAG(amount) OVER (PARTITION BY device ORDER BY amount NULLS LAST) AS prev_amount FROM "
                        + SOURCE + " WHERE " + bounds,
                "SELECT region, MAX(CAST(9007199254740993 AS BIGINT) + CAST(amount AS BIGINT)) AS exact "
                        + "FROM " + SOURCE + " WHERE " + bounds + " GROUP BY region",
                "SELECT region, SUM(amount) AS value FROM " + SOURCE + " WHERE "
                        + bounds + " AND RAND() >= 0 GROUP BY region"));
        var random = new java.util.Random(SEED);
        IntStream.range(0, 4).forEach(i -> {
            int bound = random.nextInt(HIGH) + 1;
            String base = "SELECT region, amount FROM " + SOURCE + " WHERE ts < " + bound * DAY;
            cases.add("WITH a AS (" + base + "), b AS (SELECT * FROM a UNION ALL SELECT * FROM a) "
                    + "SELECT region, COUNT(amount) AS n FROM b GROUP BY region "
                    + "ORDER BY n DESC, region NULLS FIRST LIMIT 10");
        });
        try (var factory = factory(SCOPE, true)) {
            var entry = factory.executeLogicalQueryUseCase();
            for (String sql : cases) {
                tracked.calls.clear();
                QueryResult answer;
                try {
                    answer = entry.query(sql);
                } catch (Exception candidateError) {
                    saveMismatch(sql, 0, null, candidateError);
                    throw new AssertionError("Cascada failed for " + sql, candidateError);
                }
                check(sql, 0, sql.contains(" LIMIT "), answer, "existing");
                assertThat(answer.exactCubeTrace().reason()).isEqualTo("unproven-sql-shape");
                assertThat(tracked.calls).hasSize(1);
            }
        }
        System.out.println("EXACT_CUBE_DECLINES seed=" + SEED + " cases=" + cases.size());
    }

    @Test void benchmarksFourAndSixShapesFromNormalValkeyBuckets() throws Exception {
        String original = path;
        path = scratch.resolve("large-buckets").toUri().toString();
        int factRows = 1_200_000;
        spark.sql("""
                SELECT pmod(id, 12) * 86400L AS ts, pmod(id, 257) AS region,
                       id AS device, id * 3 AS channel,
                       pmod(id, 17) AS publisher, CAST(pmod(id, 121) - 60 AS INT) AS amount
                FROM range(%d)
                """.formatted(factRows)).write().format("delta").save(path);
        var grains = List.of("region, device", "region, device, channel", "region, device, publisher",
                "region, channel", "region, channel, publisher", "region, device, channel, publisher");
        var defaults = ApplicationSettings.defaults();
        var settings = new ApplicationSettings(defaults.server(), defaults.warming(), defaults.tables(),
                new com.cascada.app.model.config.CacheSafetySettings(Set.of(), Set.of(), null, factRows));
        var json = new ObjectMapper();
        var client = HttpClient.newHttpClient();
        int repetitions = Integer.getInteger("cascada.exact.benchmark.samples", 5);
        try (var factory = factory(SCOPE, true, settings)) {
            var raw = factory.executeLogicalQueryUseCase();
            var receipts = new CopyOnWriteArrayList<QueryResult>();
            var entry = (com.cascada.cache.application.port.in.ExecuteLogicalQueryUseCase) sql -> {
                var result = raw.query(sql);
                receipts.add(result);
                return result;
            };
            try (var http = new QueryHttpServer(TEST_SERVER, entry);
                 var baseline = new QueryHttpServer(TEST_SERVER,
                         sql -> new QueryResult(oracle.execute(pinned(sql, 0)), false))) {
                http.start();
                baseline.start();
                for (int shapes : List.of(4, 6)) {
                    factory.flushCacheUseCase().flushEverything();
                    int daysPerShape = 12 / shapes;
                    for (int index = 0; index < shapes; index++) {
                        int start = index * daysPerShape;
                        int end = start + daysPerShape;
                        warm(factory, aggregate("AVG", grains.get(index), start, end, ""), start, end);
                    }
                    String query = aggregate("AVG", "region", 0, 12, "") + " ORDER BY region NULLS FIRST";
                    var expected = new ByteArrayOutputStream();
                    QueryResponseEncoder.json(oracle.execute(pinned(query, 0)), expected);
                    var expectedJson = json.readTree(expected.toByteArray());
                    String body = json.writeValueAsString(Map.of("sql", query));
                    var cached = request(http.port(), body);
                    var direct = request(baseline.port(), body);
                    var warmup = client.send(cached, HttpResponse.BodyHandlers.ofByteArray());
                    assertThat(warmup.statusCode()).isEqualTo(200);
                    assertThat(json.readTree(warmup.body())).isEqualTo(expectedJson);
                    tracked.calls.clear();
                    receipts.clear();
                    long[] cacheNanos = new long[repetitions];
                    long[] sparkNanos = new long[repetitions];
                    for (int i = 0; i < repetitions; i++) {
                        long started = System.nanoTime();
                        var response = client.send(i % 2 == 0 ? cached : direct, HttpResponse.BodyHandlers.ofByteArray());
                        long elapsed = System.nanoTime() - started;
                        assertThat(response.statusCode()).isEqualTo(200);
                        assertThat(json.readTree(response.body())).isEqualTo(expectedJson);
                        started = System.nanoTime();
                        response = client.send(i % 2 == 0 ? direct : cached, HttpResponse.BodyHandlers.ofByteArray());
                        long second = System.nanoTime() - started;
                        assertThat(response.statusCode()).isEqualTo(200);
                        assertThat(json.readTree(response.body())).isEqualTo(expectedJson);
                        cacheNanos[i] = i % 2 == 0 ? elapsed : second;
                        sparkNanos[i] = i % 2 == 0 ? second : elapsed;
                    }
                    assertThat(tracked.calls).isEmpty();
                    assertThat(receipts).hasSize(repetitions).allSatisfy(result -> {
                        var trace = result.exactCubeTrace();
                        assertThat(trace.route()).isEqualTo("exact-cube");
                        assertThat(trace.fragments()).hasSize(12);
                        assertThat(trace.fragments().stream().map(ExactCubeFragment::grain).distinct().count())
                                .isEqualTo(shapes);
                        assertThat(trace.fragments().stream().mapToLong(ExactCubeFragment::rows).sum())
                                .isEqualTo(factRows);
                        assertThat(trace.residualSql()).isEmpty();
                    });
                    Arrays.sort(cacheNanos);
                    Arrays.sort(sparkNanos);
                    var fragments = receipts.getFirst().exactCubeTrace().fragments();
                    System.out.printf(java.util.Locale.ROOT,
                            "BUCKET_CUBE shapes=%d buckets=12 rows=%d compressed=%d decoded=%d cacheP50=%.3f cacheP95=%.3f sparkP50=%.3f sparkP95=%.3f%n",
                            shapes, factRows, fragments.stream().mapToLong(ExactCubeFragment::compressedBytes).sum(),
                            fragments.stream().mapToLong(ExactCubeFragment::decodedBytes).sum(),
                            percentile(cacheNanos, .50), percentile(cacheNanos, .95),
                            percentile(sparkNanos, .50), percentile(sparkNanos, .95));
                }
            }
        } finally {
            path = original;
        }
    }

    private HttpRequest request(int port, String body) {
        return HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/query"))
                .timeout(Duration.ofSeconds(60)).POST(HttpRequest.BodyPublishers.ofString(body)).build();
    }

    private static double percentile(long[] ordered, double quantile) {
        return ordered[(int) Math.ceil(quantile * ordered.length) - 1] / 1_000_000.0;
    }

    @Test void sqlErrorsRetainTheirSparkCondition() {
        var statements = List.of(
                "SELECT (SELECT amount FROM " + SOURCE + " WHERE ts = 0) AS v",
                "SELECT SUM(CAST(amount AS BIGINT) + CAST(9223372036854775807 AS BIGINT)) AS v "
                        + "FROM " + SOURCE + " WHERE amount > 0");
        try (var factory = factory(SCOPE, true)) {
            for (String sql : statements) {
                String expected = sparkCondition(() -> oracle.execute(pinned(sql, 0)));
                assertThat(sparkCondition(() -> factory.executeLogicalQueryUseCase().query(sql))).isEqualTo(expected);
            }
        }
        System.out.println("EXACT_CUBE_ERROR_PARITY cases=" + statements.size());
    }

    private static String sparkCondition(Runnable call) {
        try {
            call.run();
        } catch (Exception failure) {
            for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
                if (cause instanceof SparkThrowable sqlError) return sqlError.getCondition();
            }
            throw new AssertionError("missing Spark SQL condition", failure);
        }
        throw new AssertionError("expected a Spark SQL error");
    }

    private List<String> warm(CascadaEngineFactory factory, String sql, long low, long high) {
        var catalog = new TableCatalog().register(RegisteredTable.of(SOURCE, path, Map.of("ts", "ts"), "ts"));
        String physical = new LogicalToPhysicalSqlTranslator(300, catalog).translateToPhysicalSql(sql);
        var canonical = new CalciteCanonicalObjectFactory().canonicalize(physical);
        var hash = new com.cascada.cache.application.service.hashing.QueryHashGenerator().generateQueryHash(canonical, 300);
        var warmer = (com.cascada.cache.application.service.WarmingOrchestrator) factory.warmCacheUseCase();
        assertThat(warmer.warmSinglePattern(hash, canonical, low * DAY, high * DAY - 1, false).bucketsWarmed())
                .isEqualTo((int) (high - low));
        return java.util.stream.LongStream.range(low, high).mapToObj(day ->
                com.cascada.cache.domain.key.CacheKeyFactory.buildBucketKey(hash, day * DAY, DAY)).toList();
    }

    private CascadaEngineFactory factory(String scope, boolean enabled) {
        return factory(scope, enabled, ApplicationSettings.defaults());
    }

    private CascadaEngineFactory factory(String scope, boolean enabled, ApplicationSettings application) {
        var settings = new EngineSettings(CacheExecutionConfiguration.defaults(), valkey.uri(), SOURCE, path, true, 8);
        return new CascadaEngineFactory(settings, new LazyQueryExecutor(() -> tracked), application, enabled, scope);
    }

    private void check(String sql, long version, boolean ordered, QueryResult actual, String route) {
        var expected = oracle.execute(pinned(sql, version));
        String message = "seed=" + SEED + " version=" + version + " SQL=" + sql;
        try {
            assertThat(actual.exactCubeTrace() == null ? null : actual.exactCubeTrace().route())
                    .as("%s trace=%s", message, actual.exactCubeTrace()).isEqualTo(route);
            assertThat(actual.frame().columnNames()).as(message).isEqualTo(expected.columnNames());
            assertThat(actual.frame().columnTypes()).as(message).isEqualTo(expected.columnTypes());
            assertThat(actual.frame().rowCount()).as(message).isEqualTo(expected.rowCount());
            if (ordered) {
                for (int i = 0; i < expected.rowCount(); i++) {
                    assertThat(actual.frame().rows().get(i)).as("%s first differing row %d", message, i)
                            .isEqualTo(expected.rows().get(i));
                }
            } else {
                var counts = multiset(actual.frame());
                for (var row : multiset(expected).entrySet()) {
                    assertThat(counts.get(row.getKey())).as("%s row %s", message, row.getKey())
                            .isEqualTo(row.getValue());
                }
            }
        } catch (AssertionError mismatch) {
            saveMismatch(sql, version, actual, mismatch);
            throw mismatch;
        }
    }

    private void saveMismatch(String sql, long version, QueryResult result, Throwable mismatch) {
        try {
            var directory = Path.of(System.getProperty("cascada.exact.mismatch.dir", "target/exact-cube-mismatches"));
            Files.createDirectories(directory);
            var reproduction = new LinkedHashMap<String, Object>();
            reproduction.put("seed", SEED);
            reproduction.put("sql", sql);
            reproduction.put("fixtureSql", fixtureSql);
            reproduction.put("pinnedVersion", version);
            reproduction.put("trace", result == null ? null : result.exactCubeTrace());
            reproduction.put("sparkSubmissions", List.copyOf(tracked.calls));
            reproduction.put("firstDifference", mismatch.getMessage());
            new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(
                    Files.createTempFile(directory, "exact-cube-", ".json").toFile(), reproduction);
        } catch (Exception artifactError) {
            mismatch.addSuppressed(artifactError);
        }
    }

    private static Map<Map<String, Object>, Long> multiset(ResultFrame frame) {
        Map<Map<String, Object>, Long> counts = new HashMap<>();
        for (var row : frame.rows()) counts.merge(row, 1L, Long::sum);
        return counts;
    }

    private String pinned(String sql, long version) {
        return sql.replaceAll("\\b" + SOURCE + "\\b", "delta.`" + path + "` VERSION AS OF " + version);
    }

    private static String aggregate(String operation, String grain, long low, long high, String filter) {
        return "SELECT " + grain + ", " + operation + "(amount) AS value FROM " + SOURCE
                + " WHERE ts >= " + low * DAY + " AND ts < " + high * DAY + filter + " GROUP BY " + grain;
    }

    private static final class TrackingExecutor implements PinnedQueryExecutorPort {
        final List<String> calls = new CopyOnWriteArrayList<>();
        final LongAdder bindCalls = new LongAdder();
        final AtomicReference<Runnable> beforePinned = new AtomicReference<>();
        final AtomicReference<Runnable> afterBind = new AtomicReference<>();
        private final SparkDeltaQueryExecutor spark;

        TrackingExecutor(SparkDeltaQueryExecutor spark) {
            this.spark = spark;
        }

        @Override public Optional<ExactCubeSource> bind(String path, String scope) {
            bindCalls.increment();
            var source = spark.bind(path, scope);
            Runnable hook = afterBind.getAndSet(null);
            if (hook != null) hook.run();
            return source;
        }

        @Override public ResultFrame executePinned(ExactCubeSource source, String sql) {
            trackPinned(sql);
            return spark.executePinned(source, sql);
        }

        @Override public ResultFrame executeWithAggregate(ExactCubeSource source, String sql, String relation,
                                                          ResultFrame frame, List<String> types) {
            trackPinned(sql);
            return spark.executeWithAggregate(source, sql, relation, frame, types);
        }

        private void trackPinned(String sql) {
            Runnable hook = beforePinned.getAndSet(null);
            if (hook != null) hook.run();
            calls.add(sql);
        }

        @Override public ResultFrame execute(String sql) {
            calls.add(sql);
            return spark.execute(sql);
        }
    }
}
