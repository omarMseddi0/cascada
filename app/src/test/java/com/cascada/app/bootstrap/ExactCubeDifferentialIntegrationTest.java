package com.cascada.app.bootstrap;

import com.cascada.cache.adapter.out.cache.ValkeyCacheBackendAdapter;
import com.cascada.cache.application.port.out.ExactCubeStorePort;
import com.cascada.cache.application.service.ExecuteLogicalQueryService;
import com.cascada.cache.application.service.cube.ExactCubeExecution;
import com.cascada.cache.domain.model.cube.CubeInterval;
import com.cascada.cache.domain.model.cube.CubeMeasure;
import com.cascada.cache.domain.model.cube.CubePopulation;
import com.cascada.sql.adapter.out.calcite.ExactCubeSqlAdapter;
import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
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

    @Test void adversarialSqlAcrossColdPartialAndFullValkeyLayouts() {
        String originalPath = path;
        String originalFixture = fixtureSql;
        path = scratch.resolve("adversarial-facts").toUri().toString();
        fixtureSql = """
                WITH samples AS (
                    SELECT pmod(id * 13 + %d, 12) AS day_no, id,
                           CASE pmod(id, 11)
                             WHEN 0 THEN NULL WHEN 1 THEN 'NULL' WHEN 2 THEN ''
                             WHEN 3 THEN '😀' WHEN 4 THEN '' WHEN 5 THEN 'é'
                             WHEN 6 THEN concat('e', chr(769)) ELSE concat('r', pmod(id, 17))
                           END AS region,
                           CASE pmod(id DIV 12, 4) WHEN 0 THEN 'x' WHEN 1 THEN 'y'
                                WHEN 2 THEN NULL ELSE '' END AS device,
                           CASE pmod(id DIV 12, 8) WHEN 0 THEN NULL WHEN 1 THEN -2147483648
                                WHEN 2 THEN 2147483647 WHEN 3 THEN 0 ELSE CAST(pmod(id, 127) - 63 AS INT)
                           END AS amount
                    FROM range(720)
                ), facts AS (
                    SELECT day_no * 86400L + pmod(id, 7) AS ts, region, device, amount,
                           pmod(id, 7) AS shop, pmod(id, 3) AS market
                    FROM samples WHERE day_no NOT IN (3, 9)
                )
                SELECT * FROM facts UNION ALL SELECT * FROM facts WHERE pmod(ts, 3) = 0
                """.formatted(SEED);
        spark.sql(fixtureSql).write().format("delta").save(path);
        var cases = CubeAdversarialSql.cases(SOURCE, DAY);
        assertThat(cases).hasSize(256);
        var expected = new HashMap<String, ResultFrame>();
        var failures = new ArrayList<AssertionError>();
        var passed = new LinkedHashMap<String, Integer>();

        try (var factory = factory(SCOPE, true)) {
            var entry = factory.executeLogicalQueryUseCase();
            for (String layout : List.of("cold", "partial", "full")) {
                if (layout.equals("partial")) {
                    warm(factory, aggregate("AVG", "region, device, shop", 0, 5, ""), 0, 5);
                    warm(factory, aggregate("AVG", "region, device, shop, market", 7, 12, ""), 7, 12);
                } else if (layout.equals("full")) {
                    warm(factory, aggregate("AVG", "region, device, market", 5, 7, ""), 5, 7);
                }
                int verified = 0;
                for (var test : cases.entrySet()) {
                    String sql = test.getKey();
                    tracked.calls.clear();
                    QueryResult actual = null;
                    try {
                        var sparkResult = expected.computeIfAbsent(sql, statement -> oracle.execute(pinned(statement, 0)));
                        actual = entry.query(sql);
                        String route = layout.equals("cold") ? "existing" : "exact-cube";
                        assertThat(actual.exactCubeTrace().route()).isEqualTo(route);
                        assertSameFrame(sparkResult, actual.frame(), test.getValue(), layout + " " + sql);
                        if (!layout.equals("cold")) {
                            var trace = actual.exactCubeTrace();
                            assertThat(trace.version()).isZero();
                            assertThat(trace.fragments()).isNotEmpty();
                            if (layout.equals("partial")) {
                                assertThat(trace.residualSql()).singleElement().asString()
                                        .contains("VERSION AS OF 0", "`ts` >= 432000", "`ts` < 604800");
                            } else {
                                assertThat(trace.residualSql()).isEmpty();
                            }
                            var statements = new ArrayList<>(trace.residualSql());
                            if (trace.outerSql() != null) statements.add(trace.outerSql());
                            assertThat(tracked.calls).containsExactlyElementsOf(statements);
                        }
                        verified++;
                    } catch (Exception | AssertionError failure) {
                        saveMismatch(sql, 0, actual, new AssertionError("layout=" + layout + " " + failure, failure));
                        failures.add(new AssertionError(layout + " " + sql, failure));
                    }
                }
                passed.put(layout, verified);
            }
        } finally {
            path = originalPath;
            fixtureSql = originalFixture;
        }
        System.out.printf("ADVERSARIAL_SQL seed=%d distinct=%d passed=%s failures=%d%n",
                SEED, cases.size(), passed, failures.size());
        if (!failures.isEmpty()) {
            var summary = new AssertionError(failures.size() + " adversarial comparisons failed");
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

    @Test void thirtyDayTopKUsesLookbackBucketsThenOtherShapesThenOnlyTheRemainingSparkGap() {
        String original = path;
        path = scratch.resolve("thirty-day-dashboard").toUri().toString();
        spark.sql("""
                SELECT day.id * 86400L AS ts, 'x' AS device, subscriber.id AS subscriber_id,
                       pmod(subscriber.id, 3) AS shop, pmod(day.id, 2) AS market,
                       CASE WHEN subscriber.id < 10 THEN CAST(60 - subscriber.id AS INT)
                            WHEN (subscriber.id - 10) DIV 10 = day.id THEN 100 ELSE 0 END AS amount
                FROM range(30) day CROSS JOIN range(310) subscriber
                UNION ALL
                SELECT 0L, 'y', 0L, 0L, 0L, 900
                UNION ALL
                SELECT 5L * 86400L, 'y', 0L, 0L, 0L, 0 FROM range(9)
                """).write().format("delta").save(path);
        String query = aggregate("AVG", "device, subscriber_id", 0, 30, "")
                + " ORDER BY value DESC, device NULLS FIRST, subscriber_id NULLS FIRST LIMIT 10";
        try (var factory = factory(SCOPE, true)) {
            // Verify the ordinary warmer writes the fifteen most recent days for the same query hash.
            var canonical = canonicalize(query);
            var hash = new com.cascada.cache.application.service.hashing.QueryHashGenerator().generateQueryHash(canonical, 300);
            var warmer = factory.warmCacheUseCase();
            warmer.recordQuery(hash, canonical);
            assertThat(warmer.warmLookbackWindow(30 * DAY, Duration.ofDays(15)).bucketsWarmed()).isEqualTo(15);
            var sameQueryKeys = java.util.stream.LongStream.range(15, 30).mapToObj(day ->
                    com.cascada.cache.domain.key.CacheKeyFactory.buildBucketKey(hash, day * DAY, DAY)).toList();
            factory.cacheBackendForShutdown().visitKeys(sameQueryKeys, (index, bucket) ->
                    assertThat(bucket).hasValueSatisfying(frame -> assertThat(frame.rowCount()).isEqualTo(310)));

            var expectedKeys = new ArrayList<>(sameQueryKeys);
            expectedKeys.addAll(warm(factory, aggregate("AVG", "device, subscriber_id, shop", 0, 5, ""), 0, 5));
            var subscriberOnlyKeys = warm(factory, aggregate("AVG", "subscriber_id", 5, 15, ""), 5, 15);

            // The best global x group is never in a daily top ten.
            String localRanks = "WITH daily AS (SELECT ts, device, subscriber_id, AVG(amount) AS mean FROM "
                    + SOURCE + " GROUP BY ts, device, subscriber_id), ranked AS (SELECT *, ROW_NUMBER() "
                    + "OVER (PARTITION BY ts ORDER BY mean DESC, device, subscriber_id) AS pos FROM daily) "
                    + "SELECT ts FROM ranked WHERE device = 'x' AND subscriber_id = 0 AND pos <= 10";
            assertThat(oracle.execute(pinned(localRanks, 0)).isEmpty()).isTrue();

            var entry = factory.executeLogicalQueryUseCase();
            tracked.calls.clear();
            var mixed = entry.query(query);
            check(query, 0, true, mixed, "exact-cube");
            assertThat(mixed.exactCubeTrace().fragments()).extracting(ExactCubeFragment::bucketKey)
                    .containsExactlyInAnyOrderElementsOf(expectedKeys).doesNotContainAnyElementsOf(subscriberOnlyKeys);
            assertThat(mixed.exactCubeTrace().residualSql()).singleElement().asString()
                    .contains("VERSION AS OF 0", "`ts` >= " + 5 * DAY, "`ts` < " + 15 * DAY,
                            "SUM(`amount`)", "COUNT(`amount`)", "GROUP BY `device`, `subscriber_id`")
                    .doesNotContain("ORDER BY", "LIMIT");
            assertThat(tracked.calls).containsExactlyElementsOf(mixed.exactCubeTrace().residualSql());
            assertThat(mixed.frame().rows().getFirst()).containsEntry("device", "y").containsEntry("value", 90.0);
            assertThat(mixed.frame().rows().get(1)).containsEntry("device", "x")
                    .containsEntry("subscriber_id", 0L).containsEntry("value", 60.0);

            expectedKeys.addAll(warm(factory, aggregate("AVG", "device, subscriber_id, shop, market", 5, 15, ""), 5, 15));
            tracked.calls.clear();
            var covered = entry.query(query);
            check(query, 0, true, covered, "exact-cube");
            assertThat(covered.exactCubeTrace().fragments()).extracting(ExactCubeFragment::bucketKey)
                    .containsExactlyInAnyOrderElementsOf(expectedKeys).doesNotContainAnyElementsOf(subscriberOnlyKeys);
            assertThat(covered.exactCubeTrace().residualSql()).isEmpty();
            assertThat(tracked.calls).isEmpty();
            System.out.println("THIRTY_DAY_CUBE own=15 mixedOther=5 mixedSparkDays=10 fullOther=15 fullSparkSql=0");
        } finally {
            path = original;
        }
    }

    @Test void concurrentReadersRejectABucketReplacedAfterPlanningWhileDeltaAdvances() throws Exception {
        String original = path;
        path = scratch.resolve("concurrent-bucket-fault").toUri().toString();
        spark.sql(fixtureSql).write().format("delta").save(path);
        String aggregate = aggregate("AVG", "region", 0, HIGH, "");
        var queries = List.of(aggregate + " ORDER BY region NULLS FIRST",
                aggregate + " ORDER BY value DESC NULLS LAST, region NULLS LAST LIMIT 4",
                "WITH q AS (" + aggregate + ") SELECT region, value FROM q WHERE value IS NOT NULL",
                "SELECT region, SUM(value) AS amount FROM (" + aggregate + ") q GROUP BY region");
        var expected = new ArrayList<ResultFrame>();
        for (String sql : queries) expected.add(oracle.execute(pinned(sql, 0)));

        try (var factory = factory(SCOPE, true);
             var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var keys = warm(factory, aggregate("AVG", "region, device", 0, HIGH, ""), 0, HIGH);
            String changedKey = keys.getFirst();
            var backend = (ValkeyCacheBackendAdapter) factory.cacheBackendForShutdown();
            var delegate = backend.exactCubeStore();
            var allPlanned = new CompletableFuture<Void>();
            var releaseReads = new CompletableFuture<Void>();
            var planned = new AtomicInteger();
            var gatedStore = new ExactCubeStorePort() {
                @Override
                public List<ExactCubeFragment> candidates(CubePopulation population,
                        CubeInterval interval,
                        List<String> grain, Set<CubeMeasure> measures) {
                    return delegate.candidates(population, interval, grain, measures);
                }

                @Override
                public void visitBuckets(List<ExactCubeFragment> manifests,
                        BiConsumer<Integer, Optional<ResultFrame>> visitor) {
                    assertThat(manifests).extracting(ExactCubeFragment::bucketKey).contains(changedKey);
                    if (planned.incrementAndGet() == queries.size()) allPlanned.complete(null);
                    releaseReads.join();
                    delegate.visitBuckets(manifests, visitor);
                }

                @Override
                public boolean storeBucket(String key, ResultFrame frame, Duration ttl, String generation,
                        CubePopulation population,
                        CubeInterval interval, List<String> grain, String averageAlias) {
                    throw new AssertionError("The query route must not publish buckets");
                }
            };
            var catalog = new TableCatalog().register(RegisteredTable.of(SOURCE, path, Map.of("ts", "ts"), "ts"));
            var exact = new ExactCubeExecution(
                    new ExactCubeSqlAdapter(Set.of(path), "ts"),
                    gatedStore, tracked, SCOPE, Clock.systemUTC(), DAY,
                    CacheExecutionConfiguration.defaults().mergeWorkerCount(), workers);
            var entry = new ExecuteLogicalQueryService(
                    new LogicalToPhysicalSqlTranslator(300, catalog), new CalciteCanonicalObjectFactory(),
                    factory.executeCachedQueryUseCase(), tracked, exact);
            tracked.calls.clear();
            var requests = new ArrayList<CompletableFuture<QueryResult>>();
            for (String sql : queries) {
                requests.add(CompletableFuture.supplyAsync(() -> entry.query(sql), workers)
                        .whenComplete((result, failure) -> {
                            if (!allPlanned.isDone()) {
                                allPlanned.completeExceptionally(new AssertionError(
                                        "Request exited before all readers reached the selected buckets", failure));
                            }
                        }));
            }
            try {
                allPlanned.join();
                spark.newSession().sql("INSERT INTO delta.`" + path
                        + "` VALUES (0L, 'added-during-read', 'x', 1000)").collectAsList();
                backend.store(changedKey,
                        ResultFrame.builder().column("replacement", ColumnType.LONG).row(99L).build(), Duration.ofHours(1));
            } finally {
                releaseReads.complete(null);
            }
            for (int i = 0; i < requests.size(); i++) {
                var actual = requests.get(i).get();
                assertThat(actual.exactCubeTrace().route()).isEqualTo("pinned-spark");
                assertThat(actual.exactCubeTrace().version()).isZero();
                assertThat(actual.exactCubeTrace().reason()).startsWith("recovery:");
                assertSameFrame(expected.get(i), actual.frame(), i < 2, queries.get(i));
            }
            assertThat(tracked.calls).hasSize(queries.size()).allSatisfy(sql -> assertThat(sql).contains("VERSION AS OF 0"));
            assertThat(backend.multiGet(List.of(changedKey)).getFirst()).hasValueSatisfying(
                    frame -> assertThat(frame.columnNames()).containsExactly("replacement"));
            assertThat(factory.executeLogicalQueryUseCase().query(queries.getFirst()).frame().rows())
                    .anySatisfy(row -> assertThat(row).containsEntry("region", "added-during-read"));
        } finally {
            path = original;
        }
    }

    @Test void binaryDistinctStringsThatDecodeToTheSameJavaTextMustStayDistinct() {
        String originalPath = path;
        String originalFixture = fixtureSql;
        path = scratch.resolve("malformed-utf8-facts").toUri().toString();
        fixtureSql = """
                SELECT 0L AS ts, CAST(X'FF' AS STRING) AS region, 'x' AS device, 1 AS amount
                UNION ALL
                SELECT 0L, '�', 'x', 9
                UNION ALL
                SELECT 86400L, '�', 'x', 7
                """;
        try {
            spark.sql(fixtureSql).write().format("delta").save(path);
            assertThat(oracle.execute(pinned("SELECT DISTINCT hex(CAST(region AS BINARY)) AS bytes FROM " + SOURCE, 0))
                    .rows()).extracting(row -> row.get("bytes")).containsExactlyInAnyOrder("FF", "EFBFBD");
            String query = aggregate("AVG", "region", 0, 1, "");
            String producer = aggregate("AVG", "region, device", 0, 1, "");
            try (var factory = factory(SCOPE, true)) {
                var canonical = canonicalize(producer);
                var hash = new com.cascada.cache.application.service.hashing.QueryHashGenerator().generateQueryHash(canonical, 300);
                var warmer = (com.cascada.cache.application.service.WarmingOrchestrator) factory.warmCacheUseCase();
                var report = warmer.warmSinglePattern(hash, canonical, 0, DAY - 1, false);
                assertThat(report.bucketsWarmed()).isZero();
                assertThat(report.bucketsSkipped()).isEqualTo(1);
                String key = com.cascada.cache.domain.key.CacheKeyFactory.buildBucketKey(hash, 0, DAY);
                assertThat(factory.cacheBackendForShutdown().existsForKeys(List.of(key))).containsExactly(false);
                var expected = oracle.execute(pinned(query, 0));
                assertThat(expected.rowCount()).isEqualTo(2);
                assertThat(expected.hasLossyStrings()).isTrue();
                var legal = oracle.execute(pinned("SELECT region FROM " + SOURCE
                        + " WHERE ts = 0 AND hex(CAST(region AS BINARY)) = 'EFBFBD'", 0));
                assertThat(legal.rowCount()).isEqualTo(1);
                assertThat(legal.hasLossyStrings()).isFalse();

                var actual = factory.executeLogicalQueryUseCase().query(query);
                assertThat(actual.exactCubeTrace().route()).isEqualTo("existing");
                assertSameFrame(expected, actual.frame(), false, query);

                String twoDayProducer = aggregate("AVG", "region, device", 0, 2, "");
                warm(factory, twoDayProducer, 1, 2);
                String mixedQuery = aggregate("AVG", "region", 0, 2, "");
                var mixed = factory.executeLogicalQueryUseCase().query(mixedQuery);
                check(mixedQuery, 0, false, mixed, "pinned-spark");
                assertThat(mixed.exactCubeTrace().residualSql()).hasSize(1);
                assertThat(mixed.exactCubeTrace().reason()).startsWith("recovery:");

                // The ordinary bucket merge also encounters the lossy grouping keys in its gap.
                try (var ordinary = factory(SCOPE, false)) {
                    check(twoDayProducer, 0, false, ordinary.executeLogicalQueryUseCase().query(twoDayProducer), null);
                }
            }
        } finally {
            path = originalPath;
            fixtureSql = originalFixture;
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

    private com.cascada.cache.domain.model.query.CanonicalQueryObject canonicalize(String sql) {
        var catalog = new TableCatalog().register(RegisteredTable.of(SOURCE, path, Map.of("ts", "ts"), "ts"));
        String physical = new LogicalToPhysicalSqlTranslator(300, catalog).translateToPhysicalSql(sql);
        return new CalciteCanonicalObjectFactory().canonicalize(physical);
    }

    private List<String> warm(CascadaEngineFactory factory, String sql, long low, long high) {
        var canonical = canonicalize(sql);
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
            assertSameFrame(expected, actual.frame(), ordered, message);
        } catch (AssertionError mismatch) {
            saveMismatch(sql, version, actual, mismatch);
            throw mismatch;
        }
    }

    private static void assertSameFrame(ResultFrame expected, ResultFrame actual, boolean ordered, String context) {
        assertThat(actual.columnNames()).as(context).isEqualTo(expected.columnNames());
        assertThat(actual.columnTypes()).as(context).isEqualTo(expected.columnTypes());
        assertThat(actual.rowCount()).as(context).isEqualTo(expected.rowCount());
        if (ordered) {
            assertThat(actual.rows()).as(context).containsExactlyElementsOf(expected.rows());
        } else {
            assertThat(multiset(actual)).as(context).isEqualTo(multiset(expected));
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
