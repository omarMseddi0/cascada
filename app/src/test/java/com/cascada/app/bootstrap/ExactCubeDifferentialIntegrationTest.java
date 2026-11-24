package com.cascada.app.bootstrap;

import com.cascada.app.config.CascadaEngineFactory;
import com.cascada.app.adapter.out.execution.LazyQueryExecutor;
import com.cascada.app.adapter.in.api.QueryHttpServer;
import com.cascada.app.adapter.in.api.QueryResponseEncoder;
import com.cascada.app.adapter.in.api.QueryGrpcService;
import com.cascada.app.adapter.in.grpc.protocol.QueryRequest;
import com.cascada.app.adapter.in.grpc.protocol.QueryServiceGrpc;
import com.cascada.app.adapter.in.grpc.protocol.ResponseFormat;
import com.cascada.app.model.config.ServerSettings;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.lettuce.core.RedisClient;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import com.cascada.app.model.config.ApplicationSettings;
import com.cascada.app.model.config.EngineSettings;
import com.cascada.cache.adapter.out.cache.ValkeyCacheBackendAdapter;
import com.cascada.cache.application.port.out.ExactCubeStorePort;
import com.cascada.cache.application.service.ExecuteLogicalQueryService;
import com.cascada.cache.application.service.cube.ExactCubeExecution;
import com.cascada.cache.domain.model.cube.CubeInterval;
import com.cascada.cache.domain.model.cube.CubeMeasure;
import com.cascada.cache.domain.model.cube.CubePopulation;
import com.cascada.cache.domain.model.cube.ExactCubeFragment;
import com.cascada.sql.adapter.out.calcite.CalciteCanonicalObjectFactory;
import com.cascada.sql.adapter.out.calcite.ExactCubeSqlAdapter;
import com.cascada.sql.adapter.out.calcite.LogicalToPhysicalSqlTranslator;
import com.cascada.sql.domain.TableCatalog;
import com.cascada.sql.domain.model.RegisteredTable;
import java.time.Clock;
import java.util.Set;
import com.cascada.cache.application.model.config.CacheExecutionConfiguration;
import com.cascada.cache.application.model.query.QueryResult;
import com.cascada.cache.application.port.out.LatestQueryExecutorPort;
import com.cascada.cache.domain.frame.ResultFrame;
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

/** Exercises the production logical-entry composition root with independent latest-version Spark SQL. */
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

    @Test void emptyGroupedAndScalarResultsHaveDifferentPresence() {
        String sum = aggregate("SUM", "region, device", 12, 13, "");
        String count = aggregate("COUNT", "region", 12, 13, "");
        String grouped = aggregate("AVG", "region", 12, 13, "");
        String scalar = "SELECT AVG(amount) AS mean FROM " + SOURCE + " WHERE ts >= " + 12 * DAY
                + " AND ts < " + 13 * DAY;
        try (var factory = factory(SCOPE, true)) {
            var entry = factory.executeLogicalQueryUseCase();
            check(sum, 0, false, entry.query(sum), "latest-build");
            check(count, 0, false, entry.query(count), "latest-build");
            tracked.calls.clear();
            var none = entry.query(grouped);
            check(grouped, 0, false, none, "exact-cube");
            assertThat(none.frame().rowCount()).isZero();
            var emptyAverage = entry.query(scalar);
            check(scalar, 0, false, emptyAverage, "exact-cube");
            assertThat(emptyAverage.frame().rowCount()).isEqualTo(1);
            assertThat(emptyAverage.frame().isNullAt(0, 0)).isTrue();
            assertThat(tracked.calls).isEmpty();
        }
        System.out.println("EXACT_CUBE_EMPTY groupedRows=0 scalarRows=1 sparkSubmissions=0");
    }

    @Test void exactCompositionThroughOrdinarySql() {
        String sum = aggregate("SUM", "region, device", 0, 6, "");
        String count = aggregate("COUNT", "region", 0, HIGH, "")
                .replace("ts < " + HIGH * DAY, "ts <= " + (HIGH * DAY - 1));
        String avg = aggregate("AVG", "region", 0, HIGH, "") + " ORDER BY value DESC NULLS LAST, region NULLS FIRST LIMIT 10";
        String full = aggregate("AVG", "region", 0, 6, "") + " ORDER BY region NULLS FIRST";
        String outer = "SELECT COUNT(*) AS group_count, SUM(value) AS total FROM ("
                + aggregate("SUM", "region", 0, 6, "")
                + ") q WHERE q.value IS NOT NULL HAVING COUNT(*) > 0";
        try (var factory = factory(SCOPE, true)) {
            var entry = factory.executeLogicalQueryUseCase();
            check(sum, 0, false, entry.query(sum), "latest-build");
            check(count, 0, false, entry.query(count), "latest-build");

            tracked.calls.clear();
            var result = entry.query(avg);
            check(avg, 0, true, result, "exact-cube");
            var trace = result.exactCubeTrace();
            assertThat(trace.fragments()).hasSize(2);
            assertThat(trace.fragments()).anySatisfy(fragment -> {
                assertThat(fragment.grain()).containsExactly("region", "device");
                assertThat(fragment.interval().end()).isEqualTo(6 * DAY);
                assertThat(fragment.measures()).extracting(Enum::name).containsExactly("SUM");
            }).anySatisfy(fragment -> {
                assertThat(fragment.grain()).containsExactly("region");
                assertThat(fragment.interval().end()).isEqualTo(HIGH * DAY);
                assertThat(fragment.measures()).extracting(Enum::name).containsExactly("COUNT");
            });
            assertThat(trace.residualSql()).hasSize(1);
            String residual = trace.residualSql().getFirst();
            assertThat(residual).contains("SUM(`amount`)", "`ts` >= 518400", "`ts` < 777600")
                    .doesNotContain("VERSION AS OF")
                    .doesNotContain("COUNT(`amount`)");
            assertThat(tracked.calls).containsExactly(residual);
            System.out.println("EXACT_CUBE_MIXED fragments=" + trace.fragments().stream().map(fragment ->
                    fragment.generation() + ":" + fragment.grain() + ":" + fragment.measures() + ":" + fragment.interval()).toList()
                    + " residual=" + residual + " compressedBytes="
                    + trace.fragments().stream().mapToInt(fragment -> fragment.compressedBytes()).sum()
                    + " decodedBytes=" + trace.fragments().stream().mapToInt(fragment -> fragment.decodedBytes()).sum()
                    + " planningNanos=" + trace.planningNanos());

            String countHead = aggregate("COUNT", "region", 0, 6, "");
            check(countHead, 0, false, entry.query(countHead), "latest-build");
            tracked.calls.clear();
            check(full, 0, true, entry.query(full), "exact-cube");
            assertThat(tracked.calls).isEmpty();

            tracked.calls.clear();
            var compound = entry.query(outer);
            assertThat(compound.exactCubeTrace().route()).as("Spark submissions %s", tracked.calls).isEqualTo("exact-cube");
            check(outer, 0, true, compound, "exact-cube");
            assertThat(compound.exactCubeTrace().fragments()).hasSize(1);
            assertThat(compound.exactCubeTrace().residualSql()).isEmpty();
            assertThat(tracked.calls).containsExactly(compound.exactCubeTrace().outerSql());
            assertThat(tracked.calls.getFirst()).contains("VALUES", "HAVING", "CAST(X'")
                    .doesNotContain("VERSION AS OF");
            System.out.println("EXACT_CUBE_OUTER route=exact-cube sourceScans=0 outerSql=" + tracked.calls.getFirst());
        }
        try (var restarted = factory(SCOPE, true); var isolated = factory("another-policy", true);
             var disabled = factory(SCOPE, false)) {
            check(avg, 0, true, restarted.executeLogicalQueryUseCase().query(avg), "exact-cube");
            check(avg, 0, true, isolated.executeLogicalQueryUseCase().query(avg), "latest-spark");
            check(avg, 0, true, disabled.executeLogicalQueryUseCase().query(avg), null);
        }
    }

    @Test void seededCompoundDeclinesStillMatchLatestSpark() {
        org.junit.jupiter.api.Assumptions.assumeTrue(Long.getLong("cascada.exact.fixture.rows", 64) <= 10000,
                "exhaustive join corpus uses the tiny fixture");
        var bounds = "ts >= 0 AND ts < " + HIGH * DAY;
        var small = "SELECT region, amount FROM " + SOURCE + " WHERE " + bounds + " AND ts < " + 2 * DAY;
        var grouped = aggregate("SUM", "region", 0, HIGH, "");
        var cases = new ArrayList<>(List.of(
                "WITH a AS (" + grouped + "), b AS (SELECT a.region, a.value FROM a JOIN a z ON a.region = z.region) "
                        + "SELECT region, value, RANK() OVER (ORDER BY value DESC) AS pos FROM b",
                "SELECT a.region, COUNT(*) AS n FROM (" + small + ") a LEFT JOIN (" + small
                        + ") b ON a.region <=> b.region WHERE b.amount IS NULL GROUP BY a.region",
                "SELECT region, amount FROM (" + small + " UNION ALL " + small + ") q EXCEPT ALL " + small,
                "SELECT q.region, SUM(q.value) AS total FROM (" + grouped + ") q WHERE EXISTS "
                        + "(SELECT 1 FROM " + SOURCE + " e WHERE q.region = e.region AND e.amount IS NOT NULL) GROUP BY q.region",
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

    @Test void transportLastByteAndConcurrentFullCoverage() throws Exception {
        var server = new ServerSettings(0, 0, Duration.ofSeconds(60));
        var json = new ObjectMapper();
        var client = HttpClient.newHttpClient();
        String query = aggregate("AVG", "region", 0, HIGH, "") + " ORDER BY value DESC NULLS LAST, region NULLS FIRST LIMIT 10";
        String sum = aggregate("SUM", "region, device", 0, 6, "");
        String count = aggregate("COUNT", "region", 0, HIGH, "");
        String sumAll = aggregate("SUM", "region", 0, HIGH, "");
        var expected = new ByteArrayOutputStream();
        QueryResponseEncoder.json(oracle.execute(latestSql(query)), expected);
        var expectedJson = json.readTree(expected.toByteArray());

        try (var factory = factory(SCOPE, true)) {
            var raw = factory.executeLogicalQueryUseCase();
            var receipts = new CopyOnWriteArrayList<QueryResult>();
            var entry = (com.cascada.cache.application.port.in.ExecuteLogicalQueryUseCase) sql -> {
                var result = raw.query(sql);
                receipts.add(result);
                return result;
            };
            try (var http = new QueryHttpServer(server, entry);
                 var direct = new QueryHttpServer(server, sql -> new QueryResult(oracle.execute(latestSql(sql)), false));
                 var grpcService = new QueryGrpcService(entry, server);
                 var threads = Executors.newFixedThreadPool(2)) {
                http.start();
                direct.start();
                var grpc = ServerBuilder.forPort(0).addService(grpcService).build().start();
                var channel = ManagedChannelBuilder.forAddress("127.0.0.1", grpc.getPort()).usePlaintext().build();
                try {
                    var body = json.writeValueAsString(java.util.Map.of("sql", query));
                    var queryRequest = request(http.port(), body);
                    var directRequest = request(direct.port(), body);
                    // One no-cache request also warms the HTTP connection and Spark code generation.
                    assertThat(json.readTree(client.send(queryRequest, HttpResponse.BodyHandlers.ofByteArray()).body()))
                            .isEqualTo(expectedJson);
                    sample("latest-spark", client, directRequest, expectedJson, json, null);
                    sample("cold", client, queryRequest, expectedJson, json, receipts);
                    long startBuild = System.nanoTime();
                    raw.query(count);
                    long buildNanos = System.nanoTime() - startBuild;
                    sample("no-benefit-count-only", client, queryRequest, expectedJson, json, receipts);
                    startBuild = System.nanoTime();
                    raw.query(sum);
                    buildNanos += System.nanoTime() - startBuild;
                    System.out.println("EXACT_CUBE_BUILD elapsedMs=" + buildNanos / 1_000_000.0);
                    sample("mixed", client, queryRequest, expectedJson, json, receipts);

                    // Replace the scoped generations with two independently published full covers.
                    factory.flushCacheUseCase().flushEverything();
                    raw.query(sumAll);
                    raw.query(count);
                    sample("fully-covered", client, queryRequest, expectedJson, json, receipts);
                    tracked.calls.clear();
                    receipts.clear();
                    var httpCall = threads.submit(() -> client.send(queryRequest, HttpResponse.BodyHandlers.ofByteArray()).body());
                    var grpcCall = threads.submit(() -> {
                        var chunks = QueryServiceGrpc.newBlockingStub(channel).withDeadlineAfter(60, TimeUnit.SECONDS)
                                .query(QueryRequest.newBuilder().setSql(query).setFormat(ResponseFormat.JSON).build());
                        var output = new ByteArrayOutputStream();
                        while (chunks.hasNext()) chunks.next().getPayload().writeTo(output);
                        return output.toByteArray();
                    });
                    assertThat(json.readTree(httpCall.get(60, TimeUnit.SECONDS))).isEqualTo(expectedJson);
                    assertThat(json.readTree(grpcCall.get(60, TimeUnit.SECONDS))).isEqualTo(expectedJson);
                    assertThat(receipts).hasSize(2).allSatisfy(result -> {
                        assertThat(result.exactCubeTrace().route()).isEqualTo("exact-cube");
                        assertThat(result.exactCubeTrace().fragments()).hasSize(2);
                    });
                    assertThat(tracked.calls).isEmpty();
                    System.out.println("EXACT_CUBE_CONCURRENT http=1 grpc=1 sparkSubmissions=0");
                } finally {
                    channel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
                    grpc.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
                }
            }
        }
    }

    private HttpRequest request(int port, String body) {
        return HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/query"))
                .timeout(Duration.ofSeconds(60)).POST(HttpRequest.BodyPublishers.ofString(body)).build();
    }

    private void sample(String route, HttpClient client, HttpRequest request,
                        com.fasterxml.jackson.databind.JsonNode expected, ObjectMapper json,
                        List<QueryResult> receipts) throws Exception {
        int repetitions = Integer.getInteger("cascada.exact.benchmark.samples", 5);
        long[] nanos = new long[repetitions];
        int bytes = 0;
        tracked.calls.clear();
        if (receipts != null) receipts.clear();
        spark.sparkContext().listenerBus().waitUntilEmpty(10_000);
        long beforeJobs = sparkJobs.sum();
        long beforeBytes = inputBytes.sum();
        for (int i = 0; i < repetitions; i++) {
            long started = System.nanoTime();
            var response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
            nanos[i] = System.nanoTime() - started;
            assertThat(response.statusCode()).as("HTTP " + route + ": " + new String(response.body(),
                    java.nio.charset.StandardCharsets.UTF_8)).isEqualTo(200);
            assertThat(json.readTree(response.body())).isEqualTo(expected);
            bytes = response.body().length;
        }
        spark.sparkContext().listenerBus().waitUntilEmpty(10_000);
        long jobs = sparkJobs.sum() - beforeJobs;
        long dataRead = inputBytes.sum() - beforeBytes;
        Arrays.sort(nanos);
        var traces = receipts == null ? List.<com.cascada.cache.application.model.query.ExactCubeTrace>of()
                : receipts.stream().map(QueryResult::exactCubeTrace).toList();
        if (receipts != null) {
            String expectedRoute = route.equals("mixed") || route.equals("fully-covered") ? "exact-cube" : "latest-spark";
            assertThat(traces).hasSize(repetitions).allSatisfy(trace -> assertThat(trace.route()).isEqualTo(expectedRoute));
            assertThat(tracked.calls).hasSize(route.equals("fully-covered") ? 0 : repetitions);
            if (route.equals("fully-covered")) assertThat(jobs).isZero();
        }
        var manifests = traces.stream().flatMap(trace -> trace.fragments().stream()).distinct().toList();
        System.out.printf(java.util.Locale.ROOT,
                "EXACT_CUBE_HTTP route=%s n=%d outputBytes=%d SQL=%d jobs=%d inputBytes=%d compressed=%d decoded=%d "
                        + "planningMs=%.3f p50=%.3f p95=%.3f p99=%.3f%n",
                route, repetitions, bytes, receipts == null ? repetitions : tracked.calls.size(), jobs, dataRead,
                manifests.stream().mapToLong(fragment -> fragment.compressedBytes()).sum(),
                manifests.stream().mapToLong(fragment -> fragment.decodedBytes()).sum(),
                traces.stream().mapToLong(trace -> trace.planningNanos()).average().orElse(0) / 1_000_000,
                percentile(nanos, .50), percentile(nanos, .95), percentile(nanos, .99));
    }

    private static double percentile(long[] ordered, double quantile) {
        return ordered[(int) Math.ceil(quantile * ordered.length) - 1] / 1_000_000.0;
    }

    @Test void writerDuringBuildAndFullyCachedReadRestartsAgainstLatest() {
        String original = path;
        path = scratch.resolve("freshness-facts").toUri().toString();
        spark.sql(fixtureSql).write().format("delta").save(path);
        String sum = aggregate("SUM", "region", 0, 6, "");
        try (var factory = factory(SCOPE, true)) {
            var entry = factory.executeLogicalQueryUseCase();
            tracked.beforeExecute.set(() -> appendFreshnessRow("during-build"));
            tracked.calls.clear();
            var changedBuild = entry.query(sum);
            check(sum, 1, false, changedBuild, "latest-spark");
            assertThat(changedBuild.exactCubeTrace().reason()).isEqualTo("recovery:CubeSourceChangedException");
            assertThat(tracked.calls).hasSize(2);
            check(sum, 1, false, entry.query(sum), "latest-build");
            check(sum, 1, false, entry.query(sum), "exact-cube");

            // Fire on the recheck, after the request used only cached state from version 1.
            tracked.beforeSourceObservation.set(() ->
                    tracked.beforeSourceObservation.set(() -> appendFreshnessRow("during-cache-hit")));
            tracked.calls.clear();
            var changedHit = entry.query(sum);
            check(sum, 2, false, changedHit, "latest-spark");
            assertThat(changedHit.exactCubeTrace().reason()).isEqualTo("recovery:CubeSourceChangedException");
            assertThat(changedHit.exactCubeTrace().fragments()).hasSize(1);
            assertThat(changedHit.exactCubeTrace().residualSql()).isEmpty();
            assertThat(tracked.calls).hasSize(1);
        } finally {
            tracked.beforeExecute.set(null);
            tracked.beforeSourceObservation.set(null);
            path = original;
        }
    }

    private void appendFreshnessRow(String region) {
        spark.newSession().sql("INSERT INTO delta.`" + path + "` VALUES (0L, '" + region + "', 'writer', 1)")
                .collectAsList();
    }

    @Test void writerAfterPlanningAndEvictedGenerationRecoverAgainstLatest() throws Exception {
        String original = path;
        path = scratch.resolve("race-facts").toUri().toString();
        spark.sql(fixtureSql).write().format("delta").save(path);
        String sum = aggregate("SUM", "region, device", 0, 6, "");
        String count = aggregate("COUNT", "region", 0, HIGH, "");
        String avg = aggregate("AVG", "region", 0, HIGH, "") + " ORDER BY value DESC NULLS LAST, region NULLS FIRST LIMIT 10";
        try (var factory = factory(SCOPE, true); var inspector = RedisClient.create(valkey.uri());
             var connection = inspector.connect()) {
            var entry = factory.executeLogicalQueryUseCase();
            entry.query(sum);
            entry.query(count);
            tracked.beforeExecute.set(() -> spark.newSession().sql("INSERT INTO delta.`" + path
                    + "` VALUES (518400L, 'external-row', 'writer', 700)").collectAsList());
            tracked.calls.clear();
            var racing = entry.query(avg);
            check(avg, 1, true, racing, "latest-spark");
            assertThat(racing.exactCubeTrace().reason()).isEqualTo("recovery:CubeSourceChangedException");
            assertThat(racing.frame().rows()).anySatisfy(row -> assertThat(row.get("region")).isEqualTo("external-row"));
            assertThat(tracked.calls).hasSize(2);
            assertThat(tracked.calls.getFirst()).isEqualTo(racing.exactCubeTrace().residualSql().getFirst());
            assertThat(tracked.calls.getLast()).contains("AVG(amount)");
            tracked.calls.clear();
            check(avg, 1, true, entry.query(avg), "latest-spark");
            assertThat(tracked.calls).singleElement().asString().doesNotContain("VERSION AS OF");

            entry.query(sum);
            entry.query(count);
            var hot = entry.query(avg);
            check(avg, 1, true, hot, "exact-cube");
            String generation = hot.exactCubeTrace().fragments().getLast().generation();
            var store = ((ValkeyCacheBackendAdapter) factory.cacheBackendForShutdown()).exactCubeStore();
            var evicting = new ExactCubeStorePort() {
                @Override public List<ExactCubeFragment> candidates(CubePopulation population, CubeInterval interval) {
                    return store.candidates(population, interval);
                }

                @Override public Optional<ResultFrame> read(ExactCubeFragment fragment) {
                    if (fragment.generation().equals(generation)) {
                        assertThat(connection.sync().del("QC:V4:exact:v1:frame:" + generation)).isEqualTo(1);
                    }
                    return store.read(fragment);
                }

                @Override public void publish(CubePopulation population, CubeInterval interval, List<String> grain,
                                              Set<CubeMeasure> measures, ResultFrame state, Duration ttl, String fence) {
                    store.publish(population, interval, grain, measures, state, ttl, fence);
                }

                @Override public String publicationFence() {
                    return store.publicationFence();
                }
            };
            var catalog = new TableCatalog().register(RegisteredTable.of(SOURCE, path, Map.of("ts", "ts"), "ts"));
            var faultEntry = new ExecuteLogicalQueryService(new LogicalToPhysicalSqlTranslator(300, catalog),
                    new CalciteCanonicalObjectFactory(), factory.executeCachedQueryUseCase(), tracked,
                    new ExactCubeExecution(new ExactCubeSqlAdapter(Set.of(path), "ts"), evicting, tracked,
                            SCOPE, Clock.systemUTC(), DAY, Duration.ofHours(1)));
            tracked.calls.clear();
            var recovered = faultEntry.query(avg);
            check(avg, 1, true, recovered, "latest-spark");
            assertThat(recovered.exactCubeTrace().reason()).startsWith("recovery:");
            assertThat(recovered.exactCubeTrace().fragments()).hasSize(1);
            assertThat(tracked.calls).singleElement().asString().doesNotContain("VERSION AS OF");

            spark.newSession().sql("UPDATE delta.`" + path + "` SET ts = 518400, amount = 8 "
                    + "WHERE region = 'zero'").collectAsList();
            check(avg, 2, true, entry.query(avg), "latest-spark");
            spark.newSession().sql("DELETE FROM delta.`" + path + "` WHERE region = 'external-row'").collectAsList();
            check(avg, 3, true, entry.query(avg), "latest-spark");

            String oldTableId = tracked.currentSource(path, SCOPE).orElseThrow().tableId();
            Files.move(Path.of(URI.create(path)), scratch.resolve("retired-race-facts"));
            spark.sql(fixtureSql).selectExpr("ts", "region", "device", "amount + 100 AS amount")
                    .write().format("delta").save(path);
            var recreated = tracked.currentSource(path, SCOPE).orElseThrow();
            assertThat(recreated.tableId()).isNotEqualTo(oldTableId);
            assertThat(recreated.version()).isZero();
            check(avg, 0, true, entry.query(avg), "latest-spark");
            System.out.println("EXACT_CUBE_RACE observedVersion=0 recoveredOnVersion=1 moveVersion=2 deleteVersion=3 "
                    + "recreatedVersion=0 evictedAfterPlan=" + generation);
        } finally {
            path = original;
        }
    }

    @Test void sqlErrorsRetainTheirSparkCondition() {
        var statements = List.of(
                "SELECT (SELECT amount FROM " + SOURCE + " WHERE ts = 0) AS v",
                "SELECT SUM(CAST(amount AS BIGINT) + CAST(9223372036854775807 AS BIGINT)) AS v "
                        + "FROM " + SOURCE + " WHERE amount > 0");
        try (var factory = factory(SCOPE, true)) {
            for (String sql : statements) {
                String expected = sparkCondition(() -> oracle.execute(latestSql(sql)));
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

    @Test void wideStateExceedsFormerRowKeyAndByteLimits() {
        String original = path;
        path = scratch.resolve("wide-facts").toUri().toString();
        spark.sql("""
                SELECT CASE WHEN id < 12000 THEN 0L ELSE 86400L END AS ts,
                       concat(lpad(CAST(id AS STRING), 8, '0'), repeat('x', 142)) AS region,
                       'skewed' AS device, CAST(pmod(id, 17) - 8 AS INT) AS amount
                FROM range(48000)
                """).write().format("delta").save(path);
        String fits = aggregate("SUM", "region", 0, 1, "");
        String larger = aggregate("SUM", "region", 1, 2, "");
        try (var factory = factory(SCOPE, true)) {
            var entry = factory.executeLogicalQueryUseCase();
            tracked.calls.clear();
            check(fits, 0, false, entry.query(fits), "latest-build");
            assertThat(tracked.calls).singleElement().asString().doesNotContain("LIMIT");
            tracked.calls.clear();
            var covered = entry.query(fits);
            check(fits, 0, false, covered, "exact-cube");
            assertThat(covered.frame().rowCount()).isEqualTo(12_000);
            assertThat(covered.exactCubeTrace().fragments()).singleElement()
                    .satisfies(fragment -> assertThat(fragment.rows()).isEqualTo(12_000));
            assertThat(tracked.calls).isEmpty();

            check(larger, 0, false, entry.query(larger), "latest-build");
            assertThat(tracked.calls).singleElement().asString().doesNotContain("LIMIT");
            tracked.calls.clear();
            var largeCover = entry.query(larger);
            check(larger, 0, false, largeCover, "exact-cube");
            assertThat(largeCover.frame().rowCount()).isEqualTo(36_000);
            assertThat(largeCover.exactCubeTrace().fragments()).singleElement().satisfies(fragment ->
                    assertThat(fragment.decodedBytes()).isGreaterThan(8 * 1024 * 1024));
            assertThat(tracked.calls).isEmpty();
        } finally {
            path = original;
        }
        System.out.println("EXACT_CUBE_WIDE cachedGroups=36000 keyBytes=150");
    }

    private CascadaEngineFactory factory(String scope, boolean enabled) {
        var settings = new EngineSettings(CacheExecutionConfiguration.defaults(), valkey.uri(), SOURCE, path, true, 8);
        return new CascadaEngineFactory(settings, new LazyQueryExecutor(() -> tracked), ApplicationSettings.defaults(), enabled, scope);
    }

    private void check(String sql, long version, boolean ordered, QueryResult actual, String route) {
        var expected = oracle.execute(latestSql(sql));
        String message = "seed=" + SEED + " version=" + version + " SQL=" + sql;
        try {
            assertThat(actual.exactCubeTrace() == null ? null : actual.exactCubeTrace().route())
                    .as("%s trace=%s", message, actual.exactCubeTrace()).isEqualTo(route);
            if ("exact-cube".equals(route) || "latest-build".equals(route)) {
                assertThat(actual.exactCubeTrace().version()).as(message).isEqualTo(version);
            }
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
            reproduction.put("expectedLatestVersion", version);
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

    private String latestSql(String sql) {
        return sql.replaceAll("\\b" + SOURCE + "\\b", "delta.`" + path + "`");
    }

    private static String aggregate(String operation, String grain, long low, long high, String filter) {
        return "SELECT " + grain + ", " + operation + "(amount) AS value FROM " + SOURCE
                + " WHERE ts >= " + low * DAY + " AND ts < " + high * DAY + filter + " GROUP BY " + grain;
    }

    private static final class TrackingExecutor implements LatestQueryExecutorPort {
        final List<String> calls = new CopyOnWriteArrayList<>();
        final AtomicReference<Runnable> beforeExecute = new AtomicReference<>();
        final AtomicReference<Runnable> beforeSourceObservation = new AtomicReference<>();
        private final SparkDeltaQueryExecutor spark;

        TrackingExecutor(SparkDeltaQueryExecutor spark) {
            this.spark = spark;
        }

        @Override public Optional<ExactCubeSource> currentSource(String path, String scope) {
            Runnable hook = beforeSourceObservation.getAndSet(null);
            if (hook != null) hook.run();
            return spark.currentSource(path, scope);
        }

        @Override public ResultFrame execute(String sql) {
            Runnable hook = beforeExecute.getAndSet(null);
            if (hook != null) hook.run();
            assertThat(sql).doesNotContain("VERSION AS OF");
            calls.add(sql);
            return spark.execute(sql);
        }
    }
}
