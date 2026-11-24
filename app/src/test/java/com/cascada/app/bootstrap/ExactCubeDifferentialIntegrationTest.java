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
import com.cascada.cache.application.port.out.PinnedQueryExecutorPort;
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

    @Test void reusesFilteredAggregatesAcrossCteJoinAndSubqueryScopes() {
        String filter = " AND device = 'x'";
        String finer = aggregate("SUM", "region, device", 0, HIGH, filter);
        String inner = "SELECT src.region AS area, SUM(src.amount) AS revenue FROM " + SOURCE
                + " src WHERE src.ts >= 0 AND src.ts < " + HIGH * DAY
                + " AND src.device = 'x' GROUP BY src.region";
        String cte = "WITH agg AS (" + inner + "), paired AS ("
                + "SELECT l.area, l.revenue + r.revenue AS combined FROM agg l JOIN agg r "
                + "ON l.area <=> r.area WHERE r.revenue IS NOT NULL) "
                + "SELECT area, combined, RANK() OVER (ORDER BY combined DESC) AS pos FROM paired "
                + "ORDER BY pos, area NULLS FIRST";
        String leftJoin = "SELECT a.area, a.revenue, f.device FROM (" + inner + ") a LEFT JOIN "
                + SOURCE + " f ON a.area = f.region AND f.ts = 0 WHERE f.amount IS NULL";
        String exists = "SELECT a.area, a.revenue FROM (" + inner + ") a WHERE EXISTS "
                + "(SELECT 1 FROM " + SOURCE + " f WHERE f.region <=> a.area AND f.amount IS NULL)";
        String scalar = "SELECT f.region FROM " + SOURCE + " f WHERE f.ts = 0 AND f.amount < "
                + "(SELECT SUM(src.amount) AS revenue FROM " + SOURCE + " src WHERE src.ts >= 0 "
                + "AND src.ts < " + HIGH * DAY + " AND src.device = 'x')";
        String wrongAlias = "SELECT f.region FROM " + SOURCE + " f WHERE f.ts = 0 AND f.amount < "
                + "(SELECT SUM(src.amount) AS revenue FROM " + SOURCE + " src WHERE src.ts >= 0 "
                + "AND src.ts < " + HIGH * DAY + " AND f.device = 'x')";
        try (var factory = factory(SCOPE, true)) {
            var entry = factory.executeLogicalQueryUseCase();
            check(finer, 0, false, entry.query(finer), "pinned-build");
            for (String sql : List.of(cte, leftJoin, exists, scalar)) {
                tracked.calls.clear();
                var answer = entry.query(sql);
                check(sql, 0, sql.equals(cte), answer, "exact-cube");
                assertThat(answer.exactCubeTrace().fragments()).hasSize(1);
                assertThat(answer.exactCubeTrace().residualSql()).isEmpty();
                assertThat(tracked.calls).containsExactly(answer.exactCubeTrace().outerSql());
                assertThat(tracked.calls.getFirst()).contains("CASCADA_COMPLETE_AGGREGATE").doesNotContain("VALUES");
                if (!sql.equals(cte)) assertThat(tracked.calls.getFirst()).contains("VERSION AS OF 0");
            }
            var result = entry.query(wrongAlias);
            check(wrongAlias, 0, false, result, "existing");
            assertThat(result.exactCubeTrace().reason()).isEqualTo("unproven-sql-shape");
        }
    }

    @Test void narrowsAnUnfilteredCubeOnlyWhenTheFilterKeySurvives() {
        String broad = aggregate("SUM", "region, device", 0, HIGH, "");
        String tooCoarse = aggregate("SUM", "region", 0, HIGH, "");
        String grouped = "SELECT region AS area, SUM(amount) AS total FROM " + SOURCE
                + " WHERE ts >= 0 AND ts < " + HIGH * DAY + " AND device = 'x' GROUP BY region";
        String filteredCte = "WITH population AS (SELECT * FROM " + SOURCE + " f WHERE f.ts >= 0 "
                + "AND f.device = 'x'), narrowed AS (SELECT * FROM population p WHERE p.ts < " + HIGH * DAY + ") "
                + "SELECT n.region AS area, SUM(n.amount) AS total FROM narrowed n GROUP BY n.region";
        String filteredDerived = "SELECT n.region AS area, SUM(n.amount) AS total FROM (SELECT * FROM "
                + SOURCE + " f WHERE f.ts >= 0 AND f.device = 'x') n WHERE n.ts < " + HIGH * DAY + " GROUP BY n.region";
        String nullKey = "SELECT device, SUM(amount) AS value FROM " + SOURCE + " WHERE ts >= 0 AND ts < "
                + HIGH * DAY + " AND region IS NULL GROUP BY device";
        String groupingAlias = grouped.replace("GROUP BY region", "GROUP BY area");
        String collidingAlias = grouped.replace(" AS area", " AS device").replace("GROUP BY region", "GROUP BY device");
        String missing = grouped.replace("device = 'x'", "device = 'absent'");
        try (var factory = factory(SCOPE, true)) {
            var entry = factory.executeLogicalQueryUseCase();
            check(tooCoarse, 0, false, entry.query(tooCoarse), "pinned-build");
            check(grouped + " ORDER BY total, area LIMIT 100", 0, true,
                    entry.query(grouped + " ORDER BY total, area LIMIT 100"), "pinned-spark");
            check(broad, 0, false, entry.query(broad), "pinned-build");
            for (String sql : List.of(grouped, groupingAlias, filteredCte, filteredDerived, nullKey, missing)) {
                tracked.calls.clear();
                var answer = entry.query(sql);
                check(sql, 0, false, answer, "exact-cube");
                assertThat(answer.exactCubeTrace().fragments()).singleElement().satisfies(fragment -> {
                    assertThat(fragment.population().predicate()).isEmpty();
                    assertThat(fragment.grain()).containsExactly("region", "device");
                });
                assertThat(answer.exactCubeTrace().residualSql()).isEmpty();
                assertThat(tracked.calls).hasSize(sql.equals(filteredCte) ? 1 : 0);
            }
            assertThat(sparkCondition(() -> entry.query(collidingAlias)))
                    .isEqualTo(sparkCondition(() -> oracle.execute(pinned(collidingAlias, 0))));
        }
    }

    @Test void missingAndCorruptGenerationsAreRemovedBeforeTheNextRequest() {
        String sql = aggregate("SUM", "region", 0, 6, "");
        try (var factory = factory(SCOPE, true); var inspector = RedisClient.create(valkey.uri());
             var connection = inspector.connect()) {
            var entry = factory.executeLogicalQueryUseCase();
            assertThat(entry.query(sql).exactCubeTrace().route()).isEqualTo("pinned-build");
            var original = entry.query(sql).exactCubeTrace().fragments().getFirst();
            var store = ((ValkeyCacheBackendAdapter) factory.cacheBackendForShutdown()).exactCubeStore();
            var frame = store.read(original).orElseThrow();
            store.publish(original.population(), original.interval(), original.grain(), original.measures(),
                    frame, Duration.ofHours(1), store.publicationFence());
            assertThat(store.candidates(original.population(), original.interval())).hasSize(2);
            var winner = entry.query(sql).exactCubeTrace().fragments().getFirst();
            assertThat(connection.sync().del("QC:V4:exact:v1:frame:" + winner.generation())).isEqualTo(1);
            tracked.calls.clear();
            var missing = entry.query(sql);
            check(sql, 0, false, missing, "pinned-spark");
            assertThat(tracked.calls).singleElement().asString().contains("VERSION AS OF 0");

            tracked.calls.clear();
            var healthy = entry.query(sql);
            check(sql, 0, false, healthy, "exact-cube");
            assertThat(healthy.exactCubeTrace().fragments()).noneMatch(winner::equals);
            assertThat(tracked.calls).isEmpty();
            assertThat(store.candidates(original.population(), original.interval())).hasSize(1);
            store.publish(original.population(), original.interval(), original.grain(), original.measures(),
                    frame, Duration.ofHours(1), store.publicationFence());
            var corrupt = entry.query(sql).exactCubeTrace().fragments().getFirst();
            connection.sync().set("QC:V4:exact:v1:frame:" + corrupt.generation(),
                    "x".repeat(corrupt.compressedBytes()));
            tracked.calls.clear();
            var answer = entry.query(sql);
            check(sql, 0, false, answer, "pinned-spark");
            assertThat(tracked.calls).singleElement().asString().contains("VERSION AS OF 0");
            assertThat(store.candidates(original.population(), original.interval())).hasSize(1);

            tracked.calls.clear();
            var afterCorruption = entry.query(sql);
            check(sql, 0, false, afterCorruption, "exact-cube");
            assertThat(afterCorruption.exactCubeTrace().fragments()).noneMatch(corrupt::equals);
            assertThat(tracked.calls).isEmpty();
        }
    }

    @Test void cancelledExactCubeAdmissionDoesNotSubmitSparkOrInvalidateTheFragment() {
        String sql = aggregate("SUM", "region", 0, 6, "");
        try (var factory = factory(SCOPE, true)) {
            var entry = factory.executeLogicalQueryUseCase();
            check(sql, 0, false, entry.query(sql), "pinned-build");
            tracked.calls.clear();
            var cancellation = new com.cascada.cache.application.execution.QueryCancellation();
            tracked.afterBind.set(cancellation::cancel);
            try (var scope = new com.cascada.cache.application.execution.QueryExecutionScope(cancellation)) {
                org.assertj.core.api.Assertions.assertThatThrownBy(() -> entry.query(sql))
                        .isInstanceOf(java.util.concurrent.CancellationException.class);
            }
            assertThat(tracked.calls).isEmpty();
            check(sql, 0, false, entry.query(sql), "exact-cube");
            assertThat(tracked.calls).isEmpty();
        }
    }

    @Test void seededNestedPlansMatchPinnedSparkAcrossColdMixedAndFullCoverage() {
        var random = new java.util.Random(SEED ^ 71_319);
        var cases = new ArrayList<String>();
        for (int i = 0; i < 9; i++) {
            String alias = "factor_" + random.nextInt(10_000);
            String inner = "SELECT " + alias + ".region AS area, AVG(" + alias + ".amount) AS average_amount FROM "
                    + SOURCE + " " + alias + " WHERE " + alias + ".ts >= 0 AND " + alias + ".ts < "
                    + HIGH * DAY + " GROUP BY " + alias + ".region";
            String outer = switch (i % 3) {
                case 0 -> "WITH value_rows AS (" + inner + ") SELECT area, average_amount FROM value_rows "
                        + "WHERE average_amount IS NOT NULL";
                case 1 -> "SELECT g.area, g.average_amount FROM (" + inner + ") g "
                        + "WHERE g.average_amount < " + (i * 7);
                default -> "WITH values_by_area AS (SELECT 'unused' AS area, 0 AS average_amount), "
                        + "current_area AS (WITH values_by_area AS (" + inner + ") "
                        + "SELECT area, average_amount FROM values_by_area) SELECT area, average_amount FROM current_area";
            };
            cases.add(outer + " ORDER BY average_amount DESC NULLS LAST, area NULLS FIRST LIMIT " + (i + 1));
        }
        try (var factory = factory(SCOPE, true)) {
            var entry = factory.executeLogicalQueryUseCase();
            for (var sql : cases) {
                tracked.calls.clear();
                check(sql, 0, true, entry.query(sql), "pinned-spark");
                assertThat(tracked.calls).singleElement().asString().contains("VERSION AS OF 0");
            }

            var sumBeforeGap = aggregate("SUM", "region, device", 0, 6, "");
            var countAll = aggregate("COUNT", "region", 0, HIGH, "");
            check(sumBeforeGap, 0, false, entry.query(sumBeforeGap), "pinned-build");
            check(countAll, 0, false, entry.query(countAll), "pinned-build");
            for (var sql : cases) {
                tracked.calls.clear();
                var answer = entry.query(sql);
                check(sql, 0, true, answer, "exact-cube");
                var trace = answer.exactCubeTrace();
                assertThat(trace.fragments()).hasSize(2);
                assertThat(trace.residualSql()).singleElement().asString()
                        .contains("VERSION AS OF 0", "SUM(`amount`)", "`ts` >= 518400", "`ts` < 777600")
                        .doesNotContain("COUNT(`amount`)");
                assertThat(tracked.calls).containsExactly(trace.residualSql().getFirst(), trace.outerSql());
            }

            var sumGap = aggregate("SUM", "region, device", 6, HIGH, "");
            check(sumGap, 0, false, entry.query(sumGap), "pinned-build");
            for (var sql : cases) {
                tracked.calls.clear();
                var answer = entry.query(sql);
                check(sql, 0, true, answer, "exact-cube");
                assertThat(answer.exactCubeTrace().fragments()).hasSize(3);
                assertThat(answer.exactCubeTrace().residualSql()).isEmpty();
                assertThat(tracked.calls).containsExactly(answer.exactCubeTrace().outerSql());
            }
        }
        System.out.println("EXACT_CUBE_GRAPH seed=" + SEED + " mixed=9 full=9 cold=9");
    }

    @Test void emptyGroupedAndScalarResultsHaveDifferentPresence() {
        String sum = aggregate("SUM", "region, device", 12, 13, "");
        String count = aggregate("COUNT", "region", 12, 13, "");
        String grouped = aggregate("AVG", "region", 12, 13, "");
        String scalar = "SELECT AVG(amount) AS mean FROM " + SOURCE + " WHERE ts >= " + 12 * DAY
                + " AND ts < " + 13 * DAY;
        try (var factory = factory(SCOPE, true)) {
            var entry = factory.executeLogicalQueryUseCase();
            check(sum, 0, false, entry.query(sum), "pinned-build");
            check(count, 0, false, entry.query(count), "pinned-build");
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
            check(sum, 0, false, entry.query(sum), "pinned-build");
            check(count, 0, false, entry.query(count), "pinned-build");

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
            assertThat(residual).contains("SUM(`amount`)", "VERSION AS OF 0", "`ts` >= 518400", "`ts` < 777600")
                    .doesNotContain("COUNT(`amount`)");
            assertThat(tracked.calls).containsExactly(residual);
            System.out.println("EXACT_CUBE_MIXED fragments=" + trace.fragments().stream().map(fragment ->
                    fragment.generation() + ":" + fragment.grain() + ":" + fragment.measures() + ":" + fragment.interval()).toList()
                    + " residual=" + residual + " compressedBytes="
                    + trace.fragments().stream().mapToInt(fragment -> fragment.compressedBytes()).sum()
                    + " decodedBytes=" + trace.fragments().stream().mapToInt(fragment -> fragment.decodedBytes()).sum()
                    + " planningNanos=" + trace.planningNanos());

            String countHead = aggregate("COUNT", "region", 0, 6, "");
            check(countHead, 0, false, entry.query(countHead), "pinned-build");
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
            assertThat(tracked.calls.getFirst()).contains("CASCADA_COMPLETE_AGGREGATE", "HAVING")
                    .doesNotContain("VERSION AS OF");
            System.out.println("EXACT_CUBE_OUTER route=exact-cube sourceScans=0 outerSql=" + tracked.calls.getFirst());
        }
        try (var restarted = factory(SCOPE, true); var isolated = factory("another-policy", true);
             var disabled = factory(SCOPE, false)) {
            check(avg, 0, true, restarted.executeLogicalQueryUseCase().query(avg), "exact-cube");
            check(avg, 0, true, isolated.executeLogicalQueryUseCase().query(avg), "pinned-spark");
            check(avg, 0, true, disabled.executeLogicalQueryUseCase().query(avg), null);
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

    @Test void transportLastByteAndConcurrentFullCoverage() throws Exception {
        var server = new ServerSettings(0, 0, Duration.ofSeconds(60));
        var json = new ObjectMapper();
        var client = HttpClient.newHttpClient();
        String query = aggregate("AVG", "region", 0, HIGH, "") + " ORDER BY value DESC NULLS LAST, region NULLS FIRST LIMIT 10";
        String sum = aggregate("SUM", "region, device", 0, 6, "");
        String count = aggregate("COUNT", "region", 0, HIGH, "");
        String sumAll = aggregate("SUM", "region", 0, HIGH, "");
        var expected = new ByteArrayOutputStream();
        QueryResponseEncoder.json(oracle.execute(pinned(query, 0)), expected);
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
                 var direct = new QueryHttpServer(server, sql -> new QueryResult(oracle.execute(pinned(sql, 0)), false));
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
                    sample("pinned-spark", client, directRequest, expectedJson, json, null);
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
            String expectedRoute = route.equals("mixed") || route.equals("fully-covered") ? "exact-cube" : "pinned-spark";
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

    @Test void writerAfterAdmissionCannotChangeACacheBuildOrFullyCoveredHit() {
        String original = path;
        path = scratch.resolve("admission-facts").toUri().toString();
        spark.sql(fixtureSql).write().format("delta").save(path);
        String sql = aggregate("SUM", "region", 0, 6, "");
        try (var factory = factory(SCOPE, true)) {
            var entry = factory.executeLogicalQueryUseCase();
            tracked.afterBind.set(() -> appendDelta("after-build-bind"));
            tracked.calls.clear();
            check(sql, 0, false, entry.query(sql), "pinned-build");
            assertThat(tracked.calls).singleElement().asString().contains("VERSION AS OF 0");
            check(sql, 1, false, entry.query(sql), "pinned-build");

            tracked.afterBind.set(() -> appendDelta("after-hit-bind"));
            tracked.calls.clear();
            check(sql, 1, false, entry.query(sql), "exact-cube");
            assertThat(tracked.calls).isEmpty();
            check(sql, 2, false, entry.query(sql), "pinned-build");
        } finally {
            path = original;
        }
    }

    private void appendDelta(String region) {
        spark.newSession().sql("INSERT INTO delta.`" + path + "` VALUES (0L, '" + region + "', 'writer', 1)")
                .collectAsList();
    }

    @Test void writerAfterPlanningAndEvictedGenerationRecoverAtPinnedVersion() throws Exception {
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
            tracked.beforePinned.set(() -> spark.newSession().sql("INSERT INTO delta.`" + path
                    + "` VALUES (518400L, 'external-row', 'writer', 700)").collectAsList());
            tracked.calls.clear();
            var racing = entry.query(avg);
            check(avg, 0, true, racing, "exact-cube");
            assertThat(tracked.calls).containsExactly(racing.exactCubeTrace().residualSql().getFirst());
            assertThat(tracked.calls.getFirst()).contains("VERSION AS OF 0");
            tracked.calls.clear();
            check(avg, 1, true, entry.query(avg), "pinned-spark");
            assertThat(tracked.calls).singleElement().asString().contains("VERSION AS OF 1");

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
            check(avg, 1, true, recovered, "pinned-spark");
            assertThat(recovered.exactCubeTrace().reason()).startsWith("recovery:");
            assertThat(recovered.exactCubeTrace().fragments()).hasSize(1);
            assertThat(tracked.calls).singleElement().asString().contains("VERSION AS OF 1");

            spark.newSession().sql("UPDATE delta.`" + path + "` SET ts = 518400, amount = 8 "
                    + "WHERE region = 'zero'").collectAsList();
            check(avg, 2, true, entry.query(avg), "pinned-spark");
            spark.newSession().sql("DELETE FROM delta.`" + path + "` WHERE region = 'external-row'").collectAsList();
            check(avg, 3, true, entry.query(avg), "pinned-spark");

            String oldTableId = tracked.bind(path, SCOPE).orElseThrow().tableId();
            Files.move(Path.of(URI.create(path)), scratch.resolve("retired-race-facts"));
            spark.sql(fixtureSql).selectExpr("ts", "region", "device", "amount + 100 AS amount")
                    .write().format("delta").save(path);
            var recreated = tracked.bind(path, SCOPE).orElseThrow();
            assertThat(recreated.tableId()).isNotEqualTo(oldTableId);
            assertThat(recreated.version()).isZero();
            check(avg, 0, true, entry.query(avg), "pinned-spark");
            System.out.println("EXACT_CUBE_RACE pinnedVersion=0 appendVersion=1 moveVersion=2 deleteVersion=3 "
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
            check(fits, 0, false, entry.query(fits), "pinned-build");
            assertThat(tracked.calls).singleElement().asString().doesNotContain("LIMIT");
            tracked.calls.clear();
            var covered = entry.query(fits);
            check(fits, 0, false, covered, "exact-cube");
            assertThat(covered.frame().rowCount()).isEqualTo(12_000);
            assertThat(covered.exactCubeTrace().fragments()).singleElement()
                    .satisfies(fragment -> assertThat(fragment.rows()).isEqualTo(12_000));
            assertThat(tracked.calls).isEmpty();

            check(larger, 0, false, entry.query(larger), "pinned-build");
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
        final AtomicReference<Runnable> beforePinned = new AtomicReference<>();
        final AtomicReference<Runnable> afterBind = new AtomicReference<>();
        private final SparkDeltaQueryExecutor spark;

        TrackingExecutor(SparkDeltaQueryExecutor spark) {
            this.spark = spark;
        }

        @Override public Optional<ExactCubeSource> bind(String path, String scope) {
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
