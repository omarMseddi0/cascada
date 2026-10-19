package com.cascada.sql.adapter.out.calcite;

import com.cascada.cache.adapter.out.cache.InMemoryBlobCacheBackendAdapter;
import com.cascada.cache.adapter.out.serialization.PortableFrameSerializer;
import com.cascada.cache.adapter.out.tracking.QueryPopularityTracker;
import com.cascada.cache.application.model.config.CacheExecutionConfiguration;
import com.cascada.cache.application.service.*;
import com.cascada.cache.application.service.hashing.QueryHashGenerator;
import com.cascada.cache.application.service.safety.SafetyRuleRegistry;
import com.cascada.cache.application.service.warming.WarmingQueue;
import com.cascada.cache.domain.frame.*;
import com.cascada.cache.domain.model.safety.CacheConfiguration;
import com.cascada.sql.domain.*;
import com.cascada.sql.domain.model.TimeDimensionMap;
import com.cascada.sql.domain.model.RegisteredTable;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class PythonCoreParityRegressionTest {
    private final CalciteCanonicalObjectFactory parser = new CalciteCanonicalObjectFactory();
    private final QueryHashGenerator hashing = new QueryHashGenerator();
    private final GapQueryRewriterAdapter rewriter = new GapQueryRewriterAdapter("ts", 86400);
    private static String series(int step) {
        return "SELECT CAST(FLOOR(ts / " + step + ") * " + step + " AS BIGINT) AS ts, SUM(bytes) AS total "
                + "FROM traffic WHERE ts >= 0 AND ts <= 86399 GROUP BY CAST(FLOOR(ts / " + step + ") * " + step + " AS BIGINT)";
    }

    @Test void anHourlyWarmCanServeBothFiveMinuteAndHourlyRequestsWithoutSpark() {
        var fine = parser.canonicalize(series(300));
        var hourly = parser.canonicalize(series(3600));
        var hash = hashing.generateQueryHash(fine, 300);
        assertThat(hashing.generateQueryHash(hourly, 300)).isEqualTo(hash);
        var backend = new InMemoryBlobCacheBackendAdapter(new PortableFrameSerializer());
        AtomicReference<String> warmedSql = new AtomicReference<>();
        var warmer = new WarmingOrchestrator(backend, sql -> {
            warmedSql.set(sql);
            return ResultFrame.builder().column("ts", ColumnType.LONG).column("total", ColumnType.LONG)
                    .row(0L, 1L).row(300L, 2L).build();
        }, rewriter, new WarmingQueue(), new QueryPopularityTracker(), 86400, 10);
        warmer.warmSinglePattern(hash, hourly, 0, 86399, false);
        assertThat(warmedSql.get()).contains("FLOOR(ts / 300) * 300").doesNotContain("3600");
        var engine = new CacheExecutionEngine(backend, sql -> { throw new AssertionError("must hit the warmed bucket"); },
                rewriter, CacheExecutionConfiguration.defaults());
        assertThat(engine.execute(fine, hash).rowCount()).isEqualTo(2);
        ResultFrame rolledUp = engine.execute(hourly, hash);
        assertThat(rolledUp.rowCount()).isOne();
        assertThat(rolledUp.longAt(0, rolledUp.columnIndex("total"))).isEqualTo(3);
    }

    @Test void ratioAndRangeCompositesCombineIngredientsBeforeApplyingTheFormula() {
        for (String expression : List.of("SUM(x)/COUNT(x)", "MAX(x)-MIN(x)")) {
            var canonical = parser.canonicalize("SELECT " + expression + " AS metric FROM traffic WHERE ts>=0 AND ts<=172799");
            assertThat(SafetyRuleRegistry.defaultRegistry().evaluate(canonical, CacheConfiguration.defaults()).isBypass()).isFalse();
            boolean ratio = expression.contains("/");
            String first = ratio ? "SUM(x)" : "MAX(x)", second = ratio ? "COUNT(x)" : "MIN(x)";
            String firstColumn = CompositeIngredientNames.alias(first), secondColumn = CompositeIngredientNames.alias(second);
            var day1 = ResultFrame.builder().column(firstColumn, ColumnType.LONG).column(secondColumn, ColumnType.LONG).row(10L, 2L).build();
            var day2 = ResultFrame.builder().column(firstColumn, ColumnType.LONG).column(secondColumn, ColumnType.LONG).row(20L, 4L).build();
            ResultFrame result = new FrameMergeService(300, "ts").mergeAndReconstruct(List.of(day1, day2), canonical);
            assertThat(result.columnNames()).containsExactly("metric");
            assertThat(((Number) result.valueAt(0, 0)).doubleValue()).isEqualTo(ratio ? 5.0 : 18.0);
            String gap = rewriter.buildStorageQuery(canonical.physicalSql(),
                    new com.cascada.cache.domain.model.time.GapPlan(Optional.empty(), List.of(0L), Optional.empty()), 300);
            assertThat(gap).contains(first, second).doesNotContain(" AS metric");
        }
    }

    @Test void compositeArithmeticPreservesNullsAndLongsBeyondDoublePrecision() {
        var canonical = parser.canonicalize("SELECT SUM(a)+SUM(b) AS total FROM t WHERE ts>=0 AND ts<=172799");
        var frame = ResultFrame.builder().column(CompositeIngredientNames.alias("SUM(a)"), ColumnType.LONG).column(CompositeIngredientNames.alias("SUM(b)"), ColumnType.LONG)
                .row(9007199254740993L, 2L).build();
        ResultFrame result = new FrameMergeService(300, "ts").mergeAndReconstruct(List.of(frame), canonical);
        assertThat(result.longAt(0, 0)).isEqualTo(9007199254740995L);
        var nullable = ResultFrame.builder().column(CompositeIngredientNames.alias("SUM(a)"), ColumnType.LONG).column(CompositeIngredientNames.alias("SUM(b)"), ColumnType.LONG)
                .row(null, 2L).build();
        assertThat(new FrameMergeService(300, "ts").mergeAndReconstruct(List.of(nullable), canonical).isNullAt(0, 0)).isTrue();
    }

    @Test void wrappedAndCteQueriesTranslateAndRunThroughTheAuthoritativeFallback() {
        var catalog = new TableCatalog().register(RegisteredTable.of("traffic", "/lake/raw",
                Map.of("app", "D1", "bytes", "M1", "ts", "T1"), "ts"));
        var translator = new LogicalToPhysicalSqlTranslator(300, catalog);
        for (String logical : List.of(
                "SELECT app,SUM(value) AS total FROM (SELECT app,SUM(bytes) AS value FROM traffic WHERE ts>=0 AND ts<=86399 GROUP BY app HAVING SUM(bytes)>0) x GROUP BY app",
                "WITH totals AS (SELECT app,SUM(bytes) AS value FROM traffic WHERE ts>=0 AND ts<=86399 GROUP BY app) SELECT app,value FROM totals",
                "SELECT app,bytes FROM traffic UNION ALL SELECT app,bytes FROM traffic")) {
            AtomicReference<String> executed = new AtomicReference<>();
            var service = new ExecuteLogicalQueryService(translator, parser,
                    q -> { throw new AssertionError("non-mergeable wrapper must execute authoritatively"); },
                    sql -> { executed.set(sql); return ResultFrame.builder().column("ok", ColumnType.LONG).row(1L).build(); });
            assertThat(service.query(logical).frame().rowCount()).isOne();
            assertThat(executed.get()).contains("delta.`/lake/raw`", "M1").doesNotContain("FROM traffic");
            assertThatCode(() -> CalciteSql.parseQuery(executed.get())).doesNotThrowAnyException();
        }
    }

    @Test void distributiveWrappedAggregationIsFlattenedAndCacheable() {
        String sql = "SELECT app,SUM(value) AS total FROM (SELECT app,SUM(bytes) AS value FROM traffic "
                + "WHERE ts>=0 AND ts<=172799 GROUP BY app) x GROUP BY app";
        var canonical = parser.canonicalize(sql);
        assertThat(canonical.physicalSql()).doesNotContain("FROM (").contains("SUM(bytes)");
        assertThat(SafetyRuleRegistry.defaultRegistry().evaluate(canonical, CacheConfiguration.defaults()).isBypass()).isFalse();
        var day1 = ResultFrame.builder().column("app", ColumnType.STRING).column("total", ColumnType.LONG).row("a", 10L).build();
        var day2 = ResultFrame.builder().column("app", ColumnType.STRING).column("total", ColumnType.LONG).row("a", 20L).build();
        var result = new FrameMergeService(300, "ts").mergeAndReconstruct(List.of(day1, day2), canonical);
        assertThat(result.longAt(0, 1)).isEqualTo(30);
    }

    @Test void groupedAliasesRemainDimensionsRatherThanAccidentallyBecomingMeasures() {
        var canonical = parser.canonicalize("SELECT country AS c,SUM(x) AS n FROM traffic WHERE ts>=0 AND ts<=172799 GROUP BY country");
        assertThat(SafetyRuleRegistry.defaultRegistry().evaluate(canonical, CacheConfiguration.defaults()).isBypass()).isFalse();
        var frame = ResultFrame.builder().column("c", ColumnType.STRING).column("n", ColumnType.LONG)
                .row("FR", 10L).row("TN", 20L).build();
        var result = new FrameMergeService(300, "ts").mergeAndReconstruct(List.of(frame, frame), canonical);
        assertThat(result.rowCount()).isEqualTo(2);
        assertThat(result.longAt(0, 1)).isEqualTo(20);
        assertThat(result.longAt(1, 1)).isEqualTo(40);
    }

    @Test void physicalSourceClassificationRetainsThePythonHotBatchOnlyPolicy() {
        var factory = new CalciteCanonicalObjectFactory(new TimeDimensionMap(Set.of("ts")), Set.of("/lake/raw"));
        var raw = factory.canonicalize("SELECT SUM(x) AS s FROM delta.`/lake/raw` WHERE ts>=0 AND ts<=86399");
        var view = factory.canonicalize("SELECT SUM(x) AS s FROM delta.`/lake/view` WHERE ts>=0 AND ts<=86399");
        assertThat(raw.metadata().usesHotBatch()).isTrue();
        assertThat(view.metadata().usesHotBatch()).isFalse();
    }

    @Test void realCanonicalProjectionCanRollUpACubeWithoutAnotherSparkCall() {
        var fine = parser.canonicalize("SELECT app,device,SUM(bytes) AS total FROM traffic WHERE ts>=0 AND ts<=172799 GROUP BY app,device");
        var coarse = parser.canonicalize("SELECT app,SUM(bytes) AS total FROM traffic WHERE ts>=0 AND ts<=172799 GROUP BY app");
        var backend = new InMemoryBlobCacheBackendAdapter(new PortableFrameSerializer());
        var calls = new AtomicInteger();
        var engine = new CacheExecutionEngine(backend, sql -> {
            calls.incrementAndGet();
            return ResultFrame.builder().column("app", ColumnType.STRING).column("device", ColumnType.STRING)
                    .column("total", ColumnType.LONG).row("a", "phone", 10L).row("a", "tablet", 20L).build();
        }, rewriter, CacheExecutionConfiguration.defaults(), null,
                new com.cascada.cache.application.service.cube.CubeShapeCatalog());
        engine.execute(fine, hashing.generateQueryHash(fine));
        ResultFrame rolled = engine.execute(coarse, hashing.generateQueryHash(coarse));
        assertThat(calls).hasValue(1);
        assertThat(rolled.columnNames()).containsExactly("app", "total");
        assertThat(rolled.longAt(0, 1)).isEqualTo(30);
    }

    @Test void explicitBucketExpressionMayReuseTheSourceTimeColumnAsItsAlias() {
        var catalog = new TableCatalog().register(RegisteredTable.of("traffic", "/lake/raw",
                Map.of("ts", "ts", "bytes", "bytes"), "ts"));
        String physical = new LogicalToPhysicalSqlTranslator(300, catalog).translateToPhysicalSql(series(3600));
        assertThat(physical).contains("FLOOR(ts / 3600) * 3600");
        assertThat(parser.canonicalize(physical).metadata().userStepSeconds()).contains(3600);
    }

    @Test void fixedStorageStepNeverRewritesTimeDependentBusinessLogic() {
        String sql = "SELECT CAST(FLOOR(ts/600)*600 AS BIGINT) AS ts, "
                + "SUM(CASE WHEN FLOOR(ts/3600)*3600=0 THEN bytes ELSE 0 END) AS total "
                + "FROM traffic WHERE ts>=0 AND ts<=86399 AND FLOOR(ts/7200)*7200=0 "
                + "GROUP BY CAST(FLOOR(ts/600)*600 AS BIGINT)";
        String stored = rewriter.buildStorageQuery(sql,
                new com.cascada.cache.domain.model.time.GapPlan(Optional.empty(), List.of(0L), Optional.empty()), 300);
        assertThat(stored).contains("FLOOR(ts / 300) * 300", "FLOOR(ts / 3600) * 3600", "FLOOR(ts / 7200) * 7200");
    }
    @Test void compositeIngredientsCannotCollideWithAnExplicitAggregateAlias() {
        var canonical = parser.canonicalize("SELECT SUM(a) AS `SUM(a)`,SUM(a)+SUM(b) AS total FROM t WHERE ts>=0 AND ts<=172799");
        var frame = ResultFrame.builder().column("SUM(a)", ColumnType.LONG)
                .column(CompositeIngredientNames.alias("SUM(a)"), ColumnType.LONG)
                .column(CompositeIngredientNames.alias("SUM(b)"), ColumnType.LONG).row(10L, 10L, 20L).build();
        var result = new FrameMergeService(300, "ts").mergeAndReconstruct(List.of(frame), canonical);
        assertThat(result.columnNames()).containsExactly("SUM(a)", "total");
        assertThat(result.longAt(0, 1)).isEqualTo(30);
        String gap = rewriter.buildStorageQuery(canonical.physicalSql(),
                new com.cascada.cache.domain.model.time.GapPlan(Optional.empty(), List.of(0L), Optional.empty()), 300);
        assertThat(gap).contains(CompositeIngredientNames.alias("SUM(a)"), CompositeIngredientNames.alias("SUM(b)"));
    }

}
