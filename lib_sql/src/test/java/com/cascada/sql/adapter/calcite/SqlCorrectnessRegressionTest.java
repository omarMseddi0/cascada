package com.cascada.sql.adapter.calcite;

import com.cascada.cache.application.service.ExecuteLogicalQueryService;
import com.cascada.cache.domain.GapPlan;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.safety.CacheConfiguration;
import com.cascada.cache.domain.safety.SafetyRuleRegistry;
import com.cascada.sql.adapter.dialect.MySqlToSparkFunctionTranslator;
import com.cascada.sql.domain.RegisteredTable;
import com.cascada.sql.domain.TableCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import static org.assertj.core.api.Assertions.*;

class SqlCorrectnessRegressionTest {
    private final CalciteCanonicalObjectFactory factory = new CalciteCanonicalObjectFactory();

    @Test void preservesLiteralWhitespaceAndIdentifierWhitespace() {
        String sql = "SELECT 'a  b', `a  b` FROM t WHERE name = 'a  b'";
        assertThat(CalciteSql.unparse(CalciteSql.parseQuery(sql))).contains("'a  b'", "`a  b`");
    }

    @Test void retainsTimeBusinessPredicatesWhenReplacingBounds() {
        String result = new GapQueryBuilder("ts", 86400).buildGapQuery(
                "SELECT SUM(x) FROM t WHERE ts>=0 AND ts<=172799 AND MOD(ts,2)=0",
                new GapPlan(Optional.empty(), List.of(0L), Optional.empty()));
        assertThat(result).contains("MOD(ts, 2) = 0", "ts <= 86399").doesNotContain("172799");
    }

    @Test void dialectRewritesCodeButNeverLiteralOrCommentText() {
        String result = new MySqlToSparkFunctionTranslator().translate(
                "SELECT NOW(), 'NOW() AS CHAR', `NOW()` /* NOW() AS CHAR */");
        assertThat(result).contains("current_timestamp()", "'NOW() AS CHAR'", "`NOW()`", "/* NOW() AS CHAR */");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT AVG(x) AS mean FROM t WHERE ts>=0 AND ts<=172799",
            "SELECT SUM(x)/COUNT(x) AS mean FROM t WHERE ts>=0 AND ts<=172799",
            "SELECT MAX(x)-MIN(x) AS spread FROM t WHERE ts>=0 AND ts<=172799",
            "SELECT country AS c,SUM(x) FROM t WHERE ts>=0 AND ts<=172799 GROUP BY country",
            "SELECT country,SUM(x) AS n FROM t WHERE ts>=0 AND ts<=172799 GROUP BY country ORDER BY n+1 LIMIT 1",
            "SELECT country,SUM(x) AS n FROM t WHERE ts>=0 AND ts<=172799 GROUP BY country ORDER BY n LIMIT 1 OFFSET 1"
    })
    void unsupportedMergeShapesExecuteDirectly(String sql) {
        var canonical = factory.canonicalize(sql);
        assertThat(SafetyRuleRegistry.defaultRegistry().evaluate(canonical, CacheConfiguration.defaults()).isBypass()).isTrue();
    }

    @Test void missingTimeRangeFallsBackToTranslatedSql() {
        AtomicReference<String> executed = new AtomicReference<>();
        var service = new ExecuteLogicalQueryService(s -> "SELECT SUM(x) FROM physical", factory,
                c -> { throw new AssertionError("cache must be bypassed"); },
                s -> { executed.set(s); return ResultFrame.empty(); });
        assertThat(service.query("SELECT SUM(x) FROM logical").servedThroughCache()).isFalse();
        assertThat(executed.get()).isEqualTo("SELECT SUM(x) FROM physical");
    }

    @Test void timeBucketingRetainsTheInternalTimeColumnName() {
        var catalog = new TableCatalog().register(RegisteredTable.of("t", "/tmp/t", Map.of("ts", "ts"), "ts"));
        String result = new LogicalToPhysicalSqlTranslator(300, catalog).translateToPhysicalSql(
                "SELECT ts,SUM(x) FROM t WHERE ts>=0 AND ts<=172799 GROUP BY ts");
        assertThat(result).contains("AS BIGINT) ts");
        assertThat(factory.canonicalize(result).logicSignature()).isEmpty();
    }
}
