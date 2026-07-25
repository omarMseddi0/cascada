package com.cascada.sql.translate;

import com.cascada.cache.domain.CanonicalQueryObject;
import com.cascada.sql.calcite.CalciteSql;
import com.cascada.sql.canonical.CalciteCanonicalObjectFactory;
import com.cascada.sql.canonical.TimeDimensionMap;
import com.cascada.sql.canonical.UnsupportedSqlException;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exhaustive coverage of {@link LogicalToPhysicalSqlTranslator} on Calcite: logical→physical column
 * and table rewriting across every clause, the Delta-path emission (which must survive as valid,
 * re-parseable Spark SQL despite Calcite's unparser), time-column bucketing only when grouped, and the
 * end-to-end canonicalisation on physical names that makes the cache key schema-rename-resistant.
 */
class LogicalToPhysicalSqlTranslatorDeepTest {

    private final LogicalToPhysicalSqlTranslator translator = new LogicalToPhysicalSqlTranslator(300);

    private TableCatalog catalog() {
        return new TableCatalog().register(RegisteredTable.of(
                "traffic", "/lakehouse/traffic",
                Map.of("country", "C5", "ts", "D17", "bytes", "M2", "device", "C9"),
                "ts"));
    }

    @Test
    void rewritesColumnsTableAndKeepsAnswerable() {
        String physical = translator.translate(
                "SELECT country, SUM(bytes) AS b FROM traffic "
                        + "WHERE ts >= 0 AND ts <= 100 AND country = 'TN' GROUP BY country",
                catalog());
        assertThat(physical).contains("C5").contains("SUM(M2)").contains("D17 >= 0")
                .contains("delta.`/lakehouse/traffic`");
        assertThat(physical).doesNotContain("country").doesNotContain("bytes").doesNotContain("FROM traffic");
    }

    @Test
    void rewritesColumnsInsideWhereAndHavingAndOrderBy() {
        String physical = translator.translate(
                "SELECT country, SUM(bytes) AS b FROM traffic "
                        + "WHERE ts >= 0 AND ts <= 100 AND device = 'mobile' "
                        + "GROUP BY country HAVING SUM(bytes) > 5 ORDER BY country",
                catalog());
        assertThat(physical).contains("C9 = 'mobile'");   // device -> C9 in WHERE
        assertThat(physical).contains("SUM(M2) > 5");      // bytes -> M2 in HAVING
        assertThat(physical).doesNotContain("device").doesNotContain("bytes");
    }

    @Test
    void bucketsTheTimeColumnOnlyWhenItIsGrouped() {
        String grouped = translator.translate(
                "SELECT ts, country, SUM(bytes) AS b FROM traffic "
                        + "WHERE ts >= 0 AND ts <= 100 GROUP BY ts, country",
                catalog());
        assertThat(grouped).contains("FLOOR(D17 / 300) * 300").contains("AS BIGINT");
        // the WHERE bound on the raw time column must NOT be bucketed
        assertThat(grouped).contains("D17 >= 0");
    }

    @Test
    void doesNotBucketWhenTheTimeColumnIsNotGrouped() {
        String physical = translator.translate(
                "SELECT country, SUM(bytes) AS b FROM traffic "
                        + "WHERE ts >= 0 AND ts <= 100 GROUP BY country",
                catalog());
        assertThat(physical).doesNotContain("FLOOR");
    }

    @Test
    void honoursADifferentBucketStep() {
        String physical = new LogicalToPhysicalSqlTranslator(600).translate(
                "SELECT ts, SUM(bytes) AS b FROM traffic WHERE ts >= 0 AND ts <= 100 GROUP BY ts",
                catalog());
        assertThat(physical).contains("FLOOR(D17 / 600) * 600");
    }

    @Test
    void theTranslatedDeltaPathIsValidReParseableSparkSql() {
        String physical = translator.translate(
                "SELECT country, SUM(bytes) AS b FROM traffic "
                        + "WHERE ts >= 0 AND ts <= 100 GROUP BY country",
                catalog());
        // The whole statement must round-trip through Calcite — proving the Delta path is valid SQL.
        assertThatCode(() -> CalciteSql.parseQuery(physical)).doesNotThrowAnyException();
    }

    @Test
    void bypassesAnUnregisteredTable() {
        assertThatThrownBy(() -> translator.translate(
                "SELECT x FROM unknown_table WHERE ts >= 0 AND ts <= 1 GROUP BY x", catalog()))
                .isInstanceOf(UnsupportedSqlException.class);
    }

    @Test
    void translatedSqlCanonicalisesWithPhysicalNames() {
        String physical = translator.translate(
                "SELECT country, SUM(bytes) AS b FROM traffic "
                        + "WHERE ts >= 0 AND ts <= 86399 AND country = 'TN' GROUP BY country",
                catalog());

        CanonicalQueryObject canonical = new CalciteCanonicalObjectFactory()
                .extractCanonicalObjectFromSql(physical, new TimeDimensionMap(Set.of("D17")));

        assertThat(canonical.hashComponents().groupBy()).containsExactly("C5");
        assertThat(canonical.hashComponents().aggregates()).containsExactly("SUM(M2)");
        assertThat(canonical.timeRange().startTimestampSeconds()).isZero();
        assertThat(canonical.timeRange().endTimestampSeconds()).isEqualTo(86_399);
    }

    @Test
    void bucketedTimeSeriesCanonicalisesAsTimeSeriesWithTheBucketStep() {
        String physical = translator.translate(
                "SELECT ts, SUM(bytes) AS b FROM traffic WHERE ts >= 0 AND ts <= 86399 GROUP BY ts",
                catalog());

        CanonicalQueryObject canonical = new CalciteCanonicalObjectFactory()
                .extractCanonicalObjectFromSql(physical, new TimeDimensionMap(Set.of("D17")));

        assertThat(canonical.metadata().isTimeSeries()).isTrue();
        assertThat(canonical.metadata().userStepSeconds()).contains(300);
    }
}
