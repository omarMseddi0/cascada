package com.cascada.sql.translate;

import com.cascada.cache.domain.CanonicalQueryObject;
import com.cascada.sql.canonical.CalciteCanonicalObjectFactory;
import com.cascada.sql.canonical.TimeDimensionMap;
import com.cascada.sql.canonical.UnsupportedSqlException;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies the logical→physical rewrite ported from {@code _transform_ast}: the customer writes
 * logical table/column names and never a path or a physical name; the platform translates. Then the
 * canonical object is extracted from the TRANSLATED SQL (physical names), matching
 * {@code analyze_and_rewrite}.
 */
class LogicalToPhysicalSqlTranslatorTest {

    private final LogicalToPhysicalSqlTranslator translator = new LogicalToPhysicalSqlTranslator(300);

    private TableCatalog catalogWithTraffic() {
        return new TableCatalog().register(RegisteredTable.of(
                "traffic", "/lakehouse/traffic",
                Map.of("country", "C5", "ts", "D17", "bytes", "M2"),
                "ts"));
    }

    @Test
    void translatesLogicalColumnsTableAndKeepsTheAnswerable() {
        String physical = translator.translate(
                "SELECT country, SUM(bytes) AS b FROM traffic "
                        + "WHERE ts >= 0 AND ts <= 100 AND country = 'TN' GROUP BY country",
                catalogWithTraffic());

        assertThat(physical).contains("C5");                 // country -> C5
        assertThat(physical).contains("SUM(M2)");            // bytes -> M2
        assertThat(physical).contains("D17 >= 0");           // ts -> D17 in the filter
        assertThat(physical).contains("delta.`/lakehouse/traffic`"); // table -> Delta path
        // the customer's logical names are gone
        assertThat(physical).doesNotContain("country");
        assertThat(physical).doesNotContain("bytes");
        assertThat(physical).doesNotContain("FROM traffic");
    }

    @Test
    void wrapsTheTimeColumnInABucketExpressionWhenItIsGrouped() {
        String physical = translator.translate(
                "SELECT ts, country, SUM(bytes) AS b FROM traffic "
                        + "WHERE ts >= 0 AND ts <= 100 GROUP BY ts, country",
                catalogWithTraffic());

        assertThat(physical).contains("FLOOR(D17 / 300) * 300");
        assertThat(physical).contains("AS BIGINT");
    }

    @Test
    void bypassesAnUnregisteredTable() {
        assertThatThrownBy(() -> translator.translate(
                "SELECT x FROM unknown_table WHERE ts >= 0 AND ts <= 1 GROUP BY x", catalogWithTraffic()))
                .isInstanceOf(UnsupportedSqlException.class);
    }

    @Test
    void translatedSqlCanonicalisesWithPhysicalNames() {
        // End-to-end: logical SQL -> physical SQL -> canonical object uses physical names (C5), as in
        // analyze_and_rewrite. The cache key is therefore resistant to logical-name/schema changes.
        String physical = translator.translate(
                "SELECT country, SUM(bytes) AS b FROM traffic "
                        + "WHERE ts >= 0 AND ts <= 86399 AND country = 'TN' GROUP BY country",
                catalogWithTraffic());

        // physical time column is D17, so canonicalise with D17 as the time dimension
        CanonicalQueryObject canonical = new CalciteCanonicalObjectFactory()
                .extractCanonicalObjectFromSql(physical, new TimeDimensionMap(Set.of("D17")));

        assertThat(canonical.hashComponents().groupBy()).containsExactly("C5");
        assertThat(canonical.hashComponents().aggregates()).containsExactly("SUM(M2)");
        assertThat(canonical.timeRange().startTimestampSeconds()).isZero();
        assertThat(canonical.timeRange().endTimestampSeconds()).isEqualTo(86_399);
    }
}
