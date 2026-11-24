package com.cascada.sql.adapter.out.calcite;

import com.cascada.cache.domain.model.cube.CubeInterval;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ExactCubeSqlAdapterTest {
    private static final ExactCubeSqlAdapter ADAPTER = new ExactCubeSqlAdapter(Set.of("/facts"), "ts");
    private static final String FILTER = " FROM delta.`/facts` WHERE ts > -1 AND ts <= 86399";

    @Test void translatesInclusiveAndStrictEpochBoundsToHalfOpenInterval() {
        var query = ADAPTER.recognize("SELECT region, AVG(amount) AS mean" + FILTER
                + " GROUP BY region ORDER BY mean DESC, region LIMIT 10").orElseThrow();
        assertThat(query.interval()).isEqualTo(new CubeInterval(0, 86400));
        assertThat(query.grain()).containsExactly("region");
        assertThat(query.limit()).isEqualTo(10);
    }

    @Test void declinesSpecialGroupingAndUnprovenExpressions() {
        assertThat(ADAPTER.recognize("SELECT SUM(amount) AS total" + FILTER + " GROUP BY ALL")).isEmpty();
        assertThat(ADAPTER.recognize("SELECT STREAM region, SUM(amount) AS total" + FILTER
                + " GROUP BY region")).isEmpty();
        assertThat(ADAPTER.recognize("SELECT region, SUM(amount) AS total" + FILTER
                + " AND RAND() > 0.5 GROUP BY region")).isEmpty();
        assertThat(ADAPTER.recognize("SELECT region, COUNT(*) AS total" + FILTER + " GROUP BY region")).isEmpty();
        assertThat(ADAPTER.recognize("WITH RECURSIVE totals AS (SELECT SUM(amount) AS total" + FILTER
                + ") SELECT total FROM totals")).isEmpty();
    }

    @Test void resolvesAliasLocalityAcrossSeededCteGraphs() {
        var random = new java.util.Random(559_821);
        for (int i = 0; i < 128; i++) {
            String alias = "s" + random.nextInt(10_000);
            String cte = "node" + i;
            String base = "SELECT " + alias + ".region AS area, SUM(" + alias + ".amount) AS total"
                    + " FROM delta.`/facts` " + alias + " WHERE " + alias + ".ts >= 0 AND " + alias
                    + ".ts < 86400 AND " + alias + ".device = 'mobile' GROUP BY " + alias + ".region";
            String sql = "WITH " + cte + " AS (" + base + ") SELECT a.area, b.total FROM " + cte
                    + " a INNER JOIN " + cte + " b ON a.area = b.area";
            var candidate = ADAPTER.recognize(sql).orElseThrow();
            assertThat(candidate.predicateTypes()).containsEntry("device", "string");
            assertThat(candidate.keyFilters()).singleElement().satisfies(filter ->
                    assertThat(filter.literal()).isEqualTo("mobile"));
            assertThat(candidate.outerSql()).contains(candidate.outerRelation(), "INNER JOIN");
            assertThat(ADAPTER.recognize(sql.replace(alias + ".device", "outer_alias.device"))).isEmpty();
            assertThat(ADAPTER.recognize("WITH " + cte + " AS (" + base + ") SELECT 1")).isEmpty();
        }
    }

    @Test void normalizesNullSafeOperatorOutsideLiteralText() {
        String rendered = CalciteSql.unparse(CalciteSql.parseQuery("SELECT '<=>' AS literal FROM f WHERE a <=> b"));
        assertThat(rendered).contains("'<=>'", "IS NOT DISTINCT FROM");
    }
}
