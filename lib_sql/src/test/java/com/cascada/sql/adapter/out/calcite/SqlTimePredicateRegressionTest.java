package com.cascada.sql.adapter.out.calcite;

import com.cascada.cache.domain.time.GapPlan;
import com.cascada.sql.domain.TimeDimensionMap;
import com.cascada.sql.exception.UnsupportedSqlException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SqlTimePredicateRegressionTest {
    private final CalciteCanonicalObjectFactory factory = new CalciteCanonicalObjectFactory();
    private final GapQueryBuilder gaps = new GapQueryBuilder("ts", 86_400);
    private final GapPlan day = new GapPlan(Optional.empty(), List.of(0L), Optional.empty());

    @Test
    void negatedBetweenCannotSupplyTheCanonicalWindow() {
        assertThatThrownBy(() -> factory.canonicalize(
                "SELECT SUM(x) FROM t WHERE ts NOT BETWEEN 100 AND 200"))
                .isInstanceOf(UnsupportedSqlException.class);
    }

    @Test
    void fractionalAndNegatedPredicatesSurviveCanonicalizationAndGapRewriting() {
        for (String predicate : List.of("ts >= 150.5", "ts < 86300.5",
                "ts NOT BETWEEN 100 AND 200", "ts BETWEEN SYMMETRIC 200 AND 100")) {
            String sql = "SELECT SUM(x) FROM t WHERE ts >= 0 AND ts <= 172799 AND " + predicate;
            var canonical = factory.canonicalize(sql);
            assertThat(canonical.timeRange().startTimestampSeconds()).isZero();
            assertThat(canonical.timeRange().endTimestampSeconds()).isEqualTo(172799);
            assertThat(canonical.hashComponents().filters()).hasSize(1);
            String preserved = canonical.hashComponents().filters().getFirst();
            assertThat(gaps.stripTimeConditions(sql)).contains(preserved);
            assertThat(gaps.buildGapQuery(sql, day)).contains(preserved).doesNotContain("172799");
        }
    }

    @Test
    void configuredTimeMapAppliesToBothPublicFactoryEntryPoints() {
        var configured = new CalciteCanonicalObjectFactory(new TimeDimensionMap(Set.of("evt")));
        String sql = "SELECT SUM(x) FROM t WHERE evt >= 10 AND evt <= 20";
        assertThat(configured.extractCanonicalObjectFromSql(sql).timeRange())
                .isEqualTo(configured.canonicalize(sql).timeRange());
    }

    @Test
    void reservedTimeColumnProducesAParseableGapPredicate() {
        String rewritten = new GapQueryBuilder("time", 86_400).buildGapQuery(
                "SELECT SUM(x) FROM t WHERE `time` >= 0 AND `time` <= 172799", day);
        var canonical = new CalciteCanonicalObjectFactory(new TimeDimensionMap(Set.of("time")))
                .canonicalize(rewritten);
        assertThat(canonical.timeRange().endTimestampSeconds()).isEqualTo(86399);
    }

    @Test
    void quotedColumnNamesStayColumnsInsteadOfBecomingContextFunctions() {
        String sql = "SELECT `user`, SUM(x) FROM t WHERE ts >= 0 AND ts <= 86399 GROUP BY `user`";
        String rendered = CalciteSql.unparse(CalciteSql.parseQuery(sql));
        assertThat(rendered).contains("`user`");
        assertThat(factory.canonicalize(rendered).timeRange().endTimestampSeconds()).isEqualTo(86399);
    }

    @Test
    void derivedTableWindowsBypassInsteadOfOverwritingInnerBounds() {
        String sql = "SELECT SUM(s.x) FROM (SELECT ts, x FROM t WHERE ts >= 5 AND ts <= 8) s "
                + "WHERE s.ts >= 0 AND s.ts <= 86399";
        assertThatThrownBy(() -> factory.canonicalize(sql)).isInstanceOf(UnsupportedSqlException.class);
        assertThatThrownBy(() -> gaps.buildGapQuery(sql, day)).isInstanceOf(UnsupportedSqlException.class);
    }
}
