package com.cascada.sql.canonical;

import com.cascada.cache.domain.CanonicalQueryObject;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exhaustive coverage of {@link CalciteCanonicalObjectFactory}, the Calcite heir to the sqlglot
 * {@code smart_sql_processor} + {@code create_canonical_object}. Every extraction concern is probed
 * with many SQL shapes: group-by, every aggregate kind, AVG decomposition, composite formulas, time
 * range via comparisons and BETWEEN, time-series detection with assorted bucket steps and raw grouping,
 * filter isolation, order/limit, multiple time-dimension names, and the bypass guards.
 */
class CalciteCanonicalObjectFactoryDeepTest {

    private final CalciteCanonicalObjectFactory factory = new CalciteCanonicalObjectFactory();

    // --- group by + aggregates -------------------------------------------------------------------

    @Test
    void extractsMultipleGroupByKeysSortedAndDeduped() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT region, appName, SUM(bytes) AS b FROM traffic "
                        + "WHERE ts >= 0 AND ts <= 100 GROUP BY appName, region");
        assertThat(canonical.hashComponents().groupBy()).containsExactly("appName", "region");
    }

    @Test
    void extractsEachAggregateKind() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT appName, SUM(a) AS s, MIN(b) AS mn, MAX(c) AS mx, COUNT(d) AS cnt FROM traffic "
                        + "WHERE ts >= 0 AND ts <= 100 GROUP BY appName");
        assertThat(canonical.hashComponents().aggregates())
                .containsExactlyInAnyOrder("SUM(a)", "MIN(b)", "MAX(c)", "COUNT(d)");
    }

    @Test
    void countStarIsCapturedAsAnAggregate() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT appName, COUNT(*) AS n FROM traffic WHERE ts >= 0 AND ts <= 100 GROUP BY appName");
        assertThat(canonical.hashComponents().aggregates()).containsExactly("COUNT(*)");
    }

    @Test
    void averageIsDecomposedIntoSumAndCountAndOriginalRecorded() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT deviceType, AVG(latency) AS avgLatency FROM traffic "
                        + "WHERE ts >= 0 AND ts <= 100 GROUP BY deviceType");
        assertThat(canonical.hashComponents().aggregates())
                .containsExactly("COUNT(latency)", "SUM(latency)");
        assertThat(canonical.metadata().originalAggregates()).containsExactly("AVG(latency)");
    }

    @Test
    void multipleAveragesEachDecompose() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT AVG(a) AS aa, AVG(b) AS bb FROM traffic WHERE ts >= 0 AND ts <= 100");
        assertThat(canonical.hashComponents().aggregates())
                .containsExactlyInAnyOrder("SUM(a)", "COUNT(a)", "SUM(b)", "COUNT(b)");
    }

    @Test
    void capturesCompositeAdditionFormula() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT appName, SUM(a) + SUM(b) AS total FROM traffic "
                        + "WHERE ts >= 0 AND ts <= 100 GROUP BY appName");
        assertThat(canonical.metadata().compositeAliases()).containsKey("total");
        assertThat(canonical.metadata().compositeAliases().get("total")).contains("SUM(a)").contains("SUM(b)");
        assertThat(canonical.hashComponents().aggregates()).contains("SUM(a)", "SUM(b)");
    }

    @Test
    void capturesCompositeDivisionFormulaForAComputedRatio() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT appName, SUM(a) / COUNT(b) AS ratio FROM traffic "
                        + "WHERE ts >= 0 AND ts <= 100 GROUP BY appName");
        assertThat(canonical.metadata().compositeAliases()).containsKey("ratio");
        assertThat(canonical.hashComponents().aggregates()).contains("SUM(a)", "COUNT(b)");
    }

    @Test
    void bareAggregateWithoutAliasIsNotTreatedAsComposite() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT appName, SUM(bytes) FROM traffic WHERE ts >= 0 AND ts <= 100 GROUP BY appName");
        assertThat(canonical.metadata().compositeAliases()).isEmpty();
        assertThat(canonical.hashComponents().aggregates()).containsExactly("SUM(bytes)");
    }

    // --- time range ------------------------------------------------------------------------------

    @Test
    void extractsTimeRangeFromGreaterEqualAndLessEqual() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT SUM(b) AS s FROM traffic WHERE ts >= 100 AND ts <= 500");
        assertThat(canonical.timeRange().startTimestampSeconds()).isEqualTo(100);
        assertThat(canonical.timeRange().endTimestampSeconds()).isEqualTo(500);
    }

    @Test
    void extractsTimeRangeFromStrictInequalities() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT SUM(b) AS s FROM traffic WHERE ts > 100 AND ts < 500");
        assertThat(canonical.timeRange().startTimestampSeconds()).isEqualTo(100);
        assertThat(canonical.timeRange().endTimestampSeconds()).isEqualTo(500);
    }

    @Test
    void extractsTimeRangeFromBetween() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT SUM(b) AS s FROM traffic WHERE ts BETWEEN 10 AND 20");
        assertThat(canonical.timeRange().startTimestampSeconds()).isEqualTo(10);
        assertThat(canonical.timeRange().endTimestampSeconds()).isEqualTo(20);
    }

    @Test
    void recognisesAlternativeTimeColumnNames() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT SUM(b) AS s FROM traffic WHERE starttime >= 0 AND stoptime <= 999");
        assertThat(canonical.timeRange().startTimestampSeconds()).isZero();
        assertThat(canonical.timeRange().endTimestampSeconds()).isEqualTo(999);
    }

    @Test
    void honoursACustomTimeDimensionMap() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT C5, SUM(M2) AS s FROM t WHERE D17 >= 0 AND D17 <= 100 GROUP BY C5",
                new TimeDimensionMap(Set.of("D17")));
        assertThat(canonical.timeRange().endTimestampSeconds()).isEqualTo(100);
    }

    // --- filters ---------------------------------------------------------------------------------

    @Test
    void isolatesNonTimePredicatesAsFiltersSortedAndDeduped() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT SUM(b) AS s FROM traffic "
                        + "WHERE ts >= 0 AND ts <= 100 AND region = 'eu' AND deviceType = 'mobile'");
        assertThat(canonical.hashComponents().filters())
                .containsExactly("deviceType = 'mobile'", "region = 'eu'");
    }

    @Test
    void keepsAnInPredicateAsAFilter() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT SUM(b) AS s FROM traffic WHERE ts >= 0 AND ts <= 100 AND region IN ('eu', 'us')");
        assertThat(canonical.hashComponents().filters()).hasSize(1);
        assertThat(canonical.hashComponents().filters().get(0)).contains("region IN");
    }

    // --- time-series detection -------------------------------------------------------------------

    @Test
    void detectsTimeSeriesAndStepAcrossSeveralBucketWidths() {
        for (int step : new int[]{60, 300, 600, 3600}) {
            CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                    "SELECT FLOOR(ts / " + step + ") * " + step + " AS bucket, SUM(b) AS s FROM traffic "
                            + "WHERE ts >= 0 AND ts <= 100 GROUP BY FLOOR(ts / " + step + ") * " + step);
            assertThat(canonical.metadata().isTimeSeries()).isTrue();
            assertThat(canonical.metadata().userStepSeconds()).contains(step);
            assertThat(canonical.metadata().preserveRawTimeSeries()).isFalse();
        }
    }

    @Test
    void detectsTimeSeriesThroughACastWrappedBucket() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT CAST(FLOOR(ts / 300) * 300 AS BIGINT) AS bucket, SUM(b) AS s FROM traffic "
                        + "WHERE ts >= 0 AND ts <= 100 GROUP BY CAST(FLOOR(ts / 300) * 300 AS BIGINT)");
        assertThat(canonical.metadata().isTimeSeries()).isTrue();
        assertThat(canonical.metadata().userStepSeconds()).contains(300);
    }

    @Test
    void groupingByRawTimeColumnIsTimeSeriesWithPreserveRaw() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT ts, SUM(b) AS s FROM traffic WHERE ts >= 0 AND ts <= 100 GROUP BY ts");
        assertThat(canonical.metadata().isTimeSeries()).isTrue();
        assertThat(canonical.metadata().preserveRawTimeSeries()).isTrue();
        assertThat(canonical.metadata().userStepSeconds()).isEmpty();
    }

    @Test
    void groupingByPlainDimensionIsNotTimeSeries() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT appName, SUM(b) AS s FROM traffic WHERE ts >= 0 AND ts <= 100 GROUP BY appName");
        assertThat(canonical.metadata().isTimeSeries()).isFalse();
    }

    // --- order by / limit ------------------------------------------------------------------------

    @Test
    void capturesDescendingOrderAndLimit() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT appName, SUM(b) AS s FROM traffic WHERE ts >= 0 AND ts <= 100 "
                        + "GROUP BY appName ORDER BY s DESC LIMIT 5");
        assertThat(canonical.postProcessing().limit()).contains(5);
        assertThat(canonical.postProcessing().orderBy()).hasSize(1);
        assertThat(canonical.postProcessing().orderBy().get(0).ascending()).isFalse();
    }

    @Test
    void capturesAscendingOrderByDefault() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT appName, SUM(b) AS s FROM traffic WHERE ts >= 0 AND ts <= 100 "
                        + "GROUP BY appName ORDER BY appName");
        assertThat(canonical.postProcessing().orderBy().get(0).ascending()).isTrue();
    }

    @Test
    void capturesLimitWithoutOrderBy() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT SUM(b) AS s FROM traffic WHERE ts >= 0 AND ts <= 100 LIMIT 7");
        assertThat(canonical.postProcessing().limit()).contains(7);
        assertThat(canonical.postProcessing().orderBy()).isEmpty();
    }

    @Test
    void noOrderOrLimitYieldsEmptyPostProcessing() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT SUM(b) AS s FROM traffic WHERE ts >= 0 AND ts <= 100");
        assertThat(canonical.postProcessing().limit()).isEmpty();
        assertThat(canonical.postProcessing().orderBy()).isEmpty();
    }

    // --- physical SQL + signatures ---------------------------------------------------------------

    @Test
    void storesTheRawPhysicalSqlVerbatim() {
        String sql = "SELECT SUM(b) AS s FROM traffic WHERE ts >= 0 AND ts <= 100";
        assertThat(factory.extractCanonicalObjectFromSql(sql).physicalSql()).isEqualTo(sql);
    }

    @Test
    void capturesTheSourceTableInTheSourceSignature() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT SUM(b) AS s FROM traffic WHERE ts >= 0 AND ts <= 100");
        assertThat(canonical.sourceSignature()).contains("traffic");
    }

    // --- bypass guards ---------------------------------------------------------------------------

    @Test
    void bypassesWhenThereIsNoTimeRange() {
        assertThatThrownBy(() -> factory.extractCanonicalObjectFromSql(
                "SELECT appName, SUM(b) FROM traffic GROUP BY appName"))
                .isInstanceOf(UnsupportedSqlException.class);
    }

    @Test
    void bypassesWhenOnlyALowerBoundIsPresent() {
        assertThatThrownBy(() -> factory.extractCanonicalObjectFromSql(
                "SELECT SUM(b) FROM traffic WHERE ts >= 0"))
                .isInstanceOf(UnsupportedSqlException.class);
    }

    @Test
    void bypassesUnparseableSql() {
        assertThatThrownBy(() -> factory.extractCanonicalObjectFromSql("NOT SQL AT ALL ;;;"))
                .isInstanceOf(UnsupportedSqlException.class);
    }

    @Test
    void bypassesNonSelectStatements() {
        assertThatThrownBy(() -> factory.extractCanonicalObjectFromSql("DROP TABLE traffic"))
                .isInstanceOf(UnsupportedSqlException.class);
    }

    @Test
    void bypassesAnInsertStatement() {
        assertThatThrownBy(() -> factory.extractCanonicalObjectFromSql(
                "INSERT INTO t (a) VALUES (1)"))
                .isInstanceOf(UnsupportedSqlException.class);
    }
}
