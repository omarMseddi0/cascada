package com.cascada.sql.adapter.calcite;

import com.cascada.sql.domain.TimeDimensionMap;
import com.cascada.sql.domain.UnsupportedSqlException;
import com.cascada.cache.domain.query.CanonicalQueryObject;
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
        // ts > 100 excludes second 100 and ts < 500 excludes second 500: the inclusive canonical
        // window is [101, 499]. Mapping strict bounds to the same range as >=/<= would make the
        // two queries share a cache entry and serve each other's boundary rows.
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT SUM(b) AS s FROM traffic WHERE ts > 100 AND ts < 500");
        assertThat(canonical.timeRange().startTimestampSeconds()).isEqualTo(101);
        assertThat(canonical.timeRange().endTimestampSeconds()).isEqualTo(499);
    }

    @Test
    void fractionalTimeComparisonsRemainPartOfTheFilterSignature() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT SUM(b) FROM traffic WHERE ts >= 0 AND ts <= 100 AND ts > 1.5");

        assertThat(canonical.timeRange().startTimestampSeconds()).isZero();
        assertThat(canonical.hashComponents().filters()).anySatisfy(
                filter -> assertThat(filter).contains("ts > 1.5"));
    }

    @Test
    void strictAndInclusiveBoundsNeverCollideOnTheSameCanonicalRange() {
        CanonicalQueryObject strict = factory.extractCanonicalObjectFromSql(
                "SELECT SUM(b) AS s FROM traffic WHERE ts > 100 AND ts <= 500");
        CanonicalQueryObject inclusive = factory.extractCanonicalObjectFromSql(
                "SELECT SUM(b) AS s FROM traffic WHERE ts >= 100 AND ts <= 500");
        assertThat(strict.timeRange()).isNotEqualTo(inclusive.timeRange());
    }

    @Test
    void extractsTimeRangeWhenTheLiteralIsOnTheLeftOfTheComparison() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT SUM(b) AS s FROM traffic WHERE 100 <= ts AND 500 >= ts");
        assertThat(canonical.timeRange().startTimestampSeconds()).isEqualTo(100);
        assertThat(canonical.timeRange().endTimestampSeconds()).isEqualTo(500);
    }

    @Test
    void tightestBoundWinsWhenSeveralPredicatesConstrainTheSameSide() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT SUM(b) AS s FROM traffic WHERE ts >= 150 AND ts >= 100 AND ts <= 500 AND ts <= 600");
        assertThat(canonical.timeRange().startTimestampSeconds()).isEqualTo(150);
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
    void reversedBetweenStaysAnOrdinaryFilterInsteadOfBecomingATimeRange() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT SUM(b) AS s FROM traffic WHERE ts >= 0 AND ts <= 100 "
                        + "AND 86400 BETWEEN ts AND 172799");

        assertThat(canonical.timeRange().startTimestampSeconds()).isZero();
        assertThat(canonical.timeRange().endTimestampSeconds()).isEqualTo(100);
        assertThat(canonical.hashComponents().filters())
                .anySatisfy(filter -> assertThat(filter).contains("86400 BETWEEN ASYMMETRIC ts AND 172799"));
    }

    @Test
    void reversedBetweenAloneDoesNotSupplyAnExtractableTimeRange() {
        assertThatThrownBy(() -> factory.extractCanonicalObjectFromSql(
                "SELECT SUM(b) AS s FROM traffic WHERE 86400 BETWEEN ts AND 172799"))
                .isInstanceOf(UnsupportedSqlException.class);
    }

    @Test
    void strictComparisonsAtLongBoundariesBypassInsteadOfWrapping() {
        assertThatThrownBy(() -> factory.extractCanonicalObjectFromSql(
                "SELECT SUM(b) FROM traffic WHERE ts > 9223372036854775807 "
                        + "AND ts <= 9223372036854775807"))
                .isInstanceOf(UnsupportedSqlException.class);
        assertThatThrownBy(() -> factory.extractCanonicalObjectFromSql(
                "SELECT SUM(b) FROM traffic WHERE ts < -9223372036854775808 "
                        + "AND ts >= -9223372036854775808"))
                .isInstanceOf(UnsupportedSqlException.class);
    }

    @Test
    void outOfRangeTimeLiteralsBypassWithASupportedSqlException() {
        assertThatThrownBy(() -> factory.extractCanonicalObjectFromSql(
                "SELECT SUM(b) FROM traffic WHERE ts > 9223372036854775808 "
                        + "AND ts <= 9223372036854775810"))
                .isInstanceOf(UnsupportedSqlException.class);
    }

    @Test
    void recognisesEachAlternativeTimeColumnWhenUsedByItself() {
        for (String timeColumn : new String[]{"starttime", "stoptime", "timestamp"}) {
            CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                    "SELECT SUM(b) AS s FROM traffic WHERE " + timeColumn + " >= 0 AND "
                            + timeColumn + " <= 999");

            assertThat(canonical.timeRange().startTimestampSeconds()).isZero();
            assertThat(canonical.timeRange().endTimestampSeconds()).isEqualTo(999);
        }
    }

    @Test
    void bypassesTimePredicatesAcrossMultipleSourcesInsteadOfCombiningTheirWindows() {
        String first = "SELECT SUM(a.amount) FROM events a, sessions b "
                + "WHERE a.ts >= 0 AND a.ts <= 99 AND b.ts >= 50 AND b.ts <= 149";
        String second = "SELECT SUM(a.amount) FROM events a, sessions b "
                + "WHERE a.ts >= 50 AND a.ts <= 149 AND b.ts >= 0 AND b.ts <= 99";

        assertThatThrownBy(() -> factory.extractCanonicalObjectFromSql(first))
                .isInstanceOf(UnsupportedSqlException.class)
                .hasMessageContaining("multiple sources");
        assertThatThrownBy(() -> factory.extractCanonicalObjectFromSql(second))
                .isInstanceOf(UnsupportedSqlException.class)
                .hasMessageContaining("multiple sources");
    }

    @Test
    void rejectsBoundsFromDifferentConfiguredTimeColumns() {
        assertThatThrownBy(() -> factory.extractCanonicalObjectFromSql(
                "SELECT SUM(b) FROM traffic WHERE ts >= 0 AND ts <= 100 "
                        + "AND stoptime >= 50 AND stoptime <= 80"))
                .isInstanceOf(UnsupportedSqlException.class)
                .hasMessageContaining("multiple time columns");
    }

    @Test
    void doesNotTreatProjectionAliasDeclarationsAsTimeColumnReferences() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT SUM(b) AS stoptime FROM traffic WHERE ts >= 0 AND ts <= 86399");

        assertThat(canonical.timeRange().endTimestampSeconds()).isEqualTo(86_399);
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
    void rejectsBucketExpressionsWhoseMultiplierOrStepCannotBePreserved() {
        for (String bucket : new String[]{
                "FLOOR(ts / 300) * 1",
                "FLOOR(ts / 600) * 300",
                "FLOOR(ts / 2147483648) * 2147483648",
                "FLOOR(ts / 0) * 0"}) {
            String sql = "SELECT " + bucket + " AS ts, SUM(b) AS s FROM traffic "
                    + "WHERE ts >= 0 AND ts <= 100 GROUP BY " + bucket;
            assertThatThrownBy(() -> factory.extractCanonicalObjectFromSql(sql))
                    .as("unsupported bucket expression: %s", bucket)
                    .isInstanceOf(UnsupportedSqlException.class);
        }
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
    void oversizedAndUnresolvedLimitsBypassInsteadOfNarrowingOrDisappearing() {
        assertThatThrownBy(() -> factory.extractCanonicalObjectFromSql(
                "SELECT SUM(b) FROM traffic WHERE ts >= 0 AND ts <= 100 LIMIT 4294967296"))
                .isInstanceOf(UnsupportedSqlException.class);
        assertThatThrownBy(() -> factory.extractCanonicalObjectFromSql(
                "SELECT SUM(b) FROM traffic WHERE ts >= 0 AND ts <= 100 LIMIT ?"))
                .isInstanceOf(UnsupportedSqlException.class);
        assertThatThrownBy(() -> factory.extractCanonicalObjectFromSql(
                "SELECT SUM(b) FROM traffic WHERE ts >= 0 AND ts <= 100 LIMIT 9223372036854775808"))
                .isInstanceOf(UnsupportedSqlException.class);
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

    @Test
    void preservesSelectProjectionOrderAndDuplicateSlots() {
        CanonicalQueryObject first = factory.extractCanonicalObjectFromSql(
                "SELECT city, SUM(x) AS total, city FROM traffic "
                        + "WHERE ts >= 0 AND ts <= 100 GROUP BY city");
        CanonicalQueryObject reversed = factory.extractCanonicalObjectFromSql(
                "SELECT SUM(x) AS total, city, city FROM traffic "
                        + "WHERE ts >= 0 AND ts <= 100 GROUP BY city");

        assertThat(first.projectionSignature()).containsExactly("city", "SUM(x) total", "city");
        assertThat(reversed.projectionSignature()).containsExactly("SUM(x) total", "city", "city");
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
