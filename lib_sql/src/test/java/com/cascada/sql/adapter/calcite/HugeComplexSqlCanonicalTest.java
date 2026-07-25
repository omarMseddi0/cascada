package com.cascada.sql.canonical;

import com.cascada.cache.domain.CanonicalQueryObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Large, realistic, "not a toy" SQL fed at the canonical factory: a wide time-series projection with
 * every aggregate kind, an AVG that must decompose, a composite arithmetic alias, mixed
 * equality/IN/range filters, and order/limit — proving the Calcite extraction stays correct at scale.
 * Also proves the safety contract: genuinely non-cacheable shapes (multi-CTE) bypass cleanly rather
 * than being mis-canonicalised.
 */
class HugeComplexSqlCanonicalTest {

    private final CalciteCanonicalObjectFactory factory = new CalciteCanonicalObjectFactory();

    private static final String HUGE_TIME_SERIES = """
            SELECT
                FLOOR(ts / 300) * 300 AS bucket,
                appName,
                deviceType,
                SUM(bytesIn) AS sin,
                SUM(bytesOut) AS sout,
                COUNT(sessionId) AS sessions,
                MIN(latency) AS minLat,
                MAX(latency) AS maxLat,
                AVG(latency) AS avgLat,
                SUM(bytesIn) + SUM(bytesOut) AS totalBytes
            FROM traffic
            WHERE ts >= 1000 AND ts <= 86399
              AND region = 'eu'
              AND deviceType IN ('mobile', 'tablet')
              AND appName <> 'internal'
            GROUP BY FLOOR(ts / 300) * 300, appName, deviceType
            ORDER BY totalBytes DESC
            LIMIT 100
            """;

    @Test
    void extractsTheFullGroupBySet() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(HUGE_TIME_SERIES);
        assertThat(canonical.hashComponents().groupBy())
                .anySatisfy(value -> assertThat(value).contains("FLOOR(ts / 300) * 300"));
        assertThat(canonical.hashComponents().groupBy()).contains("appName", "deviceType");
    }

    @Test
    void decomposesAverageAndKeepsEveryOtherAggregate() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(HUGE_TIME_SERIES);
        assertThat(canonical.hashComponents().aggregates()).contains(
                "SUM(bytesIn)", "SUM(bytesOut)", "COUNT(sessionId)",
                "MIN(latency)", "MAX(latency)",
                "SUM(latency)", "COUNT(latency)"); // AVG(latency) -> SUM + COUNT
        assertThat(canonical.metadata().originalAggregates()).contains("AVG(latency)");
    }

    @Test
    void capturesTheCompositeArithmeticAlias() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(HUGE_TIME_SERIES);
        assertThat(canonical.metadata().compositeAliases()).containsKey("totalBytes");
        assertThat(canonical.metadata().compositeAliases().get("totalBytes"))
                .contains("SUM(bytesIn)").contains("SUM(bytesOut)");
    }

    @Test
    void extractsTimeRangeTimeSeriesStepAndPostProcessing() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(HUGE_TIME_SERIES);
        assertThat(canonical.timeRange().startTimestampSeconds()).isEqualTo(1000);
        assertThat(canonical.timeRange().endTimestampSeconds()).isEqualTo(86_399);
        assertThat(canonical.metadata().isTimeSeries()).isTrue();
        assertThat(canonical.metadata().userStepSeconds()).contains(300);
        assertThat(canonical.postProcessing().limit()).contains(100);
        assertThat(canonical.postProcessing().orderBy()).hasSize(1);
        assertThat(canonical.postProcessing().orderBy().get(0).ascending()).isFalse();
    }

    @Test
    void isolatesAllNonTimeFiltersIncludingInAndInequality() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(HUGE_TIME_SERIES);
        assertThat(canonical.hashComponents().filters()).anySatisfy(f -> assertThat(f).contains("region = 'eu'"));
        assertThat(canonical.hashComponents().filters()).anySatisfy(f -> assertThat(f).contains("deviceType IN"));
        assertThat(canonical.hashComponents().filters()).anySatisfy(f -> assertThat(f).contains("appName <> 'internal'"));
    }

    @Test
    void aHugeGlobalAggregateWithManyFiltersCanonicalisesWithoutTimeSeries() {
        String hugeGlobal = """
                SELECT
                    country,
                    SUM(revenue) AS rev,
                    COUNT(orderId) AS orders,
                    AVG(basket) AS avgBasket
                FROM sales
                WHERE ts >= 0 AND ts <= 604799
                  AND channel IN ('web', 'app', 'store')
                  AND country <> 'XX'
                  AND revenue > 0
                GROUP BY country
                ORDER BY rev DESC
                LIMIT 50
                """;
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(hugeGlobal);
        assertThat(canonical.metadata().isTimeSeries()).isFalse();
        assertThat(canonical.hashComponents().groupBy()).containsExactly("country");
        assertThat(canonical.hashComponents().aggregates())
                .contains("SUM(revenue)", "COUNT(orderId)", "SUM(basket)", "COUNT(basket)");
        assertThat(canonical.timeRange().endTimestampSeconds()).isEqualTo(604_799);
    }

    @Test
    void aMultiCteQueryBypassesCleanlyRatherThanBeingMisCanonicalised() {
        // A WITH query is not a simple SELECT; the safety contract is to bypass to Spark, not guess.
        String cte = """
                WITH base AS (SELECT ts, bytes FROM traffic WHERE ts >= 0 AND ts <= 100)
                SELECT SUM(bytes) AS b FROM base
                """;
        assertThatThrownBy(() -> factory.extractCanonicalObjectFromSql(cte))
                .isInstanceOf(UnsupportedSqlException.class);
    }
}
