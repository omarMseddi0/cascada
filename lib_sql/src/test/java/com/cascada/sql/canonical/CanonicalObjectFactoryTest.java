package com.cascada.sql.canonical;

import com.cascada.cache.domain.CanonicalQueryObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies canonical-object extraction: group-by, AVG→SUM/COUNT, composite aliases, time range,
 * time-series detection with user step, ordering/limit, and the bypass-on-unsupported behaviour.
 */
class CanonicalObjectFactoryTest {

    private final CalciteCanonicalObjectFactory factory = new CalciteCanonicalObjectFactory();

    @Test
    void extractsGlobalAggregateGroupByAggregatesAndTimeRange() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT appName, SUM(bytesFromServer) AS total FROM traffic "
                        + "WHERE ts >= 100 AND ts <= 200 GROUP BY appName");

        assertThat(canonical.hashComponents().groupBy()).containsExactly("appName");
        assertThat(canonical.hashComponents().aggregates()).containsExactly("SUM(bytesFromServer)");
        assertThat(canonical.timeRange().startTimestampSeconds()).isEqualTo(100);
        assertThat(canonical.timeRange().endTimestampSeconds()).isEqualTo(200);
        assertThat(canonical.metadata().isTimeSeries()).isFalse();
    }

    @Test
    void decomposesAverageIntoSumAndCountAndRecordsOriginal() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT deviceType, AVG(latencyMillis) AS avgLatency FROM traffic "
                        + "WHERE ts >= 0 AND ts <= 86399 GROUP BY deviceType");

        // AVG is expanded for storage/hash; sorted+deduped -> COUNT then SUM.
        assertThat(canonical.hashComponents().aggregates())
                .containsExactly("COUNT(latencyMillis)", "SUM(latencyMillis)");
        assertThat(canonical.metadata().originalAggregates()).isNotEmpty();
    }

    @Test
    void detectsTimeSeriesAndUserStepFromAFloorBucketExpression() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT FLOOR(ts / 600) * 600 AS bucket, SUM(bytes) AS b FROM traffic "
                        + "WHERE ts >= 0 AND ts <= 86399 GROUP BY FLOOR(ts / 600) * 600");

        assertThat(canonical.metadata().isTimeSeries()).isTrue();
        assertThat(canonical.metadata().userStepSeconds()).contains(600);
    }

    @Test
    void capturesCompositeAliasFormula() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT appName, SUM(a) + SUM(b) AS totalBytes FROM traffic "
                        + "WHERE ts >= 0 AND ts <= 100 GROUP BY appName");

        assertThat(canonical.metadata().compositeAliases()).containsKey("totalBytes");
        assertThat(canonical.hashComponents().aggregates()).contains("SUM(a)", "SUM(b)");
    }

    @Test
    void capturesOrderByAndLimitForDeferredPostProcessing() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT appName, SUM(bytes) AS b FROM traffic WHERE ts >= 0 AND ts <= 100 "
                        + "GROUP BY appName ORDER BY b DESC LIMIT 2");

        assertThat(canonical.postProcessing().limit()).contains(2);
        assertThat(canonical.postProcessing().orderBy()).hasSize(1);
        assertThat(canonical.postProcessing().orderBy().get(0).ascending()).isFalse();
    }

    @Test
    void treatsTimePredicateSeparatelyFromOtherFilters() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT appName, SUM(bytes) AS b FROM traffic "
                        + "WHERE ts >= 0 AND ts <= 100 AND region = 'eu' GROUP BY appName");

        assertThat(canonical.hashComponents().filters()).containsExactly("region = 'eu'");
    }

    @Test
    void bypassesQueriesWithoutAnExtractableTimeRange() {
        assertThatThrownBy(() -> factory.extractCanonicalObjectFromSql(
                "SELECT appName, SUM(bytes) FROM traffic GROUP BY appName"))
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
}
