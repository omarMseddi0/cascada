package com.cascada.sql.canonical;

import com.cascada.cache.domain.CanonicalQueryObject;
import com.cascada.cache.domain.hashing.QueryHashGenerator;
import com.cascada.cache.domain.merge.AggregateFunction;
import com.cascada.identity.domain.QueryHash;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the fixes for README caveats 4, 5 and 6: {@code HAVING}, {@code JOIN ... ON} conditions and
 * {@code DISTINCT} must all reach the logic hash (two queries differing only in one of them must
 * never share a cache entry), and the parsed aggregate function must survive aliasing so the merge
 * combines {@code MAX(latency) AS peak_latency} with MAX, not SUM.
 */
class CanonicalLogicSignatureTest {

    private final CalciteCanonicalObjectFactory factory = new CalciteCanonicalObjectFactory();
    private final QueryHashGenerator hashGenerator = new QueryHashGenerator();

    private QueryHash hashOf(String sql) {
        return hashGenerator.generateQueryHash(factory.extractCanonicalObjectFromSql(sql));
    }

    @Test
    void havingChangesTheLogicHash() {
        String withoutHaving = "SELECT appName, SUM(bytes) FROM traffic "
                + "WHERE ts >= 0 AND ts <= 86399 GROUP BY appName";
        String withHaving = "SELECT appName, SUM(bytes) FROM traffic "
                + "WHERE ts >= 0 AND ts <= 86399 GROUP BY appName HAVING SUM(bytes) > 100";

        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(withHaving);
        assertThat(canonical.logicSignature()).anyMatch(marker -> marker.startsWith("HAVING "));
        assertThat(hashOf(withoutHaving)).isNotEqualTo(hashOf(withHaving));
    }

    @Test
    void differentHavingThresholdsHashDifferently() {
        String having100 = "SELECT appName, SUM(bytes) FROM traffic "
                + "WHERE ts >= 0 AND ts <= 86399 GROUP BY appName HAVING SUM(bytes) > 100";
        String having200 = "SELECT appName, SUM(bytes) FROM traffic "
                + "WHERE ts >= 0 AND ts <= 86399 GROUP BY appName HAVING SUM(bytes) > 200";
        assertThat(hashOf(having100)).isNotEqualTo(hashOf(having200));
    }

    @Test
    void joinOnConditionChangesTheLogicHash() {
        String joinOnUser = "SELECT appName, SUM(bytes) FROM traffic t JOIN dims d ON t.userId = d.userId "
                + "WHERE ts >= 0 AND ts <= 86399 GROUP BY appName";
        String joinOnDevice = "SELECT appName, SUM(bytes) FROM traffic t JOIN dims d ON t.deviceId = d.deviceId "
                + "WHERE ts >= 0 AND ts <= 86399 GROUP BY appName";

        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(joinOnUser);
        assertThat(canonical.logicSignature()).anyMatch(marker -> marker.startsWith("JOIN ON "));
        assertThat(hashOf(joinOnUser)).isNotEqualTo(hashOf(joinOnDevice));
    }

    @Test
    void distinctChangesTheLogicHash() {
        String plain = "SELECT appName, SUM(bytes) FROM traffic "
                + "WHERE ts >= 0 AND ts <= 86399 GROUP BY appName";
        String distinct = "SELECT DISTINCT appName, SUM(bytes) FROM traffic "
                + "WHERE ts >= 0 AND ts <= 86399 GROUP BY appName";

        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(distinct);
        assertThat(canonical.logicSignature()).contains("DISTINCT");
        assertThat(hashOf(plain)).isNotEqualTo(hashOf(distinct));
    }

    @Test
    void queriesWithoutHavingJoinOrDistinctKeepAnEmptyLogicSignatureAndTheirHistoricalHash() {
        // No cold-cache event on upgrade: absent markers add nothing to the canonical string.
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT appName, SUM(bytes) FROM traffic WHERE ts >= 0 AND ts <= 86399 GROUP BY appName");
        assertThat(canonical.logicSignature()).isEmpty();
        assertThat(hashGenerator.buildCanonicalString(canonical, 300)).doesNotContain("logic_signature");
    }

    @Test
    void aliasedMinMaxKeepTheirParsedCombineFunction() {
        CanonicalQueryObject canonical = factory.extractCanonicalObjectFromSql(
                "SELECT appName, MAX(latency) AS peak_latency, MIN(latency) AS best_latency, SUM(bytes) "
                        + "FROM traffic WHERE ts >= 0 AND ts <= 86399 GROUP BY appName");

        assertThat(canonical.metadata().measureAggregates())
                .containsEntry("peak_latency", AggregateFunction.MAXIMUM)
                .containsEntry("best_latency", AggregateFunction.MINIMUM);
        assertThat(canonical.metadata().measureAggregates().values())
                .contains(AggregateFunction.SUM); // the unaliased SUM(bytes) column
    }
}
