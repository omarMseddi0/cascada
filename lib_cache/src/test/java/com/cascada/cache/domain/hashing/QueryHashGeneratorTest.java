package com.cascada.cache.domain.hashing;

import com.cascada.cache.domain.CacheConstants;
import com.cascada.cache.domain.CanonicalQueryObject;
import com.cascada.cache.domain.HashComponents;
import com.cascada.cache.domain.PostProcessing;
import com.cascada.cache.domain.QueryMetadata;
import com.cascada.cache.domain.TimeRange;
import com.cascada.identity.domain.QueryHash;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the two correctness properties of the logic hash (ported from {@code cache_hashing.py}):
 * it is independent of the time range, and independent of clause ordering, but sensitive to the
 * actual intent (group-by, aggregates, filters, step, composite aliases).
 */
class QueryHashGeneratorTest {

    private final QueryHashGenerator generator = new QueryHashGenerator();

    private CanonicalQueryObject timeSeriesQuery(TimeRange timeRange, List<String> groupBy,
                                                 List<String> aggregates, List<String> filters) {
        HashComponents components = HashComponents.of(groupBy, aggregates, filters);
        QueryMetadata metadata = QueryMetadata.timeSeries(600);
        return CanonicalQueryObject.of(components, timeRange, PostProcessing.none(), metadata);
    }

    @Test
    void producesAValidLowercaseMd5Digest() {
        QueryHash hash = generator.generateQueryHash(
                timeSeriesQuery(new TimeRange(0, 86_399), List.of("appName"), List.of("SUM(bytes)"), List.of()));
        assertThat(hash.value()).matches("^[0-9a-f]{32}$");
    }

    @Test
    void hashIsIndependentOfTheTimeRange() {
        QueryHash oneDay = generator.generateQueryHash(
                timeSeriesQuery(new TimeRange(0, 86_399), List.of("appName"), List.of("SUM(bytes)"), List.of()));
        QueryHash sevenDays = generator.generateQueryHash(
                timeSeriesQuery(new TimeRange(0, 7 * 86_400L - 1), List.of("appName"), List.of("SUM(bytes)"), List.of()));
        assertThat(oneDay).isEqualTo(sevenDays);
    }

    @Test
    void hashIsIndependentOfGroupByAndFilterOrdering() {
        QueryHash forward = generator.generateQueryHash(timeSeriesQuery(new TimeRange(0, 86_399),
                List.of("appName", "deviceType"), List.of("SUM(bytes)"), List.of("a > 1", "b < 2")));
        QueryHash reversed = generator.generateQueryHash(timeSeriesQuery(new TimeRange(0, 86_399),
                List.of("deviceType", "appName"), List.of("SUM(bytes)"), List.of("b < 2", "a > 1")));
        assertThat(forward).isEqualTo(reversed);
    }

    @Test
    void differentAggregatesProduceDifferentHashes() {
        QueryHash sum = generator.generateQueryHash(
                timeSeriesQuery(new TimeRange(0, 86_399), List.of("appName"), List.of("SUM(bytes)"), List.of()));
        QueryHash max = generator.generateQueryHash(
                timeSeriesQuery(new TimeRange(0, 86_399), List.of("appName"), List.of("MAX(bytes)"), List.of()));
        assertThat(sum).isNotEqualTo(max);
    }

    @Test
    void timeSeriesFoldsTheFixedStepWhileGlobalAggregateUsesStepZero() {
        HashComponents components = HashComponents.of(List.of("appName"), List.of("SUM(bytes)"), List.of());
        CanonicalQueryObject series = CanonicalQueryObject.of(components, new TimeRange(0, 86_399),
                PostProcessing.none(), QueryMetadata.timeSeries(600));
        CanonicalQueryObject global = CanonicalQueryObject.of(components, new TimeRange(0, 86_399),
                PostProcessing.none(), QueryMetadata.globalAggregate());

        assertThat(generator.buildCanonicalString(series, CacheConstants.DEFAULT_CACHE_STEP_SECONDS))
                .contains("\"step\":300");
        assertThat(generator.buildCanonicalString(global, 0)).contains("\"step\":0");
        assertThat(generator.generateQueryHash(series)).isNotEqualTo(generator.generateQueryHash(global));
    }

    @Test
    void compositeAliasesChangeTheHash() {
        HashComponents components = HashComponents.of(List.of("appName"), List.of("SUM(a)", "SUM(b)"), List.of());
        QueryMetadata withoutAlias = QueryMetadata.timeSeries(600);
        QueryMetadata withAlias = withoutAlias.withCompositeAliases(Map.of("ratio", "SUM(a)/SUM(b)"));

        QueryHash plain = generator.generateQueryHash(CanonicalQueryObject.of(components,
                new TimeRange(0, 86_399), PostProcessing.none(), withoutAlias));
        QueryHash composite = generator.generateQueryHash(CanonicalQueryObject.of(components,
                new TimeRange(0, 86_399), PostProcessing.none(), withAlias));
        assertThat(plain).isNotEqualTo(composite);
    }

    @Test
    void hashIsStableAcrossRepeatedInvocations() {
        CanonicalQueryObject query = timeSeriesQuery(new TimeRange(123, 456),
                List.of("appName"), List.of("SUM(bytes)"), List.of("region = 'eu'"));
        assertThat(generator.generateQueryHash(query)).isEqualTo(generator.generateQueryHash(query));
    }

    @Test
    void sourceSignatureContributesToTheHash() {
        HashComponents components = HashComponents.of(List.of("appName"), List.of("SUM(bytes)"), List.of());
        CanonicalQueryObject tableA = new CanonicalQueryObject(components, new TimeRange(0, 86_399),
                PostProcessing.none(), QueryMetadata.timeSeries(600), "", List.of("traffic_a"), List.of());
        CanonicalQueryObject tableB = new CanonicalQueryObject(components, new TimeRange(0, 86_399),
                PostProcessing.none(), QueryMetadata.timeSeries(600), "", List.of("traffic_b"), List.of());
        assertThat(generator.generateQueryHash(tableA)).isNotEqualTo(generator.generateQueryHash(tableB));
    }
}
