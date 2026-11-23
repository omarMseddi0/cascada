package com.cascada.cache.application.service.warming;

import com.cascada.cache.adapter.out.cache.ValkeyCacheBackendAdapter;
import com.cascada.cache.adapter.out.serialization.PortableFrameSerializer;
import com.cascada.cache.application.port.out.QueryExecutorPort;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.model.time.TimeRange;
import com.cascada.cache.domain.model.validation.ValidationBaseline;
import com.cascada.cache.fixture.ValkeyTestDatabase;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@EnabledIfEnvironmentVariable(named = ValkeyTestDatabase.ENVIRONMENT_VARIABLE, matches = ".+")
class DataChangeDetectorTest {

    @RegisterExtension
    final ValkeyTestDatabase valkey = new ValkeyTestDatabase();

    @BeforeEach
    void connectValkey() {
        baselineStore = valkey.cache(new PortableFrameSerializer());
    }

    private static final long DAY = 86_400L;
    private static final String TEMPLATE = "SELECT SUM(bytes) FROM traffic WHERE ts >= {start} AND ts <= {end}";
    private static final TimeRange MONDAY_TO_WEDNESDAY = new TimeRange(0, 3 * DAY - 1);
    private static final TimeRange TUESDAY_TO_THURSDAY = new TimeRange(DAY, 4 * DAY - 1);

    private ValkeyCacheBackendAdapter baselineStore;
    private final AtomicReference<Double> historicalTotal = new AtomicReference<>(100.0);
    private final List<String> executedSql = new ArrayList<>();

    private DataChangeDetector detector(QueryExecutorPort executor) {
        return new DataChangeDetector(baselineStore, logicalSql -> "PHYSICAL " + logicalSql, executor, TEMPLATE,
                Duration.ofDays(7));
    }

    private DataChangeDetector detector() {
        return detector(sql -> {
            executedSql.add(sql);
            return ResultFrame.builder().column("total", ColumnType.DOUBLE).row(historicalTotal.get()).build();
        });
    }

    @Test
    void theFirstRunStartsAnEpochAndForcesARewrite() {
        assertThat(detector().hasHistoricalDataChanged(MONDAY_TO_WEDNESDAY)).isTrue();
        assertThat(baselineStore.loadValidationBaseline()).map(ValidationBaseline::window).contains(MONDAY_TO_WEDNESDAY);
        assertThat(executedSql).containsExactly(
                "PHYSICAL SELECT SUM(bytes) FROM traffic WHERE ts >= 0 AND ts <= 259199");
    }

    @Test
    void unchangedHistoryKeepsTheCacheAndSlidesTheBaselineForward() {
        DataChangeDetector detector = detector();
        detector.hasHistoricalDataChanged(MONDAY_TO_WEDNESDAY);

        assertThat(detector.hasHistoricalDataChanged(TUESDAY_TO_THURSDAY)).isFalse();
        assertThat(baselineStore.loadValidationBaseline()).map(ValidationBaseline::window).contains(TUESDAY_TO_THURSDAY);
    }

    @Test
    void changedHistoryInTheStoredWindowForcesARewrite() {
        DataChangeDetector detector = detector();
        detector.hasHistoricalDataChanged(MONDAY_TO_WEDNESDAY);
        historicalTotal.set(125.0);

        assertThat(detector.hasHistoricalDataChanged(MONDAY_TO_WEDNESDAY)).isTrue();
        assertThat(detector.hasHistoricalDataChanged(MONDAY_TO_WEDNESDAY)).isFalse();
    }

    @Test
    void aFailingValidationQueryNeverForcesARewriteNorTouchesTheBaseline() {
        detector().hasHistoricalDataChanged(MONDAY_TO_WEDNESDAY);
        var stored = baselineStore.loadValidationBaseline();

        boolean changed = detector(sql -> { throw new IllegalStateException("spark down"); })
                .hasHistoricalDataChanged(TUESDAY_TO_THURSDAY);

        assertThat(changed).isFalse();
        assertThat(baselineStore.loadValidationBaseline()).isEqualTo(stored);
    }

    @Test
    void theSignatureIgnoresRowOrder() {
        List<ResultFrame> answers = new ArrayList<>(List.of(
                ResultFrame.builder().column("app", ColumnType.STRING).row("a").row("b").build(),
                ResultFrame.builder().column("app", ColumnType.STRING).row("b").row("a").build()));
        DataChangeDetector detector = detector(sql -> answers.remove(0));
        detector.hasHistoricalDataChanged(MONDAY_TO_WEDNESDAY);

        assertThat(detector.hasHistoricalDataChanged(MONDAY_TO_WEDNESDAY)).isFalse();
    }

    private DataChangeDetector groupedDetector(QueryExecutorPort executor) {
        return new DataChangeDetector(baselineStore, sql -> sql, executor,
                "SELECT {bucketSeconds} FROM traffic WHERE ts >= {start} AND ts <= {end}", Duration.ofDays(7));
    }

    private ResultFrame buckets(Map<Long, Long> values) {
        var frame = ResultFrame.builder().column("bucket_start", ColumnType.LONG).column("total", ColumnType.LONG);
        values.forEach(frame::row);
        return frame.build();
    }

    @Test
    void groupedValidationFindsChangesWithTheSameTotalAndHandlesDeletedAndNewRows() {
        var source = new AtomicReference<>(Map.of(0L, 10L, DAY, 20L, 2 * DAY, 30L));
        var detector = groupedDetector(sql -> {
            executedSql.add(sql);
            return buckets(source.get());
        });
        var first = detector.checkBuckets(MONDAY_TO_WEDNESDAY, DAY);
        assertThat(first.requiresFullRefresh()).isTrue();
        detector.acknowledge(first);
        source.set(Map.of(0L, 20L, DAY, 10L, 2 * DAY, 30L));

        var changed = detector.checkBuckets(MONDAY_TO_WEDNESDAY, DAY);
        assertThat(changed.requiresFullRefresh()).isFalse();
        assertThat(changed.changedBuckets()).containsExactlyInAnyOrder(0L, DAY);
        detector.acknowledge(changed);
        source.set(Map.of(DAY, 10L, 3 * DAY, 30L));

        var slid = detector.checkBuckets(TUESDAY_TO_THURSDAY, DAY);
        assertThat(slid.changedBuckets()).containsExactlyInAnyOrder(2 * DAY, 3 * DAY);
        assertThat(executedSql).containsExactly(
                "SELECT 86400 FROM traffic WHERE ts >= 0 AND ts <= 259199",
                "SELECT 86400 FROM traffic WHERE ts >= 0 AND ts <= 259199",
                "SELECT 86400 FROM traffic WHERE ts >= 86400 AND ts <= 345599");
    }

    @Test
    void groupedValidationRejectsDuplicatesAndMisalignedRowsWithoutReplacingTheBaseline() {
        var valid = groupedDetector(sql -> buckets(Map.of(0L, 1L)));
        valid.acknowledge(valid.checkBuckets(MONDAY_TO_WEDNESDAY, DAY));
        var baseline = baselineStore.loadValidationBaseline();
        var duplicate = groupedDetector(sql -> ResultFrame.builder().column("bucket_start", ColumnType.LONG)
                .row(0L).row(0L).build());
        var misaligned = groupedDetector(sql -> buckets(Map.of(1L, 1L)));

        assertThat(duplicate.checkBuckets(MONDAY_TO_WEDNESDAY, DAY).nextBaseline()).isEmpty();
        assertThat(misaligned.checkBuckets(MONDAY_TO_WEDNESDAY, DAY).nextBaseline()).isEmpty();
        assertThat(baselineStore.loadValidationBaseline()).isEqualTo(baseline);
    }

    @Test
    void aTemplateWithoutWindowPlaceholdersIsRejected() {
        assertThatThrownBy(() -> new DataChangeDetector(baselineStore, sql -> sql, sql -> null,
                "SELECT COUNT(*) FROM traffic", Duration.ofDays(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
