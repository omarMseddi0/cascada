package com.cascada.cache.application.service.warming;

import com.cascada.cache.adapter.out.cache.InMemoryBlobCacheBackendAdapter;
import com.cascada.cache.adapter.out.serialization.PortableFrameSerializer;
import com.cascada.cache.application.port.out.QueryExecutorPort;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.model.time.TimeRange;
import com.cascada.cache.domain.model.validation.ValidationBaseline;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DataChangeDetectorTest {

    private static final long DAY = 86_400L;
    private static final String TEMPLATE = "SELECT SUM(bytes) FROM traffic WHERE ts >= {start} AND ts <= {end}";
    private static final TimeRange MONDAY_TO_WEDNESDAY = new TimeRange(0, 3 * DAY - 1);
    private static final TimeRange TUESDAY_TO_THURSDAY = new TimeRange(DAY, 4 * DAY - 1);

    private final InMemoryBlobCacheBackendAdapter baselineStore =
            new InMemoryBlobCacheBackendAdapter(new PortableFrameSerializer());
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

    @Test
    void aTemplateWithoutWindowPlaceholdersIsRejected() {
        assertThatThrownBy(() -> new DataChangeDetector(baselineStore, sql -> sql, sql -> null,
                "SELECT COUNT(*) FROM traffic", Duration.ofDays(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
