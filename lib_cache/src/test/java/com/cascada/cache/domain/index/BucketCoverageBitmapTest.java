package com.cascada.cache.domain.index;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class BucketCoverageBitmapTest {

    private static final long DAY = 86_400L;

    @Test
    void coverAndUncoverDriveTheCoverageAnswer() {
        BucketCoverageBitmap bitmap = new BucketCoverageBitmap(DAY);
        assertThat(bitmap.isCovered(0)).isFalse();

        bitmap.cover(0);
        bitmap.cover(2 * DAY);
        assertThat(bitmap.isCovered(0)).isTrue();
        assertThat(bitmap.isCovered(DAY)).isFalse();
        assertThat(bitmap.isCovered(2 * DAY)).isTrue();
        assertThat(bitmap.coveredCount()).isEqualTo(2);

        bitmap.uncover(0);
        assertThat(bitmap.isCovered(0)).isFalse();
        assertThat(bitmap.coveredCount()).isEqualTo(1);
    }

    @Test
    void presenceMaskMirrorsThePipelinedExistsAnswerShape() {
        BucketCoverageBitmap bitmap = new BucketCoverageBitmap(DAY);
        bitmap.cover(DAY);
        assertThat(bitmap.presenceMask(List.of(0L, DAY, 2 * DAY)))
                .containsExactly(false, true, false);
    }

    @Test
    void wireBytesRoundTripPreservesEveryBit() {
        BucketCoverageBitmap original = new BucketCoverageBitmap(DAY);
        original.cover(0);
        original.cover(7 * DAY);
        original.cover(19_000 * DAY); // a 2022-era day ordinal — realistic epoch range

        BucketCoverageBitmap reloaded = BucketCoverageBitmap.fromBytes(DAY, original.toBytes());
        assertThat(reloaded.isCovered(0)).isTrue();
        assertThat(reloaded.isCovered(7 * DAY)).isTrue();
        assertThat(reloaded.isCovered(19_000 * DAY)).isTrue();
        assertThat(reloaded.coveredCount()).isEqualTo(3);
    }

    @Test
    void preEpochBucketsAreNeverClaimedCovered() {
        BucketCoverageBitmap bitmap = new BucketCoverageBitmap(DAY);
        bitmap.cover(-DAY); // ignored: not indexable, falls back to the authoritative path
        assertThat(bitmap.isCovered(-DAY)).isFalse();
        assertThat(bitmap.coveredCount()).isZero();
    }
}
