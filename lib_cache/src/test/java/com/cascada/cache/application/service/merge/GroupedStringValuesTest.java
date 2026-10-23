package com.cascada.cache.application.service.merge;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class GroupedStringValuesTest {
    @Test
    void uniformValuesRemainSharedAcrossManyGroupsAndEqualStringObjects() {
        GroupedStringValues values = new GroupedStringValues(AggregateOperation.MINIMUM, 10);
        for (int group = 0; group < 100_000; group++) {
            values.accumulate(group, "double", false);
            values.accumulate(group, new String("double"), true);
        }
        assertThat(values.value(0)).isSameAs(values.value(99_999));
        assertThat(values.value(99_999)).isEqualTo("double");
    }

    @Test
    void independentMinimumWinnersCanDifferFromAndReturnToTheSharedValue() {
        GroupedStringValues values = new GroupedStringValues(AggregateOperation.MINIMUM, 10);
        values.accumulate(0, "middle", false);
        values.accumulate(1, "zulu", false);
        values.accumulate(900, "middle", false);
        values.accumulate(100_001, "zulu", false);
        values.accumulate(100_002, "alpha", false);
        values.accumulate(100_001, "middle", true);
        values.accumulate(100_002, "zulu", true);
        assertThat(values.value(0)).isEqualTo("middle");
        assertThat(values.value(1)).isEqualTo("zulu");
        assertThat(values.value(900)).isEqualTo("middle");
        assertThat(values.value(100_001)).isEqualTo("middle");
        assertThat(values.value(100_002)).isEqualTo("alpha");
    }

    @Test
    void maximumUsesSparkCodePointOrderAndHandlesPageBoundaries() {
        GroupedStringValues values = new GroupedStringValues(AggregateOperation.MAXIMUM, 10);
        values.accumulate(0, "\uE000", false);
        values.accumulate(1_023, "a", false);
        values.accumulate(1_024, "b", false);
        values.accumulate(1_023, "\uD800\uDC00", true);
        values.accumulate(1_024, "\uE000", true);
        assertThat(values.value(0)).isEqualTo("\uE000");
        assertThat(values.value(1_023)).isEqualTo("\uD800\uDC00");
        assertThat(values.value(1_024)).isEqualTo("\uE000");
    }
}
