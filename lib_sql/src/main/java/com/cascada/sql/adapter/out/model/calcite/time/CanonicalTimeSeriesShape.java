package com.cascada.sql.adapter.out.model.calcite.time;

import java.util.Optional;

public record CanonicalTimeSeriesShape(boolean isTimeSeries, Optional<Integer> userStepSeconds,
                                boolean preserveRawTimeSeries) {
}
