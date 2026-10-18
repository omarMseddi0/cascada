package com.cascada.sql.adapter.calcite.model;

import java.util.Optional;

public record CanonicalTimeSeriesShape(boolean isTimeSeries, Optional<Integer> userStepSeconds,
                                boolean preserveRawTimeSeries) {
}
