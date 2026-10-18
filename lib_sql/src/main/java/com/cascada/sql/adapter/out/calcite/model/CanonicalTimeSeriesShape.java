package com.cascada.sql.adapter.out.calcite.model;

import java.util.Optional;

public record CanonicalTimeSeriesShape(boolean isTimeSeries, Optional<Integer> userStepSeconds,
                                boolean preserveRawTimeSeries) {
}
