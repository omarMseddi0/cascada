package com.cascada.sql.adapter.calcite;

import java.util.Optional;

record CanonicalTimeSeriesShape(boolean isTimeSeries, Optional<Integer> userStepSeconds,
                                boolean preserveRawTimeSeries) {
}
