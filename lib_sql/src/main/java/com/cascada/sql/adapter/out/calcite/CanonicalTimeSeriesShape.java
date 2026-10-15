package com.cascada.sql.adapter.out.calcite;

import java.util.Optional;

record CanonicalTimeSeriesShape(boolean isTimeSeries, Optional<Integer> userStepSeconds,
                                boolean preserveRawTimeSeries) {
}
