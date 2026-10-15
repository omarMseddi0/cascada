package com.cascada.sql.adapter.calcite;

import com.cascada.cache.domain.time.TimeRange;

import java.util.List;
import java.util.Optional;

record CanonicalTimeRangeExtraction(Optional<TimeRange> timeRange, List<String> filters) {
}
