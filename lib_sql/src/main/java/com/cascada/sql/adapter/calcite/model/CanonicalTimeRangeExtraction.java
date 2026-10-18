package com.cascada.sql.adapter.calcite.model;

import com.cascada.cache.domain.model.time.TimeRange;
import java.util.List;
import java.util.Optional;

public record CanonicalTimeRangeExtraction(Optional<TimeRange> timeRange, List<String> filters) {
}
