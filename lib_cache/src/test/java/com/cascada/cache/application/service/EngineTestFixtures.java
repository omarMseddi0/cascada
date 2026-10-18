package com.cascada.cache.application.service;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.model.hashing.HashComponents;
import com.cascada.cache.domain.model.query.CanonicalQueryObject;
import com.cascada.cache.domain.model.query.PostProcessing;
import com.cascada.cache.domain.model.query.QueryMetadata;
import com.cascada.cache.domain.model.time.TimeRange;
import java.util.List;
import java.util.Map;

/** Query and frame shapes shared by the execution-engine tests. */
final class EngineTestFixtures {

    static final long DAY = 86_400L;

    private EngineTestFixtures() {
    }

    /** SUM(bytes) per appName over three complete days. */
    static CanonicalQueryObject globalAggregateOverThreeDays() {
        HashComponents components = HashComponents.of(List.of("appName"), List.of("SUM(bytes)"), List.of());
        return new CanonicalQueryObject(components, new TimeRange(0, 3 * DAY - 1),
                PostProcessing.none(), QueryMetadata.globalAggregate(), "FULL_SQL", List.of("traffic"), List.of());
    }

    static ResultFrame appFrame(String app, double bytes) {
        return ResultFrame.builder()
                .column("appName", ColumnType.STRING)
                .column("bytes", ColumnType.DOUBLE)
                .row(Map.of("appName", app, "bytes", bytes))
                .build();
    }
}
