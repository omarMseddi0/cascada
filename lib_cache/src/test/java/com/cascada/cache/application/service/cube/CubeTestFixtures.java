package com.cascada.cache.application.service.cube;

import com.cascada.cache.domain.model.cube.CachedShapeEntry;
import com.cascada.cache.domain.model.cube.QueryShape;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** The finest cached grain shared by the cube planner and verifier tests. */
final class CubeTestFixtures {

    private CubeTestFixtures() {
    }

    /** Finest cached grain (appName, deviceType, region) with SUM+COUNT so AVG can be reconstructed. */
    static ResultFrame finestFrame() {
        return ResultFrame.builder()
                .column("appName", ColumnType.STRING)
                .column("deviceType", ColumnType.STRING)
                .column("region", ColumnType.STRING)
                .column("SUM(latency)", ColumnType.DOUBLE)
                .column("COUNT(latency)", ColumnType.DOUBLE)
                .row(row("netflix", "mobile", "eu", 100.0, 4.0))
                .row(row("netflix", "tablet", "eu", 50.0, 1.0))
                .row(row("netflix", "mobile", "us", 30.0, 2.0))
                .row(row("youtube", "mobile", "eu", 20.0, 2.0))
                .build();
    }

    static Map<String, Object> row(String app, String device, String region, double sum, double count) {
        Map<String, Object> values = new HashMap<>();
        values.put("appName", app);
        values.put("deviceType", device);
        values.put("region", region);
        values.put("SUM(latency)", sum);
        values.put("COUNT(latency)", count);
        return values;
    }

    static CachedShapeEntry finest() {
        return new CachedShapeEntry(
                new QueryShape(Set.of("appName", "deviceType", "region"), Set.of(),
                        Set.of("SUM(latency)", "COUNT(latency)")),
                finestFrame());
    }
}
