package com.cascada.app.adapter.in.api;

import com.cascada.app.exception.InvalidRequestException;
import com.cascada.cache.application.port.in.FlushCacheUseCase;
import com.cascada.cache.application.port.in.WarmCacheUseCase;
import com.cascada.cache.application.result.WarmingReport;
import com.cascada.cache.domain.admin.CacheScope;
import com.cascada.fabric.application.port.in.ManageClusterLifecycleUseCase;
import com.cascada.fabric.application.port.in.PrepareClusterSettingsUseCase;
import com.cascada.fabric.domain.ClusterValues;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Validation and dispatch for cache administration and cluster lifecycle, shared by the HTTP and gRPC
 * transports so both accept the same requests, reject them with the same messages and answer with the
 * same statuses.
 */
final class AdministrationRequests {

    private AdministrationRequests() {
    }

    /** Flushes everything, or only the bucket keys under {@code prefix} when one is given. */
    static long flush(FlushCacheUseCase flush, Optional<String> prefix) {
        if (prefix.isEmpty()) {
            return flush.flushEverything();
        }
        String keyPrefix = prefix.get();
        if (keyPrefix.isBlank()) throw new InvalidRequestException("prefix must not be blank");
        try {
            CacheScope.forKeyPrefix(keyPrefix);
        } catch (IllegalArgumentException invalid) {
            throw new InvalidRequestException("prefix must select Cascada bucket keys (QC:V4:)");
        }
        return flush.flushKeyPrefix(keyPrefix);
    }

    static WarmingReport warm(WarmCacheUseCase warm, long start, long end, boolean forceOverwrite) {
        if (start == 0 && end == 0) throw new InvalidRequestException("start and end are required");
        if (start > end) throw new InvalidRequestException("start must be <= end");
        return warm.warmCycle(start, end, forceOverwrite);
    }

    static ClusterChangeResult changeCluster(ManageClusterLifecycleUseCase clusters, PrepareClusterSettingsUseCase preparation,
                                       ClusterChange operation, Map<String, String> settings) {
        ClusterValues values;
        try {
            values = preparation.prepareClusterSettings(settings);
        } catch (IllegalArgumentException invalid) {
            throw new InvalidRequestException("Invalid cluster settings");
        }
        return switch (operation) {
            case DEPLOY -> new ClusterChangeResult("deployed", clusters.deploy(values));
            case STOP -> { clusters.stop(values); yield new ClusterChangeResult("stopped", List.of()); }
            case START -> { clusters.start(values); yield new ClusterChangeResult("started", List.of()); }
            case RESTART -> { clusters.restart(values); yield new ClusterChangeResult("restarted", List.of()); }
            case DELETE -> { clusters.delete(values); yield new ClusterChangeResult("deleted", List.of()); }
        };
    }
}
