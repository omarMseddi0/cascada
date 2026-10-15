package com.cascada.fabric.application.service;

import com.cascada.fabric.application.port.out.EnvironmentPort;
import com.cascada.fabric.domain.ClusterValues;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Reads deployment settings through an outbound port and passes the values into the pure domain model. */
public final class ClusterSettingsReader {

    private static final List<String> SETTINGS = List.of(
            "CASCADA_RELEASE_NAME",
            "CASCADA_COPY_SUFFIX",
            "CASCADA_NAMESPACE",
            "CASCADA_CONTAINER_IMAGE",
            "CASCADA_IMAGE_PULL_SECRET",
            "CASCADA_EXECUTOR_MEMORY",
            "CASCADA_EXECUTOR_CORES",
            "CASCADA_DRIVER_MEMORY",
            "CASCADA_MIN_EXECUTORS",
            "CASCADA_MAX_EXECUTORS",
            "CASCADA_REPLICAS",
            "CASCADA_DRIVER_PORT",
            "CASCADA_BLOCKMGR_PORT",
            "CASCADA_PLACEMENT_LABEL",
            "CASCADA_PLACEMENT_VALUE",
            "CASCADA_HDFS_DEFAULT_FS",
            "CASCADA_DATA_HOST_PATH",
            "CASCADA_DATA_MOUNT_PATH");

    private final EnvironmentPort environment;

    public ClusterSettingsReader(EnvironmentPort environment) {
        this.environment = Objects.requireNonNull(environment, "environment");
    }

    public ClusterValues read() {
        Map<String, String> settings = new LinkedHashMap<>();
        for (String name : SETTINGS) {
            String value = environment.get(name);
            if (value != null) {
                settings.put(name, value);
            }
        }
        return ClusterValues.fromSettings(settings);
    }
}
