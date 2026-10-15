package com.cascada.fabric.domain;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Every value the Fabric templates need, derived from supplied settings with sensible defaults
 * (plan §6.7). Unset or blank {@code CASCADA_*} settings use the reference deployment defaults; malformed
 * numeric settings are rejected. Resource names ({@code <release>-spark-<suffix>}, …) and the few
 * derived numbers (executor limit-cores = cores + 1, the in-cluster driver host) are computed here so
 * the templates only ever see ready-to-substitute strings.
 *
 * <p>This is a pure transformation of supplied settings: environment access belongs to the application
 * layer and reaches this class only as data. Numeric settings are parsed and range-checked before they
 * can reach a manifest.
 */
public final class ClusterValues {

    private static final int MAX_EXECUTOR_CORE_COUNT = Integer.MAX_VALUE - 1;
    private static final int MAX_EXECUTOR_COUNT = Integer.MAX_VALUE;
    private static final int MAX_REPLICA_COUNT = Integer.MAX_VALUE;
    private static final int MAX_PORT_NUMBER = 65_535;
    private static final int REFERENCE_INITIAL_EXECUTOR_COUNT = 3;

    private final Map<String, String> placeholders;

    private ClusterValues(Map<String, String> placeholders) {
        this.placeholders = Collections.unmodifiableMap(new LinkedHashMap<>(placeholders));
    }

    /** Defaults only — every {@code CASCADA_*} setting falls back. Useful for tests and documentation. */
    public static ClusterValues defaults() {
        return fromSettings(Map.of());
    }

    /** Builds template values from already-read settings without performing environment or filesystem I/O. */
    public static ClusterValues fromSettings(Map<String, String> settings) {
        Objects.requireNonNull(settings, "settings");
        String release = settingOrDefault(settings, "CASCADA_RELEASE_NAME", "cascada");
        String suffix = settingOrDefault(settings, "CASCADA_COPY_SUFFIX", "copy1");
        String namespace = settingOrDefault(settings, "CASCADA_NAMESPACE", "default");
        int executorCoreCount = readBoundedIntegerSetting(
                settings, "CASCADA_EXECUTOR_CORES", 3, 1, MAX_EXECUTOR_CORE_COUNT);
        int minimumExecutors = readBoundedIntegerSetting(
                settings, "CASCADA_MIN_EXECUTORS", 2, 0, MAX_EXECUTOR_COUNT);
        int maximumExecutors = readBoundedIntegerSetting(
                settings, "CASCADA_MAX_EXECUTORS", 3, 1, MAX_EXECUTOR_COUNT);
        if (minimumExecutors > maximumExecutors) {
            throw new IllegalArgumentException("CASCADA_MIN_EXECUTORS must be <= CASCADA_MAX_EXECUTORS");
        }
        int initialExecutors = Math.max(minimumExecutors,
                Math.min(REFERENCE_INITIAL_EXECUTOR_COUNT, maximumExecutors));
        int replicas = readBoundedIntegerSetting(
                settings, "CASCADA_REPLICAS", 1, 1, MAX_REPLICA_COUNT);
        int driverPort = readBoundedIntegerSetting(
                settings, "CASCADA_DRIVER_PORT", 8002, 1, MAX_PORT_NUMBER);
        int blockManagerPort = readBoundedIntegerSetting(
                settings, "CASCADA_BLOCKMGR_PORT", 8001, 1, MAX_PORT_NUMBER);

        String serviceAccountName = release + "-spark-" + suffix;
        String driverWorkloadName = release + "-driver-" + suffix;
        String driverServiceName = release + "-spark-driver-" + suffix;

        Map<String, String> values = new LinkedHashMap<>();
        // identity / images
        values.put("releaseName", release);
        values.put("copySuffix", suffix);
        values.put("namespace", namespace);
        values.put("containerImage", settingOrDefault(settings, "CASCADA_CONTAINER_IMAGE",
                "docker.registry.local:5000/spark:python3-java17-hadoop"));
        values.put("imagePullSecret", settingOrDefault(settings, "CASCADA_IMAGE_PULL_SECRET", "regsec"));
        // derived resource names
        values.put("serviceAccountName", serviceAccountName);
        values.put("driverWorkloadName", driverWorkloadName);
        values.put("driverServiceName", driverServiceName);
        values.put("clusterRoleBindingName", release + "-cluster-edit-binding-" + suffix);
        values.put("roleName", release + "-namespace-role-" + suffix);
        values.put("roleBindingName", release + "-namespace-role-binding-" + suffix);
        values.put("coreSiteConfigMapName", release + "-hadoop-core-site-config-" + suffix);
        values.put("hdfsSiteConfigMapName", release + "-hadoop-hdfs-site-config-" + suffix);
        values.put("podTemplateConfigMapName", release + "-spark-executor-pod-template-config-" + suffix);
        values.put("sparkConfigMapName", release + "-spark-config-" + suffix);
        values.put("log4jConfigMapName", release + "-spark-log4j-config-" + suffix);
        values.put("podNamePrefix", release + "-exec-" + suffix);
        // sizing knobs
        values.put("executorMemory", settingOrDefault(settings, "CASCADA_EXECUTOR_MEMORY", "8g"));
        values.put("executorCores", Integer.toString(executorCoreCount));
        values.put("executorLimitCores", Integer.toString(executorCoreCount + 1));
        values.put("driverMemory", settingOrDefault(settings, "CASCADA_DRIVER_MEMORY", "2g"));
        values.put("minExecutors", Integer.toString(minimumExecutors));
        values.put("maxExecutors", Integer.toString(maximumExecutors));
        values.put("initialExecutors", Integer.toString(initialExecutors));
        values.put("replicas", Integer.toString(replicas));
        // networking
        values.put("driverPort", Integer.toString(driverPort));
        values.put("blockManagerPort", Integer.toString(blockManagerPort));
        values.put("driverHost", driverServiceName + "." + namespace + ".svc.cluster.local");
        // placement (the third knob, expressed as a node-selector label)
        values.put("placementLabel", settingOrDefault(settings,
                "CASCADA_PLACEMENT_LABEL", "cascada.io/placement"));
        values.put("placementValue", settingOrDefault(settings, "CASCADA_PLACEMENT_VALUE", "spread"));
        // storage
        values.put("hdfsDefaultFs", settingOrDefault(settings, "CASCADA_HDFS_DEFAULT_FS", "hdfs://namenode:9000"));
        values.put("dataHostPath", settingOrDefault(settings, "CASCADA_DATA_HOST_PATH", "/mnt/data-prod"));
        values.put("dataMountPath", settingOrDefault(settings, "CASCADA_DATA_MOUNT_PATH", "/DATA_ROOT"));
        // fixed mount paths (same on driver and executors so table paths resolve identically)
        values.put("coreSiteMount", "/opt/spark/work-dir/core-site.xml");
        values.put("hdfsSiteMount", "/opt/spark/work-dir/hdfs-site.xml");
        values.put("hadoopConfDir", "/opt/spark/work-dir");
        values.put("dnSocketPath", "/var/lib/hadoop-hdfs/dn_socket");
        values.put("podTemplateMount", "/opt/spark/templates/executor-pod-template.yaml");
        values.put("sparkJsonMount", "/app/spark.json");
        values.put("log4jMount", "/opt/spark/conf/log4j2.properties");

        return new ClusterValues(values);
    }

    public Map<String, String> placeholders() {
        return placeholders;
    }

    public String namespace() {
        return placeholders.get("namespace");
    }

    public String driverWorkloadName() {
        return placeholders.get("driverWorkloadName");
    }

    public String get(String key) {
        return placeholders.get(key);
    }

    private static String settingOrDefault(Map<String, String> settings, String key, String fallback) {
        String value = settings.get(key);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static int readBoundedIntegerSetting(Map<String, String> settings, String key, int fallback,
                                                 int minimumAllowed, int maximumAllowed) {
        String value = settingOrDefault(settings, key, Integer.toString(fallback));
        try {
            int parsed = Integer.parseInt(value.trim());
            if (parsed < minimumAllowed || parsed > maximumAllowed) {
                throw new IllegalArgumentException(key + " must be between " + minimumAllowed + " and "
                        + maximumAllowed
                        + ", but was: '" + value + "'");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be an integer, but was: '" + value + "'", e);
        }
    }
}
