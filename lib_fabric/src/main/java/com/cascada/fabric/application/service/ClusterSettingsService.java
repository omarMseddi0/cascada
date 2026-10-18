package com.cascada.fabric.application.service;

import com.cascada.sparkconfig.domain.SparkReference;
import com.cascada.fabric.application.port.in.PrepareClusterSettingsUseCase;
import com.cascada.fabric.application.port.out.ExecutorMemorySizingPort;
import com.cascada.fabric.domain.ClusterValues;
import com.cascada.sparkconfig.domain.model.SparkMemorySplit;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class ClusterSettingsService implements PrepareClusterSettingsUseCase {
    private final ExecutorMemorySizingPort memorySizing;

    public ClusterSettingsService(ExecutorMemorySizingPort memorySizing) {
        this.memorySizing = Objects.requireNonNull(memorySizing, "memorySizing");
    }

    private static final int MAX_EXECUTOR_CORE_COUNT = Integer.MAX_VALUE - 1;
    private static final int MAX_EXECUTOR_COUNT = Integer.MAX_VALUE;
    private static final int MAX_REPLICA_COUNT = 1;
    private static final int MAX_PORT_NUMBER = 65_535;

    @Override
    public ClusterValues prepareClusterSettings(Map<String, String> settings) {
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
        int initialExecutors = SparkReference.initialExecutors(minimumExecutors, maximumExecutors);
        int replicas = readBoundedIntegerSetting(
                settings, "CASCADA_REPLICAS", 1, 1, MAX_REPLICA_COUNT);
        int driverPort = readBoundedIntegerSetting(
                settings, "CASCADA_DRIVER_PORT", SparkReference.DRIVER_PORT, 1, MAX_PORT_NUMBER);
        int blockManagerPort = readBoundedIntegerSetting(
                settings, "CASCADA_BLOCKMGR_PORT", SparkReference.BLOCK_MANAGER_PORT, 1, MAX_PORT_NUMBER);
        int httpPort = readBoundedIntegerSetting(settings, "CASCADA_HTTP_PORT", 8000, 1, MAX_PORT_NUMBER);
        int grpcPort = readBoundedIntegerSetting(settings, "CASCADA_GRPC_PORT", 9090, 1, MAX_PORT_NUMBER);

        String serviceAccountName = release + "-spark-" + suffix;
        String driverWorkloadName = release + "-driver-" + suffix;
        String driverServiceName = release + "-spark-driver-" + suffix;

        boolean gluten = SparkReference.parseGlutenEnabled(settings.get("CASCADA_GLUTEN_ENABLED"), true);
        String nativeEnabled = Boolean.toString(gluten);
        int ram = readBoundedIntegerSetting(settings, "CASCADA_EXECUTOR_RAM_GB", 16,
                SparkReference.minimumExecutorRamGigabytes(gluten), Integer.MAX_VALUE);
        SparkMemorySplit memory = memorySizing.deriveMemorySplit(ram, gluten);
        Map<String, String> values = new LinkedHashMap<>();
        values.put("executorOverhead", memory.overheadGigabytes() + "g");
        values.put("executorOffHeap", memory.offHeapGigabytes() + "g");
        values.put("glutenEnabled", nativeEnabled);
        values.put("sparkPlugins", gluten ? SparkReference.GLUTEN_PLUGIN : "");
        values.put("shuffleManager", gluten ? SparkReference.COLUMNAR_SHUFFLE_MANAGER : SparkReference.SORT_SHUFFLE_MANAGER);
        // identity / images
        values.put("releaseName", release);
        values.put("copySuffix", suffix);
        values.put("namespace", namespace);
        values.put("containerImage", settingOrDefault(settings, "CASCADA_CONTAINER_IMAGE",
                SparkReference.CONTAINER_IMAGE));
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
        String executorHeap = settingOrDefault(settings, "CASCADA_EXECUTOR_MEMORY", memory.heapAsSparkMemoryString());
        if (!executorHeap.matches("[1-9][0-9]*g")) throw new IllegalArgumentException("CASCADA_EXECUTOR_MEMORY must use whole GiB");
        long heapGiB = Long.parseLong(executorHeap.substring(0, executorHeap.length() - 1));
        if (heapGiB + memory.offHeapGigabytes() + memory.overheadGigabytes() > ram) {
            throw new IllegalArgumentException("CASCADA_EXECUTOR_MEMORY + native memory + overhead exceeds CASCADA_EXECUTOR_RAM_GB");
        }
        values.put("executorMemory", executorHeap);
        values.put("executorCores", Integer.toString(executorCoreCount));
        values.put("shufflePartitions", Long.toString(Math.min(Integer.MAX_VALUE, 2L * executorCoreCount * maximumExecutors)));
        values.put("executorLimitCores", Integer.toString(SparkReference.executorLimitCores(executorCoreCount)));
        String driverMemory = settingOrDefault(settings, "CASCADA_DRIVER_MEMORY", SparkReference.DRIVER_MEMORY);
        if (!driverMemory.matches("[1-9][0-9]*g")) throw new IllegalArgumentException("CASCADA_DRIVER_MEMORY must use whole GiB, e.g. 2g");
        values.put("driverMemory", driverMemory);
        values.put("driverMaxResultSize", settingOrDefault(settings, "CASCADA_DRIVER_MAX_RESULT_SIZE", "6g"));
        values.put("driverPodMemory", (Long.parseLong(driverMemory.substring(0, driverMemory.length() - 1)) + 1) + "Gi");
        values.put("minExecutors", Integer.toString(minimumExecutors));
        values.put("maxExecutors", Integer.toString(maximumExecutors));
        values.put("initialExecutors", Integer.toString(initialExecutors));
        values.put("replicas", Integer.toString(replicas));
        // networking
        values.put("driverPort", Integer.toString(driverPort));
        values.put("blockManagerPort", Integer.toString(blockManagerPort));
        values.put("httpPort", Integer.toString(httpPort));
        values.put("grpcPort", Integer.toString(grpcPort));
        values.put("driverHost", driverServiceName + "." + namespace + ".svc.cluster.local");
        // placement (the third knob, expressed as a node-selector label)
        values.put("placementLabel", settingOrDefault(settings,
                "CASCADA_PLACEMENT_LABEL", "cascada.io/placement"));
        values.put("placementValue", settingOrDefault(settings, "CASCADA_PLACEMENT_VALUE", "spread"));
        values.put("timeColumn", settingOrDefault(settings, "CASCADA_TIME_COLUMN", "ts"));
        values.put("mainTableName", settingOrDefault(settings, "CASCADA_MAIN_TABLE_NAME", "main"));
        values.put("mainTablePath", settingOrDefault(settings, "CASCADA_MAIN_TABLE_PATH", "/DATA_ROOT/main"));
        values.put("redisUrl", settingOrDefault(settings, "REDIS_URL", "redis://valkey:6379"));
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
