package com.cascada.fabric.application.service;

import com.cascada.sparkconfig.domain.SparkReference;
import com.cascada.fabric.application.port.in.PrepareClusterSettingsUseCase;
import com.cascada.fabric.application.port.out.ExecutorMemorySizingPort;
import com.cascada.fabric.domain.model.ClusterValues;
import com.cascada.sparkconfig.domain.model.SparkMemorySplit;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class ClusterSettingsService implements PrepareClusterSettingsUseCase {
    private static final int MAX_EXECUTOR_CORE_COUNT = Integer.MAX_VALUE - 1;
    private static final int MAX_EXECUTOR_COUNT = Integer.MAX_VALUE;
    private static final int MAX_REPLICA_COUNT = 1;
    private static final int MAX_PORT_NUMBER = 65_535;

    private final ExecutorMemorySizingPort memorySizing;

    public ClusterSettingsService(ExecutorMemorySizingPort memorySizing) {
        this.memorySizing = Objects.requireNonNull(memorySizing, "memorySizing");
    }

    @Override
    public ClusterValues prepareClusterSettings(Map<String, String> settings) {
        Objects.requireNonNull(settings, "settings");
        String releaseName = settingOrDefault(settings, "CASCADA_RELEASE_NAME", "cascada");
        String copySuffix = settingOrDefault(settings, "CASCADA_COPY_SUFFIX", "copy1");
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

        String serviceAccountName = releaseName + "-spark-" + copySuffix;
        String driverWorkloadName = releaseName + "-driver-" + copySuffix;
        String driverServiceName = releaseName + "-spark-driver-" + copySuffix;

        boolean gluten = SparkReference.parseGlutenEnabled(settings.get("CASCADA_GLUTEN_ENABLED"), true);
        int executorRamGigabytes = readBoundedIntegerSetting(settings, "CASCADA_EXECUTOR_RAM_GB", 16,
                SparkReference.minimumExecutorRamGigabytes(gluten), Integer.MAX_VALUE);
        SparkMemorySplit memory = memorySizing.deriveMemorySplit(executorRamGigabytes, gluten);
        Map<String, String> values = new LinkedHashMap<>();
        values.put("executorOverhead", memory.overheadGigabytes() + "g");
        values.put("executorOffHeap", memory.offHeapGigabytes() + "g");
        values.put("glutenEnabled", Boolean.toString(gluten));
        values.put("sparkPlugins", gluten ? SparkReference.GLUTEN_PLUGIN : "");
        values.put("shuffleManager", gluten ? SparkReference.COLUMNAR_SHUFFLE_MANAGER : SparkReference.SORT_SHUFFLE_MANAGER);
        // identity / images
        values.put("releaseName", releaseName);
        values.put("copySuffix", copySuffix);
        values.put("namespace", namespace);
        values.put("containerImage", settingOrDefault(settings, "CASCADA_CONTAINER_IMAGE",
                SparkReference.CONTAINER_IMAGE));
        values.put("imagePullSecret", settingOrDefault(settings, "CASCADA_IMAGE_PULL_SECRET", "regsec"));
        // derived resource names
        values.put("serviceAccountName", serviceAccountName);
        values.put("driverWorkloadName", driverWorkloadName);
        values.put("driverServiceName", driverServiceName);
        values.put("clusterRoleBindingName", releaseName + "-cluster-edit-binding-" + copySuffix);
        values.put("roleName", releaseName + "-namespace-role-" + copySuffix);
        values.put("roleBindingName", releaseName + "-namespace-role-binding-" + copySuffix);
        values.put("coreSiteConfigMapName", releaseName + "-hadoop-core-site-config-" + copySuffix);
        values.put("hdfsSiteConfigMapName", releaseName + "-hadoop-hdfs-site-config-" + copySuffix);
        values.put("podTemplateConfigMapName", releaseName + "-spark-executor-pod-template-config-" + copySuffix);
        values.put("sparkConfigMapName", releaseName + "-spark-config-" + copySuffix);
        values.put("log4jConfigMapName", releaseName + "-spark-log4j-config-" + copySuffix);
        values.put("podNamePrefix", releaseName + "-exec-" + copySuffix);
        // sizing knobs
        values.put("executorMemory", readExecutorHeapSetting(settings, memory, executorRamGigabytes));
        values.put("executorCores", Integer.toString(executorCoreCount));
        values.put("shufflePartitions", Long.toString(Math.min(Integer.MAX_VALUE, 2L * executorCoreCount * maximumExecutors)));
        values.put("executorLimitCores", Integer.toString(SparkReference.executorLimitCores(executorCoreCount)));
        String driverMemory = settingOrDefault(settings, "CASCADA_DRIVER_MEMORY", SparkReference.DRIVER_MEMORY);
        if (!driverMemory.matches("[1-9][0-9]*g")) {
            throw new IllegalArgumentException("CASCADA_DRIVER_MEMORY must use whole GiB, e.g. 2g");
        }
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
        addPlacementAndStorageValues(settings, values);
        return new ClusterValues(values);
    }

    private static String readExecutorHeapSetting(Map<String, String> settings, SparkMemorySplit memory,
                                                   int executorRamGigabytes) {
        String executorHeap = settingOrDefault(settings, "CASCADA_EXECUTOR_MEMORY", memory.heapAsSparkMemoryString());
        if (!executorHeap.matches("[1-9][0-9]*g")) {
            throw new IllegalArgumentException("CASCADA_EXECUTOR_MEMORY must use whole GiB");
        }
        long heapGigabytes = Long.parseLong(executorHeap.substring(0, executorHeap.length() - 1));
        if (heapGigabytes + memory.offHeapGigabytes() + memory.overheadGigabytes() > executorRamGigabytes) {
            throw new IllegalArgumentException("CASCADA_EXECUTOR_MEMORY + native memory + overhead exceeds CASCADA_EXECUTOR_RAM_GB");
        }
        return executorHeap;
    }

    private static void addPlacementAndStorageValues(Map<String, String> settings, Map<String, String> values) {
        values.put("placementLabel", settingOrDefault(settings,
                "CASCADA_PLACEMENT_LABEL", "cascada.io/placement"));
        values.put("placementValue", settingOrDefault(settings, "CASCADA_PLACEMENT_VALUE", "spread"));
        values.put("timeColumn", settingOrDefault(settings, "CASCADA_TIME_COLUMN", "ts"));
        values.put("mainTableName", settingOrDefault(settings, "CASCADA_MAIN_TABLE_NAME", "main"));
        values.put("mainTablePath", settingOrDefault(settings, "CASCADA_MAIN_TABLE_PATH", "/DATA_ROOT/main"));
        values.put("redisUrl", settingOrDefault(settings, "REDIS_URL", "redis://valkey:6379"));
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
