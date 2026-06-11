package com.cascada.fabric;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * Every value the Fabric YAML templates need, read from environment variables with sensible defaults
 * (plan §6.7). This is the whole "configuration" story: set a few {@code CASCADA_*} env vars, the rest
 * defaults to the reference deployment. Resource names ({@code <release>-spark-<suffix>}, …) and the few
 * derived numbers (executor limit-cores = cores + 1, the in-cluster driver host) are computed here so
 * the templates only ever see ready-to-substitute strings.
 *
 * <p>{@link #fromEnvironment(UnaryOperator)} takes an env lookup so it is trivially testable;
 * {@link #fromSystemEnvironment()} wires it to {@link System#getenv(String)} in production.
 */
public final class ClusterValues {

    private final Map<String, String> placeholders;

    private ClusterValues(Map<String, String> placeholders) {
        this.placeholders = placeholders;
    }

    public static ClusterValues fromSystemEnvironment() {
        return fromEnvironment(System::getenv);
    }

    public static ClusterValues fromEnvironment(UnaryOperator<String> env) {
        String release = get(env, "CASCADA_RELEASE_NAME", "cascada");
        String suffix = get(env, "CASCADA_COPY_SUFFIX", "copy1");
        String namespace = get(env, "CASCADA_NAMESPACE", "default");
        String executorCores = get(env, "CASCADA_EXECUTOR_CORES", "3");

        String serviceAccountName = release + "-spark-" + suffix;
        String driverWorkloadName = release + "-driver-" + suffix;
        String driverServiceName = release + "-spark-driver-" + suffix;

        Map<String, String> v = new LinkedHashMap<>();
        // identity / images
        v.put("releaseName", release);
        v.put("copySuffix", suffix);
        v.put("namespace", namespace);
        v.put("containerImage", get(env, "CASCADA_CONTAINER_IMAGE",
                "docker.registry.local:5000/spark:python3-java17-hadoop"));
        v.put("imagePullSecret", get(env, "CASCADA_IMAGE_PULL_SECRET", "regsec"));
        // derived resource names
        v.put("serviceAccountName", serviceAccountName);
        v.put("driverWorkloadName", driverWorkloadName);
        v.put("driverServiceName", driverServiceName);
        v.put("clusterRoleBindingName", release + "-cluster-edit-binding-" + suffix);
        v.put("roleName", release + "-namespace-role-" + suffix);
        v.put("roleBindingName", release + "-namespace-role-binding-" + suffix);
        v.put("coreSiteConfigMapName", release + "-hadoop-core-site-config-" + suffix);
        v.put("hdfsSiteConfigMapName", release + "-hadoop-hdfs-site-config-" + suffix);
        v.put("podTemplateConfigMapName", release + "-spark-executor-pod-template-config-" + suffix);
        v.put("sparkConfigMapName", release + "-spark-config-" + suffix);
        v.put("log4jConfigMapName", release + "-spark-log4j-config-" + suffix);
        v.put("podNamePrefix", release + "-exec-" + suffix);
        // sizing knobs
        v.put("executorMemory", get(env, "CASCADA_EXECUTOR_MEMORY", "8g"));
        v.put("executorCores", executorCores);
        v.put("executorLimitCores", Integer.toString(parseInt(executorCores, 3) + 1));
        v.put("driverMemory", get(env, "CASCADA_DRIVER_MEMORY", "2g"));
        v.put("minExecutors", get(env, "CASCADA_MIN_EXECUTORS", "2"));
        v.put("maxExecutors", get(env, "CASCADA_MAX_EXECUTORS", "3"));
        v.put("replicas", get(env, "CASCADA_REPLICAS", "1"));
        // networking
        v.put("driverPort", get(env, "CASCADA_DRIVER_PORT", "8002"));
        v.put("blockManagerPort", get(env, "CASCADA_BLOCKMGR_PORT", "8001"));
        v.put("driverHost", driverServiceName + "." + namespace + ".svc.cluster.local");
        // placement (the third knob, expressed as a node-selector label)
        v.put("placementLabel", get(env, "CASCADA_PLACEMENT_LABEL", "cascada.io/placement"));
        v.put("placementValue", get(env, "CASCADA_PLACEMENT_VALUE", "spread"));
        // storage
        v.put("hdfsDefaultFs", get(env, "CASCADA_HDFS_DEFAULT_FS", "hdfs://namenode:9000"));
        v.put("dataHostPath", get(env, "CASCADA_DATA_HOST_PATH", "/mnt/data-prod"));
        v.put("dataMountPath", get(env, "CASCADA_DATA_MOUNT_PATH", "/DATA_ROOT"));
        // fixed mount paths (same on driver and executors so table paths resolve identically)
        v.put("coreSiteMount", "/opt/spark/work-dir/core-site.xml");
        v.put("hdfsSiteMount", "/opt/spark/work-dir/hdfs-site.xml");
        v.put("dnSocketPath", "/var/lib/hadoop-hdfs/dn_socket");
        v.put("podTemplateMount", "/opt/spark/templates/executor-pod-template.yaml");
        v.put("sparkJsonMount", "/app/spark.json");
        v.put("log4jMount", "/opt/spark/conf/log4j2.properties");

        return new ClusterValues(v);
    }

    public Map<String, String> placeholders() {
        return Map.copyOf(placeholders);
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

    private static String get(UnaryOperator<String> env, String key, String fallback) {
        String value = env.apply(key);
        return (value == null || value.isBlank()) ? fallback : value;
    }

    private static int parseInt(String value, int fallback) {
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
