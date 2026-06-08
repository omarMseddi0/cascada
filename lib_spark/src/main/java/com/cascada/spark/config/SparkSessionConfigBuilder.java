package com.cascada.spark.config;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Builds a {@link SparkSessionConfig} the same way {@code spark_manager.py} /
 * {@code spark_configs.py} do: start from the Delta-enabling defaults, layer in any grouped
 * {@code spark.json} the operator supplies, then apply environment-variable overrides (highest
 * priority). The result is the flat {@code spark.*} map handed to {@code SparkSession.builder}.
 *
 * <p>The builder is pure and Spark-free, so the entire configuration contract is unit-testable without
 * a Spark runtime. Two facts are guaranteed and tested:
 * <ul>
 *   <li><b>Delta is always on</b> — {@code spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension}
 *       and {@code spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog} are
 *       present unless explicitly overridden, so a {@code SELECT ... FROM delta.`/path`} always works;</li>
 *   <li><b>local vs cluster is just the master</b> — {@link #forLocal} and {@link #forKubernetes} differ
 *       only in the master string (and local drops the k8s-only keys); the executor code is identical.</li>
 * </ul>
 */
public final class SparkSessionConfigBuilder {

    public static final String DELTA_EXTENSIONS = "io.delta.sql.DeltaSparkSessionExtension";
    public static final String DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog";

    private static final String DEFAULT_KUBERNETES_MASTER =
            "k8s://https://kubernetes.default.svc.cluster.local:443";
    private static final String DEFAULT_LOCAL_MASTER = "local[*]";

    private String appName = "CascadaDeltaQueryExecutor";
    private String master = DEFAULT_KUBERNETES_MASTER;
    private final Map<String, String> properties = new LinkedHashMap<>();
    private Function<String, String> environment = System::getenv;

    public SparkSessionConfigBuilder() {
        // Delta-enabling defaults — the minimum that makes delta.`/path` queryable.
        properties.put("spark.sql.extensions", DELTA_EXTENSIONS);
        properties.put("spark.sql.catalog.spark_catalog", DELTA_CATALOG);
        // Sensible cross-environment defaults (overridable by spark.json / env).
        properties.put("spark.sql.adaptive.enabled", "true");
        properties.put("spark.sql.adaptive.coalescePartitions.enabled", "true");
    }

    /** A local in-process session (single node / dev / tests): {@code local[*]} master. */
    public static SparkSessionConfigBuilder forLocal() {
        return new SparkSessionConfigBuilder().master(DEFAULT_LOCAL_MASTER);
    }

    /** A Kubernetes session (the production data plane): {@code k8s://...} master. */
    public static SparkSessionConfigBuilder forKubernetes() {
        return new SparkSessionConfigBuilder().master(DEFAULT_KUBERNETES_MASTER);
    }

    public SparkSessionConfigBuilder appName(String appName) {
        this.appName = appName;
        return this;
    }

    public SparkSessionConfigBuilder master(String master) {
        this.master = master;
        return this;
    }

    /** Override the environment lookup (used by tests to inject a fixed env without touching the OS). */
    public SparkSessionConfigBuilder withEnvironment(Function<String, String> environment) {
        this.environment = environment;
        return this;
    }

    /** Layer in one grouped block of a {@code spark.json} (e.g. the "delta" or "common" object). */
    public SparkSessionConfigBuilder withGroup(Map<String, String> group) {
        if (group != null) {
            properties.putAll(group);
        }
        return this;
    }

    /** Set a single property (last write wins, mirroring the Python dict update order). */
    public SparkSessionConfigBuilder withProperty(String key, String value) {
        properties.put(key, value);
        return this;
    }

    /**
     * Apply an environment override for a {@code spark.*} property: if {@code envVar} is set in the
     * environment, its value replaces (or adds) {@code sparkKey}. This is the
     * {@code os.getenv(..., default)} pattern from {@code spark_configs.py}, but explicit so the
     * precedence (env > spark.json > defaults) is testable.
     */
    public SparkSessionConfigBuilder withEnvOverride(String sparkKey, String envVar) {
        String value = environment.apply(envVar);
        if (value != null && !value.isEmpty()) {
            properties.put(sparkKey, value);
        }
        return this;
    }

    public SparkSessionConfig build() {
        // SPARK_MASTER env always wins for the master (matches spark_manager.py's os.getenv("SPARK_MASTER")).
        String envMaster = environment.apply("SPARK_MASTER");
        String resolvedMaster = (envMaster != null && !envMaster.isEmpty()) ? envMaster : master;
        return new SparkSessionConfig(appName, resolvedMaster, properties);
    }
}
