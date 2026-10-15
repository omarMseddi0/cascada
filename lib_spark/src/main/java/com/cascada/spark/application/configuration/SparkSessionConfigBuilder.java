package com.cascada.spark.application.configuration;

import com.cascada.spark.application.port.out.EnvironmentPort;
import com.cascada.spark.domain.SparkSessionConfig;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.LinkedHashSet;
import java.util.Arrays;

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
    public static final String GLUTEN_PLUGIN = "org.apache.gluten.GlutenPlugin";
    public static final String GLUTEN_EXTENSIONS = "org.apache.gluten.extension.GlutenSessionExtensions";
    public static final String GLUTEN_SHUFFLE = "org.apache.spark.shuffle.sort.ColumnarShuffleManager";
    public static final long DEFAULT_VELOX_OFFHEAP_BYTES = 512L * 1024 * 1024;

    private static final String DEFAULT_KUBERNETES_MASTER =
            "k8s://https://kubernetes.default.svc.cluster.local:443";
    private static final String DEFAULT_LOCAL_MASTER = "local[*]";

    private String appName = "CascadaDeltaQueryExecutor";
    private String master = DEFAULT_KUBERNETES_MASTER;
    private final Map<String, String> properties = new LinkedHashMap<>();
    /**
     * Defaults to "nothing is configured" so this builder is deterministic on every machine. The
     * composition root opts in to the real process environment by passing
     * {@code SystemEnvironmentAdapter.INSTANCE}; reading the OS is never implicit.
     */
    private EnvironmentPort environment = EnvironmentPort.empty();

    public SparkSessionConfigBuilder() {
        // Delta-enabling defaults — the minimum that makes delta.`/path` queryable.
        properties.put("spark.sql.extensions", DELTA_EXTENSIONS);
        properties.put("spark.sql.catalog.spark_catalog", DELTA_CATALOG);
        // Sensible cross-environment defaults (overridable by spark.json / env).
        properties.put("spark.sql.adaptive.enabled", "true");
        properties.put("spark.scheduler.mode", "FAIR");
        properties.put("spark.sql.adaptive.coalescePartitions.enabled", "true");
        // Native execution is the product default; explicit false opts into portable JVM execution.
        withVelox(DEFAULT_VELOX_OFFHEAP_BYTES);
    }

    /** A local in-process session (single node / dev / tests): {@code local[*]} master. */
    public static SparkSessionConfigBuilder forLocal() {
        return new SparkSessionConfigBuilder().master(DEFAULT_LOCAL_MASTER);
    }

    /** A Kubernetes session (the production data plane): {@code k8s://...} master. */
    public static SparkSessionConfigBuilder forKubernetes() {
        SparkSessionConfigBuilder builder = new SparkSessionConfigBuilder().master(DEFAULT_KUBERNETES_MASTER);
        // K8s has no external shuffle service: if the operator's spark.json enables dynamic
        // allocation, it is only legal with shuffle tracking — default it on so a spark.json that
        // omits the key doesn't produce a session that refuses to start. spark.json still wins.
        builder.properties.put("spark.dynamicAllocation.shuffleTracking.enabled", "true");
        return builder;
    }

    public SparkSessionConfigBuilder appName(String appName) {
        this.appName = appName;
        return this;
    }

    public SparkSessionConfigBuilder master(String master) {
        this.master = master;
        return this;
    }

    /**
     * Supply the environment this builder reads. Pass {@code SystemEnvironmentAdapter.INSTANCE} in a
     * composition root, or a map-backed lambda in a test. Omitting it means "nothing is set".
     */
    public SparkSessionConfigBuilder withEnvironment(EnvironmentPort environment) {
        this.environment = environment == null ? EnvironmentPort.empty() : environment;
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

    /** Configure Velox. The matching Gluten bundle must already be on the driver JVM classpath. */
    public SparkSessionConfigBuilder withVelox(long offHeapBytes) {
        if (offHeapBytes <= 0) throw new IllegalArgumentException("Velox off-heap bytes must be positive");
        LinkedHashSet<String> plugins = new LinkedHashSet<>();
        Arrays.stream(properties.getOrDefault("spark.plugins", "").split(","))
                .map(String::trim).filter(value -> !value.isEmpty()).forEach(plugins::add);
        plugins.add(GLUTEN_PLUGIN);
        properties.put("spark.plugins", String.join(",", plugins));
        properties.put("spark.gluten.enabled", "true");
        // Gluten 1.7 falls back for Spark 4's default ANSI mode. This native profile is explicit
        // about legacy arithmetic/cast semantics; an operator's subsequent true override wins.
        properties.putIfAbsent("spark.sql.ansi.enabled", "false");
        properties.put("spark.shuffle.manager", GLUTEN_SHUFFLE);
        properties.put("spark.memory.offHeap.enabled", "true");
        properties.put("spark.memory.offHeap.size", Long.toString(offHeapBytes));
        // Apache Gluten recommends task memory isolation when concurrent queries share a session.
        // Task-slot quotas are derived by Gluten from Spark's resources; don't hardcode its
        // internal per-task byte settings or bypass tracked memory/spilling.
        properties.putIfAbsent("spark.gluten.memory.isolation", "true");
        properties.putIfAbsent("spark.gluten.sql.columnar.backend.velox.spillStrategy", "auto");
        properties.putIfAbsent("spark.gluten.sql.columnar.maxBatchSize", "4096");
        properties.putIfAbsent("spark.gluten.memory.dynamic.offHeap.sizing.enabled", "false");
        synchronizeVeloxExtensions();
        return this;
    }

    private void synchronizeVeloxExtensions() {
        boolean enabled = Arrays.stream(properties.getOrDefault("spark.plugins", "").split(","))
                .map(String::trim).anyMatch(GLUTEN_PLUGIN::equals);
        LinkedHashSet<String> extensions = new LinkedHashSet<>();
        Arrays.stream(properties.getOrDefault("spark.sql.extensions", "").split(","))
                .map(String::trim).filter(value -> !value.isEmpty() && !value.equals(GLUTEN_EXTENSIONS)).forEach(extensions::add);
        if (enabled) extensions.add(GLUTEN_EXTENSIONS);
        properties.put("spark.sql.extensions", String.join(",", extensions));
    }

    private void applyVeloxEnvironment() {
        String enabled = environment.get("CASCADA_GLUTEN_ENABLED");
        if (enabled == null || enabled.isBlank()) return;
        if (!enabled.equalsIgnoreCase("true") && !enabled.equalsIgnoreCase("false"))
            throw new IllegalArgumentException("CASCADA_GLUTEN_ENABLED must be true or false");
        if (Boolean.parseBoolean(enabled)) {
            String bytes = environment.get("CASCADA_GLUTEN_OFFHEAP_BYTES");
            String configuredMemory = properties.get("spark.memory.offHeap.size");
            long memory;
            try { memory = bytes == null || bytes.isBlank() ? DEFAULT_VELOX_OFFHEAP_BYTES : Long.parseLong(bytes); }
            catch (NumberFormatException invalid) {
                throw new IllegalArgumentException("CASCADA_GLUTEN_OFFHEAP_BYTES must be positive integer bytes", invalid);
            }
            withVelox(memory);
            // Enabling the plugin alone must not reset a reviewed spark.json memory budget.
            if ((bytes == null || bytes.isBlank()) && configuredMemory != null)
                properties.put("spark.memory.offHeap.size", configuredMemory);
            String jar = environment.get("CASCADA_GLUTEN_JAR");
            if (jar != null && !jar.isBlank()) {
                LinkedHashSet<String> jars = new LinkedHashSet<>();
                Arrays.stream(properties.getOrDefault("spark.jars", "").split(","))
                        .map(String::trim).filter(value -> !value.isEmpty()).forEach(jars::add);
                jars.add(jar);
                properties.put("spark.jars", String.join(",", jars));
            }
        } else {
            properties.put("spark.gluten.enabled", "false");
            String plugins = Arrays.stream(properties.getOrDefault("spark.plugins", "").split(","))
                    .map(String::trim).filter(value -> !value.isEmpty() && !value.equals(GLUTEN_PLUGIN))
                    .collect(java.util.stream.Collectors.joining(","));
            if (plugins.isEmpty()) properties.remove("spark.plugins");
            else properties.put("spark.plugins", plugins);
            if (GLUTEN_SHUFFLE.equals(properties.get("spark.shuffle.manager"))) properties.remove("spark.shuffle.manager");
        }
    }

    /**
     * Apply an environment override for a {@code spark.*} property: if {@code envVar} is set in the
     * environment, its value replaces (or adds) {@code sparkKey}. This is the
     * {@code os.getenv(..., default)} pattern from {@code spark_configs.py}, but explicit so the
     * precedence (env > spark.json > defaults) is testable.
     */
    public SparkSessionConfigBuilder withEnvOverride(String sparkKey, String envVar) {
        String value = environment.get(envVar);
        if (value != null && !value.isEmpty()) {
            properties.put(sparkKey, value);
        }
        return this;
    }

    public SparkSessionConfig build() {
        applyVeloxEnvironment();
        // In-process Spark builders can override the extension injected by the driver plugin.
        // Reconcile after grouped JSON and environment overrides, preserving Delta and other hooks.
        synchronizeVeloxExtensions();
        // SPARK_MASTER env always wins for the master (matches spark_manager.py's os.getenv("SPARK_MASTER")).
        String envMaster = environment.get("SPARK_MASTER");
        String resolvedMaster = (envMaster != null && !envMaster.isEmpty()) ? envMaster : master;
        return new SparkSessionConfig(appName, resolvedMaster, properties);
    }
}
