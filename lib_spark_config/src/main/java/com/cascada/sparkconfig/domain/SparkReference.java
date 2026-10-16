package com.cascada.sparkconfig.domain;

import java.util.Locale;
import java.util.Map;

/**
 * The Spark/Delta/Gluten names and reference values that every part of the platform must agree on:
 * the configuration deriver, the Kubernetes deployment settings and the session builder. Keeping them
 * here means a class name or a default cannot drift between the place a value is derived and the place
 * it is deployed.
 */
public final class SparkReference {

    public static final String DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension";
    public static final String DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog";
    public static final String GLUTEN_PLUGIN = "org.apache.gluten.GlutenPlugin";
    public static final String GLUTEN_SESSION_EXTENSIONS = "org.apache.gluten.extension.GlutenSessionExtensions";
    public static final String COLUMNAR_SHUFFLE_MANAGER = "org.apache.spark.shuffle.sort.ColumnarShuffleManager";
    public static final String SORT_SHUFFLE_MANAGER = "org.apache.spark.shuffle.sort.SortShuffleManager";

    public static final String DRIVER_MEMORY = "2g";
    public static final int DRIVER_PORT = 8002;
    public static final int BLOCK_MANAGER_PORT = 8001;
    public static final String CONTAINER_IMAGE = "cascada:0.1.0";
    public static final int INITIAL_EXECUTOR_COUNT = 3;

    /** Smallest executor RAM that fits a heap and explicit overhead; Gluten also needs off-heap room. */
    public static final int MINIMUM_EXECUTOR_RAM_GIGABYTES = 2;
    public static final int MINIMUM_GLUTEN_EXECUTOR_RAM_GIGABYTES = 3;

    /**
     * Velox defaults applied when Gluten is on and the key is not configured. The Kubernetes
     * deployment's {@code spark.json} must carry the same values.
     */
    public static final Map<String, String> GLUTEN_TUNING_DEFAULTS = Map.of(
            "spark.sql.ansi.enabled", "false",
            "spark.gluten.memory.isolation", "true",
            "spark.gluten.sql.columnar.backend.velox.spillStrategy", "auto",
            "spark.gluten.sql.columnar.maxBatchSize", "4096",
            "spark.gluten.memory.dynamic.offHeap.sizing.enabled", "false");

    private SparkReference() {
    }

    /** The startup executor target, kept inside the dynamic-allocation budget. */
    public static int initialExecutors(int minimumExecutors, int maximumExecutors) {
        return Math.max(minimumExecutors, Math.min(INITIAL_EXECUTOR_COUNT, maximumExecutors));
    }

    /** Kubernetes CPU limit for an executor pod: one core of headroom over {@code spark.executor.cores}. */
    public static int executorLimitCores(int executorCores) {
        return executorCores + 1;
    }

    public static int minimumExecutorRamGigabytes(boolean glutenEnabled) {
        return glutenEnabled ? MINIMUM_GLUTEN_EXECUTOR_RAM_GIGABYTES : MINIMUM_EXECUTOR_RAM_GIGABYTES;
    }

    /**
     * The one contract for {@code CASCADA_GLUTEN_ENABLED}: {@code true} or {@code false} in any case,
     * surrounding blanks ignored; unset or blank means {@code defaultValue}; anything else is rejected.
     */
    public static boolean parseGlutenEnabled(String rawValue, boolean defaultValue) {
        if (rawValue == null || rawValue.isBlank()) {
            return defaultValue;
        }
        return switch (rawValue.trim().toLowerCase(Locale.ROOT)) {
            case "true" -> true;
            case "false" -> false;
            default -> throw new IllegalArgumentException("CASCADA_GLUTEN_ENABLED must be true or false");
        };
    }
}
