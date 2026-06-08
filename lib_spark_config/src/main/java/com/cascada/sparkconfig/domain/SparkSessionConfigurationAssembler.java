package com.cascada.sparkconfig.domain;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Assembles the <em>complete</em> Spark session configuration the orchestrator hands to
 * {@code SparkSession.builder}, the Java analogue of {@code spark_manager.get_full_config()}: it merges
 * the fixed {@link StaticSparkInfrastructureSettings} with the knob-derived
 * {@link SparkConfigurationDeriver} output (derived values win on the few overlapping keys, exactly as
 * {@code get_full_config()} layers the tuned sections last).
 *
 * <p>It also carries the same change-application contract the Python {@code SparkSessionManager}
 * encodes: {@link #LIVE_MUTABLE_KEYS} can be applied to a running session via {@code spark.conf.set},
 * while {@link #RESTART_REQUIRED_KEYS} (JVM/K8s startup specs) need a stop+recreate. Session creation
 * itself (the {@code builder.config(...).getOrCreate()} call) lives in a Spark-runtime adapter, not in
 * this framework-free domain; this class produces the exact map that adapter would apply.
 */
public final class SparkSessionConfigurationAssembler {

    /** Keys safe to apply live on a running session (ported from {@code _LIVE_MUTABLE_SPARK_KEYS}). */
    public static final Set<String> LIVE_MUTABLE_KEYS = Set.of(
            "spark.sql.shuffle.partitions");

    /** Keys that require a session stop+recreate to change (ported from {@code _RESTART_REQUIRED_SPARK_KEYS}). */
    public static final Set<String> RESTART_REQUIRED_KEYS = Set.of(
            "spark.driver.memory",
            "spark.executor.memory",
            "spark.driver.cores",
            "spark.executor.cores",
            "spark.dynamicAllocation.minExecutors",
            "spark.dynamicAllocation.maxExecutors");

    /** Merge static infrastructure with the knob-derived config; derived values win on overlap. */
    public SparkConfiguration assembleFullConfiguration(SparkConfiguration derived) {
        Map<String, String> full = new LinkedHashMap<>(StaticSparkInfrastructureSettings.referenceInfrastructure());
        full.putAll(derived.entries()); // tuned/derived keys layered last, like get_full_config()
        return new SparkConfiguration(full);
    }

    public boolean isLiveMutable(String key) {
        return LIVE_MUTABLE_KEYS.contains(key);
    }

    public boolean requiresRestart(String key) {
        return RESTART_REQUIRED_KEYS.contains(key);
    }
}
