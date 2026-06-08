package com.cascada.spark.config;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The fully-resolved Spark session configuration — an app name, a master URL, and the flat
 * {@code spark.*} key/value map that is fed to {@code SparkSession.builder.config(k, v)} one entry at a
 * time. Ported from the grouped-config → flat-config flow of {@code spark_configs.py.get_full_config}
 * and {@code spark_manager.py}.
 *
 * <p><b>Local and cluster are the same code, only this config differs</b> (the project's core premise):
 * a {@code local[*]} master with Delta extensions runs the identical executor the Kubernetes
 * ({@code k8s://...}) master does — the engine never knows which it is talking to. This value object is
 * deliberately framework-free (no Spark types) so it can be built and asserted without a Spark runtime.
 */
public final class SparkSessionConfig {

    private final String appName;
    private final String master;
    private final Map<String, String> sparkProperties;

    public SparkSessionConfig(String appName, String master, Map<String, String> sparkProperties) {
        this.appName = Objects.requireNonNull(appName, "appName");
        this.master = Objects.requireNonNull(master, "master");
        this.sparkProperties = new LinkedHashMap<>(Objects.requireNonNull(sparkProperties, "sparkProperties"));
    }

    public String appName() {
        return appName;
    }

    public String master() {
        return master;
    }

    /** The flat {@code spark.*} properties, insertion-ordered (mirrors the Python dict iteration). */
    public Map<String, String> sparkProperties() {
        return new LinkedHashMap<>(sparkProperties);
    }

    /** True when the master targets Kubernetes (the production data plane), false for local/dev. */
    public boolean isKubernetes() {
        return master.startsWith("k8s://");
    }

    /** True when the master is an in-process local runner (used by tests and single-node dev). */
    public boolean isLocal() {
        return master.startsWith("local");
    }
}
