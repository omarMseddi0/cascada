package com.cascada.fabric.application.port.out;

/**
 * <b>Secondary (driven) port</b> for reading the deployment configuration that sizes and names a
 * cluster ({@code CASCADA_*} variables today; a ConfigMap, a Helm values file, or a control-plane API
 * tomorrow).
 *
 * <p><b>Why this exists.</b> Reading deployment settings is an external concern. This port keeps the
 * application independent of the process environment and lets tests provide deterministic values.
 *
 * <p>{@code ClusterSettingsReader} consumes this interface; {@code SystemEnvironmentAdapter} is the
 * single implementation that touches the OS, and tests can pass a map-backed implementation instead.
 * Swapping environment variables for a ConfigMap later is a new adapter, not a change to domain logic.
 *
 * <p>This module declares its own copy rather than sharing {@code lib_spark}'s identical port on
 * purpose: a port belongs to the hexagon that needs it, and a shared "common utils" module every
 * hexagon depends on is the coupling this architecture exists to avoid.
 */
public interface EnvironmentPort {

    /** The value of {@code name}, or {@code null} when unset or blank. */
    String get(String name);

    /** {@code name}'s value, or {@code fallback} when unset or blank. */
    default String getOrDefault(String name, String fallback) {
        String value = get(name);
        return (value == null || value.isBlank()) ? fallback : value;
    }

    /** The deterministic default: nothing is configured, so every value falls back to its default. */
    static EnvironmentPort empty() {
        return name -> null;
    }
}
