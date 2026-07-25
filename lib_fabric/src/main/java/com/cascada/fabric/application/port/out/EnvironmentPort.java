package com.cascada.fabric.application.port.out;

/**
 * <b>Secondary (driven) port</b> for reading the deployment configuration that sizes and names a
 * cluster ({@code CASCADA_*} variables today; a ConfigMap, a Helm values file, or a control-plane API
 * tomorrow).
 *
 * <p><b>Why this exists.</b> {@code ClusterValues.fromSystemEnvironment()} called
 * {@link System#getenv(String)} from what is otherwise a pure value object. That put an I/O call in the
 * innermost ring, which is precisely what the dependency rule forbids — and it meant the only way to
 * test the defaults was to mutate the real process environment or thread a lambda through by hand.
 *
 * <p>The domain now depends on this interface; {@code SystemEnvironmentAdapter} is the single
 * implementation that touches the OS, and a test passes a map instead. Swapping env vars for a
 * ConfigMap later is a new adapter, not a change to the manifest logic.
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
