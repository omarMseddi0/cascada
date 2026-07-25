package com.cascada.spark.application.port.out;

/**
 * <b>Secondary (driven) port</b> for reading deployment configuration supplied by the surrounding
 * environment (process environment variables, a mounted ConfigMap, a secrets manager).
 *
 * <p><b>Why this exists.</b> {@code SparkSessionConfigBuilder} is a pure function that must be provable
 * without a machine: given defaults, a {@code spark.json} group, and some overrides, it produces one
 * flat property map. It previously defaulted its lookup to {@code System::getenv}, which quietly made a
 * "pure" domain class read the operating system — the class could then behave differently on two
 * machines, and a test had to remember to override the default to stay deterministic. Reading the OS is
 * an I/O concern and belongs in an adapter.
 *
 * <p>The domain now defaults to {@link #empty()} (nothing is set), so the untouched behaviour is the
 * deterministic one and reaching the real environment is an explicit, visible decision made in the
 * composition root via {@code SystemEnvironmentAdapter}.
 *
 * <p>Each module declares its own copy of this two-method seam rather than sharing one. That is
 * deliberate: a port belongs to the hexagon that needs it, and a shared "utilities" module that every
 * hexagon depends on is exactly the coupling this architecture exists to prevent.
 */
public interface EnvironmentPort {

    /** The value of {@code name}, or {@code null} when it is unset or blank. */
    String get(String name);

    /** {@code name}'s value, or {@code fallback} when unset or blank. */
    default String getOrDefault(String name, String fallback) {
        String value = get(name);
        return (value == null || value.isEmpty()) ? fallback : value;
    }

    /** The deterministic default: nothing is configured. Used by the domain and by unit tests. */
    static EnvironmentPort empty() {
        return name -> null;
    }
}
