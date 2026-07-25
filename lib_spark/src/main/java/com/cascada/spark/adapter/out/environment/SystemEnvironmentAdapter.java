package com.cascada.spark.adapter.out.environment;

import com.cascada.spark.application.port.out.EnvironmentPort;

/**
 * The one class in this module allowed to read the operating system: an {@link EnvironmentPort} backed
 * by {@link System#getenv(String)}.
 *
 * <p>It is deliberately trivial and deliberately isolated. Because it is the only path to the real
 * environment, "does this code depend on the machine it runs on?" is answered by a single grep for this
 * class name, and every other class in the module is provably deterministic. A composition root passes
 * {@link #INSTANCE}; a test passes a map-backed lambda instead.
 */
public final class SystemEnvironmentAdapter implements EnvironmentPort {

    /** Shared instance — the adapter is stateless. */
    public static final SystemEnvironmentAdapter INSTANCE = new SystemEnvironmentAdapter();

    private SystemEnvironmentAdapter() {
    }

    @Override
    public String get(String name) {
        String value = System.getenv(name);
        return (value == null || value.isBlank()) ? null : value;
    }
}
