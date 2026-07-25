package com.cascada.fabric.adapter.out.environment;

import com.cascada.fabric.application.port.out.EnvironmentPort;

/**
 * The only class in this module allowed to read the operating system: an {@link EnvironmentPort} backed
 * by {@link System#getenv(String)}.
 *
 * <p>A composition root passes {@link #INSTANCE} to
 * {@code ClusterValues.fromEnvironment(...)}; every test passes a map-backed lambda. Keeping the OS
 * behind one named class is what makes the rest of the module provably deterministic.
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
