package com.cascada.fabric.domain.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Immutable single-line scalar values passed to the manifest rendering boundary. */
public record ClusterValues(Map<String, String> placeholders) {
    public ClusterValues {
        placeholders.forEach((key, value) -> {
            if (key == null || !key.matches("[A-Za-z_][A-Za-z0-9_]*") || value == null
                    || !value.matches("[\\p{L}\\p{M}\\p{N}_./:@=+,%?~\\-]*")) {
                throw new IllegalArgumentException("Unsafe manifest placeholder: " + key
                        + "; use simple scalar values and percent-encode special URL characters");
            }
        });
        placeholders = Collections.unmodifiableMap(new LinkedHashMap<>(placeholders));
    }

    public String namespace() { return placeholders.get("namespace"); }
    public String driverWorkloadName() { return placeholders.get("driverWorkloadName"); }
    public String get(String key) { return placeholders.get(key); }
}
