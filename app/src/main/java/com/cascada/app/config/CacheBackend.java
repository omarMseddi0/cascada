package com.cascada.app.config;

import java.util.Locale;

/** Storage selection is independent of where Spark executes. */
public enum CacheBackend {
    MEMORY, VALKEY;

    public static CacheBackend parse(String value) {
        return switch (value.trim().toLowerCase(Locale.ROOT)) {
            case "memory" -> MEMORY;
            case "valkey" -> VALKEY;
            default -> throw new IllegalArgumentException("CASCADA_CACHE_BACKEND must be memory or valkey: " + value);
        };
    }
}
