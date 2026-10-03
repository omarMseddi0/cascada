package com.cascada.cache.domain.time;

/** Shared time units and fixed cache-step policy used by the cache domain. */
public final class CacheTimeConstants {

    private CacheTimeConstants() {
    }

    public static final int SECONDS_PER_DAY = 86_400;
    public static final int DEFAULT_CACHE_STEP_SECONDS = 300;
    public static final int SECONDS_PER_DAY_MINUS_ONE = 86_399;
}
