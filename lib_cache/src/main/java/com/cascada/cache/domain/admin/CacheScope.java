package com.cascada.cache.domain.admin;

import com.cascada.cache.domain.key.CacheKeyConstants;

import java.util.Objects;

/**
 * What a flush operation applies to. Flush-all matches Cascada bucket keys, while prefix scopes
 * provide explicit operator-selected string prefixes for surgical eviction.
 *
 * <ul>
 *   <li>{@link #everything()} — every Cascada bucket key, leaving unrelated keys alone.</li>
 *   <li>{@link #forKeyPrefix(String)} — an explicit prefix such as one query-hash family or bucket-width band.</li>
 * </ul>
 */
public final class CacheScope {

    private final String keyPrefix;
    private final String description;
    private final boolean everything;

    private CacheScope(String keyPrefix, String description, boolean everything) {
        this.keyPrefix = keyPrefix;
        this.description = description;
        this.everything = everything;
    }

    public static CacheScope everything() {
        return new CacheScope("", "all Cascada buckets", true);
    }

    public static CacheScope forKeyPrefix(String keyPrefix) {
        Objects.requireNonNull(keyPrefix, "keyPrefix");
        return new CacheScope(keyPrefix, "prefix '" + keyPrefix + "'", false);
    }

    /** True when this scope covers the entire cache (no prefix filter). */
    public boolean isEverything() {
        return everything;
    }

    /** True iff {@code key} belongs to this scope. Flush-all is limited to Cascada bucket keys. */
    public boolean matches(String key) {
        if (everything) {
            return CacheKeyConstants.isBucketKey(key);
        }
        return key.startsWith(keyPrefix);
    }

    public String keyPrefix() {
        return keyPrefix;
    }

    @Override
    public String toString() {
        return description;
    }
}
