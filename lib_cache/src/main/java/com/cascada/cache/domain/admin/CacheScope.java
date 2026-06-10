package com.cascada.cache.domain.admin;

import com.cascada.identity.domain.TenantIdentifier;

import java.util.Objects;

/**
 * What a flush/size operation applies to (plan §8.17). Because every key embeds the tenant segment
 * inside its signed material ({@link com.cascada.cache.domain.CacheKeyFactory}), scoping is purely a
 * key-prefix match — there is no way for a tenant-scoped flush to reach another tenant's buckets.
 *
 * <ul>
 *   <li>{@link #everything()} — the whole cache (admin "Flush all").</li>
 *   <li>{@link #forTenant(TenantIdentifier)} — only one tenant's buckets.</li>
 *   <li>{@link #forKeyPrefix(String)} — any explicit prefix (e.g. one query-hash family, or
 *       {@code QC:V4:B86400:} to drop only day-buckets) for surgical eviction from the console.</li>
 * </ul>
 */
public final class CacheScope {

    private final String keyPrefix;
    private final String description;

    private CacheScope(String keyPrefix, String description) {
        this.keyPrefix = keyPrefix;
        this.description = description;
    }

    public static CacheScope everything() {
        return new CacheScope("", "all tenants");
    }

    public static CacheScope forTenant(TenantIdentifier tenant) {
        Objects.requireNonNull(tenant, "tenant");
        // The trailing ':' pins the match to the full tenant segment — without it, flushing
        // tenant "abc" would also sweep "abcd"'s buckets ("abcd:QC:..." startsWith "abc").
        return new CacheScope(tenant.asKeyPrefixSegment() + ":", "tenant " + tenant.asKeyPrefixSegment());
    }

    public static CacheScope forKeyPrefix(String keyPrefix) {
        Objects.requireNonNull(keyPrefix, "keyPrefix");
        return new CacheScope(keyPrefix, "prefix '" + keyPrefix + "'");
    }

    /** True when this scope covers the entire cache (no prefix filter). */
    public boolean isEverything() {
        return keyPrefix.isEmpty();
    }

    /** True iff {@code key} belongs to this scope. The empty prefix matches everything. */
    public boolean matches(String key) {
        return keyPrefix.isEmpty() || key.startsWith(keyPrefix);
    }

    public String keyPrefix() {
        return keyPrefix;
    }

    @Override
    public String toString() {
        return description;
    }
}
