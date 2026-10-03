package com.cascada.cache.domain.admin;

import com.cascada.identity.domain.TenantIdentifier;

import java.util.Objects;

/**
 * What a flush operation applies to (plan §8.17). The current bucket-key format supports either a
 * tenant-prefixed key or the bare {@code QC:V4:...} form attributed to the default tenant. Tenant
 * scopes use that attribution rule; prefix scopes remain explicit operator-selected string prefixes.
 *
 * <ul>
 *   <li>{@link #everything()} — every Cascada bucket key (admin "Flush all"), leaving unrelated keys alone.</li>
 *   <li>{@link #forTenant(TenantIdentifier)} — only one tenant's buckets.</li>
 *   <li>{@link #forKeyPrefix(String)} — any explicit prefix (e.g. one query-hash family, or
 *       {@code QC:V4:B86400:} to drop only day-buckets) for surgical eviction from the console.</li>
 * </ul>
 */
public final class CacheScope {

    private final String keyPrefix;
    private final String description;
    private final boolean everything;
    private final String tenantSegment;

    private CacheScope(String keyPrefix, String description, boolean everything, String tenantSegment) {
        this.keyPrefix = keyPrefix;
        this.description = description;
        this.everything = everything;
        this.tenantSegment = tenantSegment;
    }

    public static CacheScope everything() {
        return new CacheScope("", "all Cascada buckets", true, null);
    }

    public static CacheScope forTenant(TenantIdentifier tenant) {
        Objects.requireNonNull(tenant, "tenant");
        String segment = tenant.asKeyPrefixSegment();
        // A bare QC:V4 key is attributed to the default tenant, so its tenant scope must include
        // both bare and explicitly prefixed default keys.
        return new CacheScope(segment + ":", "tenant " + segment, false, segment);
    }

    public static CacheScope forKeyPrefix(String keyPrefix) {
        Objects.requireNonNull(keyPrefix, "keyPrefix");
        return new CacheScope(keyPrefix, "prefix '" + keyPrefix + "'", false, null);
    }

    /** True when this scope covers the entire cache (no prefix filter). */
    public boolean isEverything() {
        return everything;
    }

    /** True iff {@code key} belongs to this scope. Flush-all is limited to Cascada bucket keys. */
    public boolean matches(String key) {
        if (everything) {
            return CacheKeyTenantSegment.isBucketKey(key);
        }
        if (tenantSegment != null) {
            return CacheKeyTenantSegment.isBucketKey(key)
                    && tenantSegment.equals(CacheKeyTenantSegment.of(key));
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
