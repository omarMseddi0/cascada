package com.cascada.cache.domain.admin;

import com.cascada.cache.domain.CacheConstants;

/**
 * Derives the tenant bucket a stored key belongs to, for the per-tenant size breakdown. Production keys
 * are {@code <tenantSegment>:QC:V4:...} (the tenant prefix is prepended to the bucket key from
 * {@link com.cascada.cache.domain.CacheKeyFactory}); a bare {@code QC:V4:...} key (single-tenant/dev) is
 * attributed to {@code "default"}. This is the one place that knows the prefix convention, so the
 * adapters and the report agree.
 */
public final class CacheKeyTenantSegment {

    /** The bucket attributed to keys that carry no explicit tenant prefix. */
    public static final String DEFAULT_TENANT = "default";

    private CacheKeyTenantSegment() {
    }

    public static String of(String key) {
        int marker = key.indexOf(CacheConstants.CACHE_KEY_PREFIX);
        if (marker <= 0) {
            // marker == 0 → key starts with QC:V4 (no tenant prefix); marker < 0 → no marker at all
            return DEFAULT_TENANT;
        }
        // strip the trailing ':' that joins the tenant segment to the bucket key, if present
        String prefix = key.substring(0, marker);
        return prefix.endsWith(":") ? prefix.substring(0, prefix.length() - 1) : prefix;
    }
}
