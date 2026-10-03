package com.cascada.cache.domain.admin;

import com.cascada.cache.domain.key.CacheKeyConstants;

import java.util.regex.Pattern;

/**
 * Derives the tenant bucket a stored key belongs to, for administration reports. Bucket keys may be
 * {@code <tenantSegment>:QC:V4:...} or bare {@code QC:V4:...}; bare keys are attributed to {@code "default"}.
 * This is the one place that knows the current prefix convention, so the adapters, scopes, and report agree.
 */
public final class CacheKeyTenantSegment {

    /** The bucket attributed to Cascada keys that carry no explicit tenant prefix. */
    public static final String DEFAULT_TENANT = "default";
    public static final String UNATTRIBUTED_TENANT = "unattributed";

    private static final Pattern TENANT_SEGMENT = Pattern.compile("[a-z0-9][a-z0-9_-]{0,62}");
    private static final Pattern BUCKET_SUFFIX = Pattern.compile(":B[1-9][0-9]*:[^:]+:-?[0-9]+");

    private CacheKeyTenantSegment() {
    }

    public static String of(String key) {
        if (!isBucketKey(key)) {
            return UNATTRIBUTED_TENANT;
        }
        int marker = key.indexOf(CacheKeyConstants.CACHE_KEY_PREFIX);
        if (marker == 0) {
            return DEFAULT_TENANT;
        }
        return key.substring(0, marker - 1);
    }

    /** True only for a Cascada bucket key, optionally prefixed by one valid tenant segment. */
    public static boolean isBucketKey(String key) {
        if (key == null) {
            return false;
        }
        int marker = key.indexOf(CacheKeyConstants.CACHE_KEY_PREFIX);
        if (marker < 0) {
            return false;
        }
        if (marker > 0) {
            if (key.charAt(marker - 1) != ':') {
                return false;
            }
            String tenant = key.substring(0, marker - 1);
            if (!TENANT_SEGMENT.matcher(tenant).matches()) {
                return false;
            }
        }
        String suffix = key.substring(marker + CacheKeyConstants.CACHE_KEY_PREFIX.length());
        return BUCKET_SUFFIX.matcher(suffix).matches();
    }
}
