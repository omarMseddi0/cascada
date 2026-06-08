package com.cascada.identity.domain;

import java.util.regex.Pattern;

/**
 * The stable, opaque identity of one customer workspace.
 *
 * <p>Cascada is multi-tenant by construction: every cache key, every metadata row,
 * and every metering event is prefixed with a {@code TenantIdentifier}, which is what
 * makes cross-tenant reads impossible rather than merely forbidden (plan §8.4, §12).
 *
 * <p>This is a framework-free value object: it imports nothing outside the JDK.
 */
public record TenantIdentifier(String value) {

    private static final Pattern ALLOWED_CHARACTERS = Pattern.compile("^[a-z0-9][a-z0-9_-]{0,62}$");

    public TenantIdentifier {
        if (value == null) {
            throw new IllegalArgumentException("tenant identifier value must not be null");
        }
        if (!ALLOWED_CHARACTERS.matcher(value).matches()) {
            throw new IllegalArgumentException(
                    "tenant identifier must be 1-63 chars of lowercase letters, digits, hyphen or underscore "
                            + "and start with a letter or digit, but was: '" + value + "'");
        }
    }

    public static TenantIdentifier of(String value) {
        return new TenantIdentifier(value);
    }

    /** The prefix segment used when rendering tenant-scoped cache and metadata keys. */
    public String asKeyPrefixSegment() {
        return value;
    }
}
