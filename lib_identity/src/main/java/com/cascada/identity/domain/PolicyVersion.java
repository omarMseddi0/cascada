package com.cascada.identity.domain;

/**
 * The version of the cache rewriter / merge logic that produced an entry (plan §8.4).
 *
 * <p>Bumping the policy version when rewriter behaviour changes makes every previously
 * stored entry unreachable by construction, so a logic change can never serve a value
 * computed under the old, possibly-incorrect rules.
 */
public record PolicyVersion(int value) {

    public PolicyVersion {
        if (value < 1) {
            throw new IllegalArgumentException("policy version must be >= 1, but was: " + value);
        }
    }

    public static PolicyVersion of(int value) {
        return new PolicyVersion(value);
    }

    /** The current shipping policy version of the rewriter / merge logic. */
    public static PolicyVersion current() {
        return new PolicyVersion(4);
    }
}
