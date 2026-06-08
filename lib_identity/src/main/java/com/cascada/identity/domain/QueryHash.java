package com.cascada.identity.domain;

import java.util.regex.Pattern;

/**
 * The logic hash of a query — the time-independent MD5 fingerprint over a query's
 * group-by, aggregates, filters, fixed step, source and projection signatures
 * (ported from {@code cache_hashing.py}).
 *
 * <p>Two queries with the same intent over different time ranges share the same
 * {@code QueryHash}; this is what lets a one-day query and a seven-day query reuse
 * each other's cached buckets (plan §3.3, §8.4).
 */
public record QueryHash(String value) {

    private static final Pattern HEXADECIMAL_MD5 = Pattern.compile("^[0-9a-f]{32}$");

    public QueryHash {
        if (value == null || !HEXADECIMAL_MD5.matcher(value).matches()) {
            throw new IllegalArgumentException(
                    "query hash must be a 32-character lowercase hexadecimal MD5 digest, but was: '" + value + "'");
        }
    }

    public static QueryHash of(String value) {
        return new QueryHash(value);
    }
}
