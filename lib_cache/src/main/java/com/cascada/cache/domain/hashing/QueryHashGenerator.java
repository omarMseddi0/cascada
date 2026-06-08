package com.cascada.cache.domain.hashing;

import com.cascada.cache.domain.CacheConstants;
import com.cascada.cache.domain.CanonicalQueryObject;
import com.cascada.cache.domain.HashComponents;
import com.cascada.identity.domain.QueryHash;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Generates the time-independent logic hash of a query, ported from {@code generate_query_hash} in
 * {@code cache_hashing.py}.
 *
 * <p>Two correctness properties are preserved exactly:
 * <ul>
 *   <li><b>Fixed-step strategy</b> — for a time-series query the step folded into the hash is the
 *       universal fixed step (default 300s), never the user's requested step, so that a one-day and
 *       a seven-day version of the same query share buckets. Global aggregates use step 0.</li>
 *   <li><b>Order independence</b> — group-by, aggregate, filter, source and projection lists are
 *       sorted before hashing, and the canonical JSON sorts object keys, so reordering clauses does
 *       not change the hash.</li>
 * </ul>
 *
 * <p>The digest is MD5 over the UTF-8 canonical string, exactly as the Python reference.
 */
public final class QueryHashGenerator {

    /** Builds the logic hash for a canonical object using the platform default fixed step. */
    public QueryHash generateQueryHash(CanonicalQueryObject canonicalObject) {
        return generateQueryHash(canonicalObject, CacheConstants.DEFAULT_CACHE_STEP_SECONDS);
    }

    public QueryHash generateQueryHash(CanonicalQueryObject canonicalObject, int fixedStepSeconds) {
        int step = canonicalObject.metadata().isTimeSeries() ? fixedStepSeconds : 0;
        String canonicalString = buildCanonicalString(canonicalObject, step);
        return new QueryHash(md5Hexadecimal(canonicalString));
    }

    /** Exposed for tests and diagnostics: the exact string that is hashed. */
    public String buildCanonicalString(CanonicalQueryObject canonicalObject, int step) {
        HashComponents components = canonicalObject.hashComponents();

        Map<String, Object> hashDna = new TreeMap<>();
        hashDna.put("group_by", sortedCopy(components.groupBy()));
        hashDna.put("aggregates", sortedCopy(components.aggregates()));
        hashDna.put("filters", sortedCopy(components.filters()));
        hashDna.put("step", step);
        hashDna.put("source_signature", sortedCopy(canonicalObject.sourceSignature()));
        hashDna.put("projection_signature", sortedCopy(canonicalObject.projectionSignature()));

        Map<String, String> compositeAliases = canonicalObject.metadata().compositeAliases();
        if (!compositeAliases.isEmpty()) {
            hashDna.put("composite_aliases", new TreeMap<>(compositeAliases));
        }

        return CanonicalJsonWriter.write(hashDna);
    }

    private static List<String> sortedCopy(List<String> values) {
        return values.stream().sorted().toList();
    }

    private static String md5Hexadecimal(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] hashed = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hexadecimal = new StringBuilder(hashed.length * 2);
            for (byte singleByte : hashed) {
                hexadecimal.append(Character.forDigit((singleByte >> 4) & 0xF, 16));
                hexadecimal.append(Character.forDigit(singleByte & 0xF, 16));
            }
            return hexadecimal.toString();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("MD5 is a required JDK algorithm but was unavailable", impossible);
        }
    }
}
