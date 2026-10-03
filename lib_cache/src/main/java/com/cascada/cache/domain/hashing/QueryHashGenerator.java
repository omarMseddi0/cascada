package com.cascada.cache.domain.hashing;

import com.cascada.cache.domain.query.CanonicalQueryObject;
import com.cascada.cache.domain.time.CacheTimeConstants;
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
 *   <li><b>Stable clause identity</b> — group-by, aggregate, filter and source lists are sorted
 *       before hashing, while projection order is preserved because SQL result columns are
 *       ordinal. The canonical JSON sorts object keys.</li>
 * </ul>
 *
 * <p>The digest is MD5 over the UTF-8 canonical string. Its semantics version prevents entries
 * created under older projection-order rules from being reused after a cache identity change.
 */
public final class QueryHashGenerator {

    /** Version 2 separates corrected ordinal projection hashes from entries written by version 1. */
    private static final int CANONICAL_SEMANTICS_VERSION = 2;

    /** Builds the logic hash for a canonical object using the platform default fixed step. */
    public QueryHash generateQueryHash(CanonicalQueryObject canonicalObject) {
        return generateQueryHash(canonicalObject, CacheTimeConstants.DEFAULT_CACHE_STEP_SECONDS);
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
        hashDna.put("canonical_semantics_version", CANONICAL_SEMANTICS_VERSION);
        hashDna.put("group_by", sortedCopy(components.groupBy()));
        hashDna.put("aggregates", sortedCopy(components.aggregates()));
        hashDna.put("filters", sortedCopy(components.filters()));
        hashDna.put("step", step);
        hashDna.put("source_signature", sortedCopy(canonicalObject.sourceSignature()));
        hashDna.put("projection_signature", canonicalObject.projectionSignature());

        Map<String, String> compositeAliases = canonicalObject.metadata().compositeAliases();
        if (!compositeAliases.isEmpty()) {
            hashDna.put("composite_aliases", new TreeMap<>(compositeAliases));
        }

        // HAVING, JOIN ON conditions and DISTINCT change the answer, so they must change the key.
        List<String> logicSignature = canonicalObject.logicSignature();
        if (!logicSignature.isEmpty()) {
            hashDna.put("logic_signature", sortedCopy(logicSignature));
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
