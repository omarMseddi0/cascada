package com.cascada.sql.canonical;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Expands {@code AVG} into {@code SUM} and {@code COUNT} for storage and hashing, ported verbatim
 * from {@code _normalize_aggregates} / {@code _extract_avg_column} in {@code cache_component_adapter.py}.
 *
 * <p>This is the linchpin of the AVG-correctness story: an average is never stored or hashed as
 * {@code AVG}; it is decomposed into the two distributive ingredients that <em>can</em> be merged
 * across buckets, and reconstructed only at the end (Root Cause 4).
 */
public final class AggregateNormalizer {

    private static final Pattern AVG_EXPRESSION = Pattern.compile("(?i)AVG\\s*\\(\\s*([^)]+)\\s*\\)");

    /** The result: the deduped, sorted hash forms, and the original aggregates iff an AVG was present. */
    public record NormalizedAggregates(List<String> normalizedForHash, List<String> originalAggregates) {
        public NormalizedAggregates {
            normalizedForHash = List.copyOf(normalizedForHash);
            originalAggregates = List.copyOf(originalAggregates);
        }
    }

    /** Ported from {@code _extract_avg_column}: pull the column out of an {@code AVG(...)} expression. */
    public String extractAverageColumn(String aggregateExpression) {
        Matcher matcher = AVG_EXPRESSION.matcher(aggregateExpression);
        if (matcher.find() && matcher.start() == 0) {
            return matcher.group(1).trim();
        }
        // The Python uses re.match (anchored at start); mirror that anchoring.
        Matcher anchored = AVG_EXPRESSION.matcher(aggregateExpression.trim());
        if (anchored.lookingAt()) {
            return anchored.group(1).trim();
        }
        return "";
    }

    /** Ported from {@code _normalize_aggregates}: AVG -> (SUM, COUNT); dedupe and sort the hash forms. */
    public NormalizedAggregates normalize(List<String> aggregates) {
        List<String> normalized = new ArrayList<>();
        boolean hasAverage = false;

        for (String aggregate : aggregates) {
            if (aggregate.strip().toUpperCase().startsWith("AVG(")) {
                hasAverage = true;
                String column = extractAverageColumn(aggregate);
                if (!column.isEmpty()) {
                    normalized.add("SUM(" + column + ")");
                    normalized.add("COUNT(" + column + ")");
                } else {
                    normalized.add(aggregate);
                }
            } else {
                normalized.add(aggregate);
            }
        }

        List<String> sortedDeduped = new ArrayList<>(new TreeSet<>(normalized));
        return new NormalizedAggregates(sortedDeduped, hasAverage ? List.copyOf(aggregates) : List.of());
    }
}
