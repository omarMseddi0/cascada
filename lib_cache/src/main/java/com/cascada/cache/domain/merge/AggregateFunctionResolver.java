package com.cascada.cache.domain.merge;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Resolves the cross-bucket combine op for a stored measure column from the <em>declared aggregate
 * specs</em> instead of sniffing the column name (README caveat 4).
 *
 * <p>The historical heuristic looked only at the column name ({@code max(...)} / {@code max_...}
 * prefixes). That silently breaks the moment the user aliases the aggregate:
 * {@code MAX(latency) AS peak_latency} stores a column named {@code peak_latency}, the {@code max}
 * signal is gone, and the merge SUMs maxima across buckets — a wrong number served with confidence.
 * Worse, when the verifying oracle uses the same heuristic it <em>confirms</em> the wrong answer.
 *
 * <p>This resolver parses each aggregate spec ({@code FUNC(col)} or {@code FUNC(col) AS alias}) once
 * and maps the resulting output column name — the alias when present, the expression otherwise — to
 * the parsed {@link AggregateFunction}. Name sniffing survives only as the fallback for columns no
 * spec claims (e.g. AVG's decomposed {@code SUM(col)}/{@code COUNT(col)} ingredients, which combine
 * additively and are correctly matched by the {@code sum(}/{@code count(} shape anyway).
 */
public final class AggregateFunctionResolver {

    private static final Pattern SIMPLE_AGGREGATE_SPEC = Pattern.compile(
            "(?i)^\\s*(SUM|COUNT|MIN|MAX)\\s*\\(\\s*([^)]*?)\\s*\\)\\s*(?:AS\\s+([A-Za-z_][A-Za-z0-9_]*)\\s*)?$");

    private AggregateFunctionResolver() {
    }

    /**
     * Builds the output-column → combine-op map from a collection of aggregate specs. Specs that are
     * not a plain aggregate-of-column (composites, holistic aggregates, AVG) contribute nothing —
     * unmapped columns simply fall back to {@link #resolve}'s name heuristic, which is never less
     * correct than the pre-fix behaviour.
     */
    public static Map<String, AggregateFunction> fromAggregateSpecs(Collection<String> aggregateSpecs) {
        Map<String, AggregateFunction> byOutputColumn = new LinkedHashMap<>();
        for (String spec : aggregateSpecs) {
            Matcher matcher = SIMPLE_AGGREGATE_SPEC.matcher(spec);
            if (!matcher.matches()) {
                continue;
            }
            AggregateFunction function = functionForName(matcher.group(1));
            if (function == null) {
                continue;
            }
            String column = matcher.group(2);
            String alias = matcher.group(3);
            String outputColumn = alias != null ? alias
                    : matcher.group(1).toUpperCase(Locale.ROOT) + "(" + column + ")";
            byOutputColumn.put(normalize(outputColumn), function);
        }
        return byOutputColumn;
    }

    /**
     * The combine op for one stored measure column: the parsed spec map first, the historical
     * name-sniffing heuristic only for columns the specs did not claim.
     */
    public static AggregateFunction resolve(String column, Map<String, AggregateFunction> byOutputColumn) {
        AggregateFunction parsed = byOutputColumn.get(normalize(column));
        if (parsed != null) {
            return parsed;
        }
        String lower = column.toLowerCase(Locale.ROOT);
        if (lower.startsWith("min(") || lower.startsWith("min_")) {
            return AggregateFunction.MINIMUM;
        }
        if (lower.startsWith("max(") || lower.startsWith("max_")) {
            return AggregateFunction.MAXIMUM;
        }
        // SUM and COUNT both combine additively across buckets.
        return AggregateFunction.SUM;
    }

    /** Maps a SQL aggregate keyword to its combine op; {@code null} for anything not distributive. */
    public static AggregateFunction functionForName(String name) {
        return switch (name.toUpperCase(Locale.ROOT)) {
            case "SUM" -> AggregateFunction.SUM;
            case "COUNT" -> AggregateFunction.COUNT;
            case "MIN" -> AggregateFunction.MINIMUM;
            case "MAX" -> AggregateFunction.MAXIMUM;
            default -> null;
        };
    }

    /** Case/space/backtick-insensitive column identity, matching the merge path's own resolution. */
    private static String normalize(String column) {
        return column.replace("`", "").replaceAll("\\s+", "").toUpperCase(Locale.ROOT);
    }
}
