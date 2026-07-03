package com.cascada.cache.domain.cube;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.merge.AggregateFunction;
import com.cascada.cache.domain.merge.AggregateFunctionResolver;
import com.cascada.cache.domain.merge.AggregationRow;
import com.cascada.cache.domain.merge.AverageReconstructionService;
import com.cascada.cache.domain.merge.GlobalAggregateMerger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * On a cache miss, before paying for Spark, asks whether a <em>coarser</em> already-cached shape can
 * answer the <em>finer</em> incoming query purely in memory (ARCHITECTURE §4, plan §8.12, formal rules
 * in Appendix I). Because buckets store mergeable ingredients, a cached
 * {@code GROUP BY appName, deviceType} can answer {@code GROUP BY appName} by roll-up, an unfiltered
 * shape can answer a filtered query by filter-down, and a stored {@code SUM}+{@code COUNT} can answer
 * an {@code AVG} by reconstruction — all without Spark.
 *
 * <p>This is the full Appendix-I implementation:
 * <ul>
 *   <li><b>I.2/1 group-by superset</b> — {@link #isGroupBySuperset};</li>
 *   <li><b>I.2/2 aggregate derivability</b> — {@link #areAggregatesCompatible} rejects holistic
 *       aggregates, and {@link #areAggregatesDerivable} additionally requires every requested
 *       aggregate to be reconstructible from the candidate's stored ingredients ({@code AVG} needs
 *       {@code SUM}+{@code COUNT});</li>
 *   <li><b>I.2/3 filter narrowing</b> — {@link #isFilterNarrowable} (candidate filters ⊆ query
 *       filters) plus {@link #extraFiltersAreOnGroupedColumns} (each added predicate is on a grouped
 *       column, so it can be applied in memory), supporting equality and {@code IN};</li>
 *   <li><b>I.4 best subsumer</b> — {@link #findBestSubsumingCacheEntryForQuery} picks the cheapest:
 *       the smallest group-by superset, then the tightest (most-filtered) candidate.</li>
 * </ul>
 *
 * <p>The guardrail: roll-up is valid only for distributive/algebraic aggregates ({@code SUM},
 * {@code COUNT}, {@code MIN}, {@code MAX}, and {@code AVG} via reconstruction); holistic aggregates
 * (exact {@code COUNT(DISTINCT)}, {@code MEDIAN}, {@code PERCENTILE}) are rejected so a wrong number is
 * never returned.
 */
public final class CubeSubsumptionPlanner {

    private static final Pattern EQUALITY_FILTER =
            Pattern.compile("\\s*([A-Za-z_][A-Za-z0-9_]*)\\s*=\\s*'?([^']*?)'?\\s*");
    private static final Pattern IN_FILTER =
            Pattern.compile("(?i)\\s*([A-Za-z_][A-Za-z0-9_]*)\\s+IN\\s*\\((.*)\\)\\s*");
    private static final Pattern AGGREGATE_OF_COLUMN =
            Pattern.compile("(?i)\\s*(SUM|COUNT|MIN|MAX|AVG)\\s*\\(\\s*([^)]*?)\\s*\\)\\s*");
    private static final Set<String> HOLISTIC_MARKERS = Set.of("DISTINCT", "MEDIAN", "PERCENTILE");

    private final GlobalAggregateMerger globalAggregateMerger = new GlobalAggregateMerger();
    private final AverageReconstructionService averageReconstructionService = new AverageReconstructionService();

    /** The first catalogued entry that can answer the query by roll-up/filter-down, if any. */
    public Optional<CachedShapeEntry> findSubsumingCacheEntryForQuery(QueryShape query,
                                                                      List<CachedShapeEntry> candidates) {
        for (CachedShapeEntry candidate : candidates) {
            if (subsumes(candidate.shape(), query)) {
                return Optional.of(candidate);
            }
        }
        return Optional.empty();
    }

    /**
     * Appendix I.4 — when several entries subsume the query, pick the cheapest to roll up: the smallest
     * group-by superset (fewest rows to scan), then the tightest already-applied filters.
     */
    public Optional<CachedShapeEntry> findBestSubsumingCacheEntryForQuery(QueryShape query,
                                                                          List<CachedShapeEntry> candidates) {
        return candidates.stream()
                .filter(candidate -> subsumes(candidate.shape(), query))
                .min(Comparator
                        .<CachedShapeEntry>comparingInt(candidate -> candidate.shape().groupBy().size())
                        .thenComparing(candidate -> -candidate.shape().filters().size()));
    }

    public boolean subsumes(QueryShape candidate, QueryShape query) {
        return isGroupBySuperset(candidate, query)
                && areAggregatesCompatible(query)
                && areAggregatesDerivable(candidate, query)
                && isFilterNarrowable(candidate, query)
                && extraFiltersAreOnGroupedColumns(candidate, query);
    }

    /** Candidate must group by at least everything the query groups by (so it can roll up). */
    public boolean isGroupBySuperset(QueryShape candidate, QueryShape query) {
        return candidate.groupBy().containsAll(query.groupBy());
    }

    /** Reject holistic aggregates that cannot be rolled up exactly. */
    public boolean areAggregatesCompatible(QueryShape query) {
        for (String aggregate : query.aggregates()) {
            String upper = aggregate.toUpperCase(Locale.ROOT);
            for (String holistic : HOLISTIC_MARKERS) {
                if (upper.contains(holistic)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Appendix I.2 rule 2: every requested aggregate must be reconstructible from what the candidate
     * stored — {@code SUM}/{@code COUNT}/{@code MIN}/{@code MAX} directly, {@code AVG(col)} from a
     * stored {@code SUM(col)} and {@code COUNT(col)}.
     */
    public boolean areAggregatesDerivable(QueryShape candidate, QueryShape query) {
        Set<String> candidateNormalized = new java.util.HashSet<>();
        for (String aggregate : candidate.aggregates()) {
            candidateNormalized.add(normalize(aggregate));
        }
        for (String aggregate : query.aggregates()) {
            Matcher matcher = AGGREGATE_OF_COLUMN.matcher(aggregate);
            if (!matcher.matches()) {
                // not a simple aggregate-of-column; require it to be stored verbatim
                if (!candidateNormalized.contains(normalize(aggregate))) {
                    return false;
                }
                continue;
            }
            String function = matcher.group(1).toUpperCase(Locale.ROOT);
            String column = matcher.group(2);
            if (function.equals("AVG")) {
                if (!candidateNormalized.contains(normalize("SUM(" + column + ")"))
                        || !candidateNormalized.contains(normalize("COUNT(" + column + ")"))) {
                    return false;
                }
            } else if (!candidateNormalized.contains(normalize(aggregate))) {
                return false;
            }
        }
        return true;
    }

    /** Candidate's filters must be a subset of the query's, so the query only ever <em>narrows</em>. */
    public boolean isFilterNarrowable(QueryShape candidate, QueryShape query) {
        return query.filters().containsAll(candidate.filters());
    }

    /**
     * Appendix I.2 rule 3: every predicate the query adds beyond the candidate must be on a column the
     * candidate grouped by — only then can it be evaluated against the cached frame in memory.
     */
    public boolean extraFiltersAreOnGroupedColumns(QueryShape candidate, QueryShape query) {
        Set<String> extra = new java.util.HashSet<>(query.filters());
        extra.removeAll(candidate.filters());
        for (String filter : extra) {
            Optional<String> column = filterColumn(filter);
            if (column.isEmpty() || !candidate.groupBy().contains(column.get())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Answer the query from the candidate frame: apply the query's extra equality/IN filters, roll up
     * to the query's group-by by summing distributive ingredients (MIN/MAX combine by extremum), then
     * reconstruct any requested {@code AVG(col)} from the rolled-up {@code SUM(col)}/{@code COUNT(col)}.
     */
    public ResultFrame rollUpAndFilterDown(CachedShapeEntry candidate, QueryShape query) {
        Set<String> extraFilters = new java.util.HashSet<>(query.filters());
        extraFilters.removeAll(candidate.shape().filters());

        List<Map<String, Object>> filteredRows = applyNarrowingFilters(candidate.frame(), extraFilters);

        List<String> dimensionColumns = candidate.frame().columnNames().stream()
                .filter(column -> query.groupBy().contains(column))
                .toList();
        // A rolled-away dimension (grouped by the candidate but not the query) is NOT a measure,
        // even when it is numeric — summing a device id or an hour bucket would fabricate a column.
        List<String> measureColumns = candidate.frame().columnNames().stream()
                .filter(column -> candidate.frame().columnType(column) != ColumnType.STRING)
                .filter(column -> !query.groupBy().contains(column))
                .filter(column -> !candidate.shape().groupBy().contains(column))
                .toList();

        List<AggregationRow> rows = new ArrayList<>();
        for (Map<String, Object> row : filteredRows) {
            Map<String, String> dimensions = new LinkedHashMap<>();
            dimensionColumns.forEach(dimension -> dimensions.put(dimension, String.valueOf(row.get(dimension))));
            Map<String, Double> measures = new LinkedHashMap<>();
            measureColumns.forEach(measure -> measures.put(measure, ((Number) row.get(measure)).doubleValue()));
            rows.add(new AggregationRow(dimensions, measures));
        }

        // README caveat 4: combine ops come from the DECLARED aggregate specs (candidate shape first,
        // then the query's), never from the column name alone — an aliased MAX(latency) AS peak_latency
        // would otherwise be SUMmed across the rolled-up rows.
        Map<String, AggregateFunction> declaredAggregates =
                new LinkedHashMap<>(AggregateFunctionResolver.fromAggregateSpecs(query.aggregates()));
        declaredAggregates.putAll(AggregateFunctionResolver.fromAggregateSpecs(candidate.shape().aggregates()));
        Map<String, AggregateFunction> aggregations = new LinkedHashMap<>();
        measureColumns.forEach(measure ->
                aggregations.put(measure, AggregateFunctionResolver.resolve(measure, declaredAggregates)));
        List<AggregationRow> rolledUp = globalAggregateMerger.merge(rows, aggregations, false);

        List<AverageColumn> averages = requestedAverages(query, measureColumns);

        ResultFrame.Builder builder = ResultFrame.builder();
        dimensionColumns.forEach(dimension -> builder.column(dimension, ColumnType.STRING));
        measureColumns.forEach(measure -> builder.column(measure, ColumnType.DOUBLE));
        averages.forEach(average -> builder.column(average.alias(), ColumnType.DOUBLE));
        for (AggregationRow row : rolledUp) {
            Map<String, Object> values = new LinkedHashMap<>(row.dimensions());
            values.putAll(row.measures());
            for (AverageColumn average : averages) {
                // Math.round, not a bare cast: counts ride in double measures, and a sum of
                // doubles can land at 41.999999999999996 — truncation would divide by 41.
                double reconstructed = averageReconstructionService.reconstructAverageFromStoredSumAndCount(
                        row.measure(average.sumColumn()), Math.round(row.measure(average.countColumn())));
                values.put(average.alias(), reconstructed);
            }
            builder.row(values);
        }
        return builder.build();
    }

    // --- helpers ---------------------------------------------------------------------------------

    private List<AverageColumn> requestedAverages(QueryShape query, List<String> measureColumns) {
        Set<String> measureNormalized = new java.util.HashSet<>();
        for (String measure : measureColumns) {
            measureNormalized.add(normalize(measure));
        }
        List<AverageColumn> averages = new ArrayList<>();
        for (String aggregate : query.aggregates()) {
            Matcher matcher = AGGREGATE_OF_COLUMN.matcher(aggregate);
            if (!matcher.matches() || !matcher.group(1).equalsIgnoreCase("AVG")) {
                continue;
            }
            String column = matcher.group(2);
            String sumColumn = resolveMeasureColumn(measureColumns, measureNormalized, "SUM(" + column + ")");
            String countColumn = resolveMeasureColumn(measureColumns, measureNormalized, "COUNT(" + column + ")");
            if (sumColumn != null && countColumn != null) {
                averages.add(new AverageColumn(aggregate.trim(), sumColumn, countColumn));
            }
        }
        return averages;
    }

    private String resolveMeasureColumn(List<String> measureColumns, Set<String> measureNormalized, String target) {
        String normalizedTarget = normalize(target);
        if (!measureNormalized.contains(normalizedTarget)) {
            return null;
        }
        for (String measure : measureColumns) {
            if (normalize(measure).equals(normalizedTarget)) {
                return measure;
            }
        }
        return null;
    }

    private List<Map<String, Object>> applyNarrowingFilters(ResultFrame frame, Set<String> filters) {
        List<Map<String, Object>> kept = new ArrayList<>();
        for (Map<String, Object> row : frame.rows()) {
            if (filters.stream().allMatch(filter -> rowMatchesFilter(row, filter))) {
                kept.add(row);
            }
        }
        return kept;
    }

    private boolean rowMatchesFilter(Map<String, Object> row, String filter) {
        Matcher inMatcher = IN_FILTER.matcher(filter);
        if (inMatcher.matches()) {
            String column = inMatcher.group(1);
            Set<String> allowed = parseInList(inMatcher.group(2));
            return row.containsKey(column)
                    && allowed.stream().anyMatch(literal -> valueEqualsLiteral(row.get(column), literal));
        }
        Matcher equalityMatcher = EQUALITY_FILTER.matcher(filter);
        if (!equalityMatcher.matches()) {
            return false; // a filter we cannot evaluate in memory means this row cannot be claimed
        }
        String column = equalityMatcher.group(1);
        String expected = equalityMatcher.group(2);
        return row.containsKey(column) && valueEqualsLiteral(row.get(column), expected);
    }

    /**
     * SQL-faithful IN-list split: commas inside single-quoted strings separate nothing, and a doubled
     * {@code ''} inside quotes is the SQL escape for one literal quote. A naive {@code split(",")}
     * would turn {@code IN ('a,b', 'c')} into the wrong member set and filter-down wrong rows.
     */
    private Set<String> parseInList(String rawList) {
        Set<String> values = new java.util.HashSet<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        boolean wasQuoted = false;
        for (int index = 0; index < rawList.length(); index++) {
            char character = rawList.charAt(index);
            if (character == '\'') {
                if (inQuotes && index + 1 < rawList.length() && rawList.charAt(index + 1) == '\'') {
                    current.append('\'');
                    index++;
                    continue;
                }
                inQuotes = !inQuotes;
                wasQuoted = true;
                continue;
            }
            if (!inQuotes && character == ',') {
                addInListMember(values, current, wasQuoted);
                current.setLength(0);
                wasQuoted = false;
                continue;
            }
            if (!inQuotes && Character.isWhitespace(character)) {
                continue; // padding around members; quoted members keep their internal spaces
            }
            current.append(character);
        }
        addInListMember(values, current, wasQuoted);
        return values;
    }

    private void addInListMember(Set<String> values, StringBuilder token, boolean wasQuoted) {
        String member = token.toString();
        if (wasQuoted || !member.isEmpty()) {
            values.add(member);
        }
    }

    /**
     * A frame cell vs a filter literal: exact string match, or — when the cell is numeric — numeric
     * equality, so {@code device_id = 5} still matches a cell stored as {@code 5.0}.
     */
    private boolean valueEqualsLiteral(Object cellValue, String literal) {
        if (String.valueOf(cellValue).equals(literal)) {
            return true;
        }
        if (cellValue instanceof Number number) {
            try {
                return number.doubleValue() == Double.parseDouble(literal.trim());
            } catch (NumberFormatException notANumber) {
                return false;
            }
        }
        return false;
    }

    private Optional<String> filterColumn(String filter) {
        Matcher inMatcher = IN_FILTER.matcher(filter);
        if (inMatcher.matches()) {
            return Optional.of(inMatcher.group(1));
        }
        Matcher equalityMatcher = EQUALITY_FILTER.matcher(filter);
        return equalityMatcher.matches() ? Optional.of(equalityMatcher.group(1)) : Optional.empty();
    }

    private String normalize(String aggregate) {
        return aggregate.replaceAll("\\s+", "").toUpperCase(Locale.ROOT);
    }

    /** A requested AVG and the rolled-up SUM/COUNT columns it is reconstructed from. */
    private record AverageColumn(String alias, String sumColumn, String countColumn) {
    }
}
