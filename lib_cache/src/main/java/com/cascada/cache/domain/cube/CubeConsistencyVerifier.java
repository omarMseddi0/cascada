package com.cascada.cache.domain.cube;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The cube's data-inconsistency guard (plan §8.12, §8.18, {@code CACHE_INCONSISTENCY_EXPLAINED.md}).
 *
 * <p>{@link CubeSubsumptionPlanner#subsumes} decides — by static shape algebra — that a coarser cached
 * frame <em>can</em> answer a finer query, and {@link CubeSubsumptionPlanner#rollUpAndFilterDown}
 * produces that answer. But a true static check can still meet a frame whose <em>contents</em> violate
 * the assumption the algebra relies on (a missing {@code COUNT} ingredient an {@code AVG} needs, a
 * dimension column the subsumption claimed but the frame does not carry, a non-numeric value where a
 * measure is expected, a holistic aggregate that slipped through). Serving such a roll-up would return a
 * silently wrong number — the one failure mode this engine must never have.
 *
 * <p>This verifier is the independent second opinion: from the <em>same candidate frame</em> it
 * re-derives, by a deliberately simple and separate code path (a direct group-and-combine
 * "oracle"), what the answer to the query must be, and compares it cell-by-cell to what the planner
 * produced. It is the in-memory analogue of the cache-correctness gate's {@code DirectComputeOracle}:
 * if the two disagree, the roll-up is rejected and the engine falls through to Spark (a slow correct
 * answer instead of a fast wrong one). It does <strong>not</strong> hit Spark or the source tables — it
 * only proves the roll-up is internally consistent with the bytes the cache already holds.
 *
 * <p>The verifier is intentionally conservative: any condition it cannot itself evaluate (a filter it
 * cannot parse, a column it cannot find, a value it cannot read as a number) is reported as
 * <em>inconsistent</em> rather than waved through, because "I am not sure" must bypass to Spark.
 */
public final class CubeConsistencyVerifier {

    private static final Pattern EQUALITY_FILTER =
            Pattern.compile("\\s*([A-Za-z_][A-Za-z0-9_]*)\\s*=\\s*'?([^']*?)'?\\s*");
    private static final Pattern IN_FILTER =
            Pattern.compile("(?i)\\s*([A-Za-z_][A-Za-z0-9_]*)\\s+IN\\s*\\((.*)\\)\\s*");
    private static final Pattern AGGREGATE_OF_COLUMN =
            Pattern.compile("(?i)\\s*(SUM|COUNT|MIN|MAX|AVG)\\s*\\(\\s*([^)]*?)\\s*\\)\\s*");
    private static final Set<String> HOLISTIC_MARKERS = Set.of("DISTINCT", "MEDIAN", "PERCENTILE");

    /** Floating-point measures are compared to this relative+absolute tolerance, never with {@code ==}. */
    private static final double TOLERANCE = 1e-9;

    /**
     * Verify that {@code plannerAnswer} (the frame {@link CubeSubsumptionPlanner#rollUpAndFilterDown}
     * returned for {@code query} from {@code candidate}) is consistent with an independent direct
     * re-derivation from the same candidate frame.
     *
     * @return a {@link VerificationResult}; {@link VerificationResult#isConsistent()} is {@code true}
     *     only when the roll-up can be trusted and served from cache.
     */
    public VerificationResult verifyRollUp(CachedShapeEntry candidate, QueryShape query,
                                           ResultFrame plannerAnswer) {
        // Guard 0: holistic aggregates must never reach a roll-up. If one did, refuse outright.
        for (String aggregate : query.aggregates()) {
            String upper = aggregate.toUpperCase(Locale.ROOT);
            for (String holistic : HOLISTIC_MARKERS) {
                if (upper.contains(holistic)) {
                    return VerificationResult.inconsistent(
                            "holistic aggregate '" + aggregate + "' cannot be rolled up exactly");
                }
            }
        }

        Oracle oracle;
        try {
            oracle = computeOracle(candidate, query);
        } catch (UnverifiableRollUp unverifiable) {
            return VerificationResult.inconsistent(unverifiable.getMessage());
        }

        // Same number of result groups.
        if (plannerAnswer.rowCount() != oracle.groups.size()) {
            return VerificationResult.inconsistent("row count mismatch: planner=" + plannerAnswer.rowCount()
                    + " oracle=" + oracle.groups.size());
        }

        // Every planner row must match the oracle group with the same dimension key, cell for cell.
        List<String> dimensionColumns = oracle.dimensionColumns;
        List<String> measureColumns = oracle.measureColumns;
        List<AverageColumn> averages = oracle.averages;
        for (Map<String, Object> answerRow : plannerAnswer.rows()) {
            Map<String, String> key = new TreeMap<>();
            for (String dimension : dimensionColumns) {
                if (!answerRow.containsKey(dimension)) {
                    return VerificationResult.inconsistent("planner row missing dimension '" + dimension + "'");
                }
                key.put(dimension, String.valueOf(answerRow.get(dimension)));
            }
            Map<String, Double> expected = oracle.groups.get(key);
            if (expected == null) {
                return VerificationResult.inconsistent("planner produced an unexpected group: " + key);
            }
            for (String measure : measureColumns) {
                double planned = readNumber(answerRow.get(measure), "measure '" + measure + "'");
                double truth = expected.getOrDefault(measure, 0.0);
                if (!closeEnough(planned, truth)) {
                    return VerificationResult.inconsistent("measure '" + measure + "' for group " + key
                            + " disagrees: planner=" + planned + " oracle=" + truth);
                }
            }
            for (AverageColumn average : averages) {
                double planned = readNumber(answerRow.get(average.alias()), "average '" + average.alias() + "'");
                double truthSum = expected.getOrDefault(average.sumColumn(), 0.0);
                double truthCount = expected.getOrDefault(average.countColumn(), 0.0);
                double truth = truthCount == 0.0 ? Double.NaN : truthSum / truthCount;
                if (!closeEnough(planned, truth)) {
                    return VerificationResult.inconsistent("AVG '" + average.alias() + "' for group " + key
                            + " disagrees: planner=" + planned + " oracle=" + truth);
                }
            }
        }
        return VerificationResult.consistent();
    }

    // --- the independent oracle ----------------------------------------------------------------------

    private Oracle computeOracle(CachedShapeEntry candidate, QueryShape query) {
        ResultFrame frame = candidate.frame();

        List<String> dimensionColumns = frame.columnNames().stream()
                .filter(column -> query.groupBy().contains(column))
                .toList();
        // Every grouped column the query asked for must physically exist in the candidate frame.
        for (String grouped : query.groupBy()) {
            if (!frame.columnNames().contains(grouped)) {
                throw new UnverifiableRollUp("query group-by column '" + grouped
                        + "' is absent from the cached frame");
            }
        }
        // A rolled-away dimension (grouped by the candidate but not the query) is NOT a measure,
        // even when it is numeric — summing a device id or an hour bucket would fabricate a column.
        List<String> measureColumns = frame.columnNames().stream()
                .filter(column -> frame.columnType(column) != ColumnType.STRING)
                .filter(column -> !query.groupBy().contains(column))
                .filter(column -> !candidate.shape().groupBy().contains(column))
                .toList();
        List<AverageColumn> averages = requestedAverages(query, measureColumns);

        Set<String> extraFilters = new java.util.HashSet<>(query.filters());
        extraFilters.removeAll(candidate.shape().filters());

        Map<Map<String, String>, Map<String, Double>> groups = new LinkedHashMap<>();
        for (Map<String, Object> row : frame.rows()) {
            if (!allFiltersMatch(row, extraFilters)) {
                continue;
            }
            Map<String, String> key = new TreeMap<>();
            for (String dimension : dimensionColumns) {
                key.put(dimension, String.valueOf(row.get(dimension)));
            }
            Map<String, Double> combined = groups.computeIfAbsent(key, ignored -> new TreeMap<>());
            for (String measure : measureColumns) {
                double value = readNumber(row.get(measure), "measure '" + measure + "'");
                combined.merge(measure, value, combinerFor(measure));
            }
        }
        return new Oracle(dimensionColumns, measureColumns, averages, groups);
    }

    private java.util.function.BiFunction<Double, Double, Double> combinerFor(String measure) {
        String lower = measure.toLowerCase(Locale.ROOT);
        if (lower.startsWith("min(") || lower.startsWith("min_")) {
            return Math::min;
        }
        if (lower.startsWith("max(") || lower.startsWith("max_")) {
            return Math::max;
        }
        return Double::sum; // SUM and COUNT both combine additively (RC4: never average averages).
    }

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
            if (sumColumn == null || countColumn == null) {
                throw new UnverifiableRollUp("AVG(" + column + ") needs stored SUM and COUNT ingredients");
            }
            averages.add(new AverageColumn(aggregate.trim(), sumColumn, countColumn));
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

    private boolean allFiltersMatch(Map<String, Object> row, Set<String> filters) {
        for (String filter : filters) {
            if (!rowMatchesFilter(row, filter)) {
                return false;
            }
        }
        return true;
    }

    private boolean rowMatchesFilter(Map<String, Object> row, String filter) {
        Matcher inMatcher = IN_FILTER.matcher(filter);
        if (inMatcher.matches()) {
            String column = inMatcher.group(1);
            Set<String> allowed = parseInList(inMatcher.group(2));
            if (!row.containsKey(column)) {
                throw new UnverifiableRollUp("IN filter on absent column '" + column + "'");
            }
            return allowed.stream().anyMatch(literal -> valueEqualsLiteral(row.get(column), literal));
        }
        Matcher equalityMatcher = EQUALITY_FILTER.matcher(filter);
        if (!equalityMatcher.matches()) {
            throw new UnverifiableRollUp("filter '" + filter + "' cannot be evaluated in memory");
        }
        String column = equalityMatcher.group(1);
        if (!row.containsKey(column)) {
            throw new UnverifiableRollUp("equality filter on absent column '" + column + "'");
        }
        return valueEqualsLiteral(row.get(column), equalityMatcher.group(2));
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

    private double readNumber(Object value, String what) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        throw new UnverifiableRollUp(what + " is not numeric: " + value);
    }

    private boolean closeEnough(double left, double right) {
        if (Double.isNaN(left) && Double.isNaN(right)) {
            return true; // NaN is the shared "no data" marker for a zero-count AVG.
        }
        if (Double.isNaN(left) != Double.isNaN(right)) {
            return false;
        }
        double diff = Math.abs(left - right);
        return diff <= TOLERANCE || diff <= TOLERANCE * Math.max(Math.abs(left), Math.abs(right));
    }

    private String normalize(String aggregate) {
        return aggregate.replaceAll("\\s+", "").toUpperCase(Locale.ROOT);
    }

    /** The independently re-derived ground truth for a query against one candidate frame. */
    private record Oracle(List<String> dimensionColumns, List<String> measureColumns,
                          List<AverageColumn> averages,
                          Map<Map<String, String>, Map<String, Double>> groups) {
    }

    /** A requested AVG and the SUM/COUNT columns it reconstructs from. */
    private record AverageColumn(String alias, String sumColumn, String countColumn) {
    }

    /** Raised internally when the candidate frame's contents make the roll-up impossible to verify. */
    private static final class UnverifiableRollUp extends RuntimeException {
        UnverifiableRollUp(String message) {
            super(message);
        }
    }

    /** The verdict: consistent (serve from cache) or inconsistent (bypass to Spark) with a reason. */
    public static final class VerificationResult {
        private final boolean consistent;
        private final String reason;

        private VerificationResult(boolean consistent, String reason) {
            this.consistent = consistent;
            this.reason = reason;
        }

        static VerificationResult consistent() {
            return new VerificationResult(true, "");
        }

        static VerificationResult inconsistent(String reason) {
            return new VerificationResult(false, reason);
        }

        public boolean isConsistent() {
            return consistent;
        }

        /** Empty when consistent; otherwise a human-readable explanation for the bypass decision. */
        public String reason() {
            return reason;
        }
    }
}
