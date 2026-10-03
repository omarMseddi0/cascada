package com.cascada.cache.domain.cube;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.merge.AggregateFunction;
import com.cascada.cache.domain.merge.AggregateFunctionResolver;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Independently checks a proposed cube roll-up against a typed, direct regrouping of its input. */
public final class CubeConsistencyVerifier {

    private static final Pattern EQUALITY_FILTER =
            Pattern.compile("\\s*([A-Za-z_][A-Za-z0-9_]*)\\s*=\\s*(.*?)\\s*");
    private static final Pattern IN_FILTER =
            Pattern.compile("(?i)\\s*([A-Za-z_][A-Za-z0-9_]*)\\s+IN\\s*\\((.*)\\)\\s*");
    private static final Pattern AGGREGATE_OF_COLUMN =
            Pattern.compile("(?i)\\s*(SUM|COUNT|MIN|MAX|AVG)\\s*\\(\\s*([^)]*?)\\s*\\)\\s*");
    private static final Pattern PROJECTION_ALIAS = Pattern.compile(
            "(?is)^.*?\\s+(?:AS\\s+)?([A-Za-z_][A-Za-z0-9_]*)\\s*$");
    private static final Set<String> HOLISTIC_MARKERS = Set.of("DISTINCT", "MEDIAN", "PERCENTILE");
    private static final double TOLERANCE = 1e-9;

    /** Any exception while checking a candidate is a cache miss, never an exception to the query. */
    public VerificationResult verifyRollUp(CachedShapeEntry candidate, QueryShape query,
                                           ResultFrame plannerAnswer) {
        try {
            return verifyRollUpUnchecked(candidate, query, plannerAnswer);
        } catch (RuntimeException unverifiable) {
            String message = unverifiable.getMessage();
            return VerificationResult.inconsistent(message == null
                    ? "candidate roll-up could not be verified" : message);
        }
    }

    private VerificationResult verifyRollUpUnchecked(CachedShapeEntry candidate, QueryShape query,
                                                      ResultFrame plannerAnswer) {
        for (String aggregate : query.aggregates()) {
            String upper = aggregate.toUpperCase(Locale.ROOT);
            if (HOLISTIC_MARKERS.stream().anyMatch(upper::contains)) {
                return VerificationResult.inconsistent(
                        "holistic aggregate '" + aggregate + "' cannot be rolled up exactly");
            }
        }

        ResultFrame frame = candidate.frame();
        List<String> dimensions = frame.columnNames().stream().filter(query.groupBy()::contains).toList();
        for (String grouped : query.groupBy()) {
            if (!frame.columnNames().contains(grouped)) {
                return VerificationResult.inconsistent("query group-by column '" + grouped
                        + "' is absent from the cached frame");
            }
        }
        List<String> measures = measureColumns(candidate);
        List<CubeAverageColumn> averages = averages(query, measures, frame);
        if (!query.projectionSignature().isEmpty() && !averages.isEmpty()) {
            return VerificationResult.inconsistent("AVG projection cannot be proven from this cached schema");
        }
        CubeExpectedSchema expectedSchema = expectedSchema(candidate, query, dimensions, measures, averages);
        if (!plannerAnswer.columnNames().equals(expectedSchema.names())
                || !plannerAnswer.columnTypes().equals(expectedSchema.types())) {
            return VerificationResult.inconsistent("planner output schema or column order differs from the query");
        }

        Set<String> extraFilters = new java.util.HashSet<>(query.filters());
        extraFilters.removeAll(candidate.shape().filters());
        Map<String, AggregateFunction> declared = declaredAggregateFunctions(candidate.shape(), query);
        validateMeasureCombiners(measures, declared);

        Map<List<Object>, CubeVerifierGroup> oracle = new LinkedHashMap<>();
        for (Map<String, Object> row : frame.rows()) {
            if (!matchesAll(frame, row, extraFilters)) {
                continue;
            }
            List<Object> parts = new ArrayList<>(dimensions.size());
            for (String dimension : dimensions) {
                parts.add(canonicalDimension(frame.columnType(dimension), row.get(dimension)));
            }
            List<Object> key = Collections.unmodifiableList(parts);
            CubeVerifierGroup group = oracle.computeIfAbsent(key, ignored -> new CubeVerifierGroup(measures.size()));
            for (int index = 0; index < measures.size(); index++) {
                String measure = measures.get(index);
                Object value = row.get(measure);
                if (value == null) {
                    continue;
                }
                ColumnType type = frame.columnType(measure);
                validateDecimal(type, value);
                if (!group.measurePresent()[index]) {
                    group.measureValues()[index] = value;
                    group.measurePresent()[index] = true;
                } else {
                    AggregateFunction function = declared.get(normalize(measure));
                    if (function == null) {
                        function = AggregateFunctionResolver.resolve(measure, declared);
                    }
                    group.measureValues()[index] = combineIndependently(group.measureValues()[index], value, type, function);
                }
            }
        }

        if (plannerAnswer.rowCount() != oracle.size()) {
            return VerificationResult.inconsistent("row count mismatch: planner=" + plannerAnswer.rowCount()
                    + " oracle=" + oracle.size());
        }
        java.util.HashSet<List<Object>> seenGroups = new java.util.HashSet<>();
        for (Map<String, Object> row : plannerAnswer.rows()) {
            List<Object> parts = new ArrayList<>(dimensions.size());
            for (String dimension : dimensions) {
                if (!row.containsKey(dimension)) {
                    return VerificationResult.inconsistent("planner row missing dimension '" + dimension + "'");
                }
                parts.add(canonicalDimension(frame.columnType(dimension), row.get(dimension)));
            }
            List<Object> key = Collections.unmodifiableList(parts);
            if (!seenGroups.add(key)) {
                return VerificationResult.inconsistent("planner emitted a duplicate group");
            }
            CubeVerifierGroup truth = oracle.get(key);
            if (truth == null) {
                return VerificationResult.inconsistent("planner produced an unexpected group");
            }
            for (int index = 0; index < measures.size(); index++) {
                String measure = measures.get(index);
                if (!expectedSchema.names().contains(measure)) {
                    continue;
                }
                Object expected = truth.measurePresent()[index] ? truth.measureValues()[index] : null;
                if (!valueEquals(row.get(measure), expected, frame.columnType(measure))) {
                    return VerificationResult.inconsistent("measure '" + measure + "' disagrees with direct roll-up");
                }
            }
            for (CubeAverageColumn average : averages) {
                Double expected = averageValue(rowForOracle(truth, measures), average);
                Object actual = row.get(average.alias());
                boolean matchesAverage = expected == null
                        ? actual == null
                        : actual instanceof Number number && closeEnough(number.doubleValue(), expected);
                if (!matchesAverage) {
                    return VerificationResult.inconsistent("AVG '" + average.alias()
                            + "' disagrees with direct roll-up");
                }
            }
        }
        return VerificationResult.consistent();
    }

    private CubeExpectedSchema expectedSchema(CachedShapeEntry candidate, QueryShape query,
                                  List<String> dimensions, List<String> measures,
                                  List<CubeAverageColumn> averages) {
        ResultFrame frame = candidate.frame();
        List<String> names = new ArrayList<>();
        Map<String, ColumnType> types = new LinkedHashMap<>();
        if (candidate.shape().projectionSignature().isEmpty() != query.projectionSignature().isEmpty()) {
            throw new CubeRollUpUnavailableException("one side has no projection signature");
        }
        if (!query.projectionSignature().isEmpty()) {
            if (!candidate.shape().projectionSignature().equals(query.projectionSignature())) {
                throw new CubeRollUpUnavailableException("ordered projection signature differs");
            }
            if (frame.columnNames().size() != query.projectionSignature().size()) {
                throw new CubeRollUpUnavailableException("projection and cached result schema have different widths");
            }
            for (int index = 0; index < frame.columnNames().size(); index++) {
                String column = frame.columnNames().get(index);
                if (!projectionMatchesColumn(query.projectionSignature().get(index), column)) {
                    throw new CubeRollUpUnavailableException("projection order does not match the cached result schema");
                }
                if (candidate.shape().groupBy().contains(column) && !query.groupBy().contains(column)) {
                    throw new CubeRollUpUnavailableException("a rolled-away group column remains in the projection");
                }
                if (!dimensions.contains(column) && !measures.contains(column)) {
                    throw new CubeRollUpUnavailableException("projection column is unavailable after roll-up");
                }
                names.add(column);
                types.put(column, frame.columnType(column));
            }
            return new CubeExpectedSchema(List.copyOf(names), Map.copyOf(types));
        }
        for (String dimension : dimensions) {
            names.add(dimension);
            types.put(dimension, frame.columnType(dimension));
        }
        for (String measure : requestedMeasureColumns(query, measures, averages)) {
            names.add(measure);
            types.put(measure, frame.columnType(measure));
        }
        for (CubeAverageColumn average : averages) {
            names.add(average.alias());
            types.put(average.alias(), ColumnType.DOUBLE);
        }
        return new CubeExpectedSchema(List.copyOf(names), Map.copyOf(types));
    }

    private List<String> requestedMeasureColumns(QueryShape query, List<String> measures,
                                                 List<CubeAverageColumn> averages) {
        Set<String> requestedNames = new java.util.HashSet<>();
        Set<AggregateFunction> requestedFunctions = new java.util.HashSet<>();
        for (String aggregate : query.aggregates()) {
            Matcher matcher = AGGREGATE_OF_COLUMN.matcher(aggregate);
            if (matcher.matches() && matcher.group(1).equalsIgnoreCase("AVG")) {
                continue;
            }
            Map<String, AggregateFunction> parsed =
                    AggregateFunctionResolver.fromAggregateSpecs(List.of(aggregate));
            if (parsed.isEmpty()) {
                if (findMeasure(measures, aggregate) != null) {
                    requestedNames.add(normalize(aggregate));
                    continue;
                }
                throw new CubeRollUpUnavailableException(
                        "aggregate output '" + aggregate + "' cannot be identified safely");
            }
            AggregateFunction function = parsed.values().iterator().next();
            requestedFunctions.add(function);
            boolean hasDeclaredAlias = false;
            for (Map.Entry<String, AggregateFunction> output : query.outputAggregates().entrySet()) {
                if (output.getValue() == function) {
                    requestedNames.add(normalize(output.getKey()));
                    hasDeclaredAlias = true;
                }
            }
            if (!hasDeclaredAlias) {
                requestedNames.addAll(parsed.keySet());
            }
        }
        for (Map.Entry<String, AggregateFunction> output : query.outputAggregates().entrySet()) {
            if (requestedFunctions.contains(output.getValue())) {
                requestedNames.add(normalize(output.getKey()));
            }
        }
        for (CubeAverageColumn average : averages) {
            requestedNames.add(normalize(average.alias()));
        }

        List<String> requestedColumns = new ArrayList<>();
        for (String measure : measures) {
            if (requestedNames.remove(normalize(measure))) {
                requestedColumns.add(measure);
            }
        }
        for (CubeAverageColumn average : averages) {
            requestedNames.remove(normalize(average.alias()));
        }
        if (!requestedNames.isEmpty()) {
            throw new CubeRollUpUnavailableException(
                    "requested aggregate output is absent from the cached frame: " + requestedNames);
        }
        return requestedColumns;
    }

    private List<String> measureColumns(CachedShapeEntry candidate) {
        List<String> measures = new ArrayList<>();
        for (String column : candidate.frame().columnNames()) {
            if (candidate.shape().groupBy().contains(column)) {
                continue;
            }
            if (candidate.frame().columnType(column) == ColumnType.STRING) {
                throw new CubeRollUpUnavailableException("string aggregate output '" + column + "' is unsupported");
            }
            measures.add(column);
        }
        return measures;
    }

    private void validateMeasureCombiners(List<String> measures,
                                          Map<String, AggregateFunction> declared) {
        for (String measure : measures) {
            if (declared.containsKey(normalize(measure))) {
                continue;
            }
            Matcher rawAggregate = AGGREGATE_OF_COLUMN.matcher(measure);
            if (!rawAggregate.matches() || rawAggregate.group(1).equalsIgnoreCase("AVG")) {
                throw new CubeRollUpUnavailableException("measure '" + measure
                        + "' has no declared mergeable aggregate function");
            }
        }
    }

    private Map<String, AggregateFunction> declaredAggregateFunctions(QueryShape candidate, QueryShape query) {
        Map<String, AggregateFunction> declared = new LinkedHashMap<>();
        addAggregateSpecs(declared, candidate.aggregates());
        addAggregateSpecs(declared, query.aggregates());
        addAggregateMappings(declared, candidate.outputAggregates());
        addAggregateMappings(declared, query.outputAggregates());
        return declared;
    }

    private void addAggregateSpecs(Map<String, AggregateFunction> declared, Set<String> specifications) {
        for (String specification : specifications) {
            Map<String, AggregateFunction> parsed =
                    AggregateFunctionResolver.fromAggregateSpecs(List.of(specification));
            addAggregateMappings(declared, parsed);
        }
    }

    private void addAggregateMappings(Map<String, AggregateFunction> declared,
                                      Map<String, AggregateFunction> additions) {
        for (Map.Entry<String, AggregateFunction> addition : additions.entrySet()) {
            String column = normalize(addition.getKey());
            AggregateFunction existing = declared.putIfAbsent(column, addition.getValue());
            if (existing != null && existing != addition.getValue()) {
                throw new CubeRollUpUnavailableException(
                        "conflicting aggregate functions are declared for output column '" + column + "'");
            }
        }
    }

    private List<CubeAverageColumn> averages(QueryShape query, List<String> measures, ResultFrame frame) {
        List<CubeAverageColumn> result = new ArrayList<>();
        for (String aggregate : query.aggregates()) {
            Matcher matcher = AGGREGATE_OF_COLUMN.matcher(aggregate);
            if (!matcher.matches() || !matcher.group(1).equalsIgnoreCase("AVG")) {
                continue;
            }
            String column = matcher.group(2);
            String sum = findMeasure(measures, "SUM(" + column + ")");
            String count = findMeasure(measures, "COUNT(" + column + ")");
            if (sum == null || count == null) {
                throw new CubeRollUpUnavailableException("AVG(" + column + ") needs stored SUM and COUNT ingredients");
            }
            if (frame.columnType(sum) != ColumnType.DOUBLE || frame.columnType(count) != ColumnType.DOUBLE) {
                throw new CubeRollUpUnavailableException("AVG ingredients are not DOUBLE columns");
            }
            result.add(new CubeAverageColumn(aggregate.trim(), sum, count));
        }
        return result;
    }

    private String findMeasure(List<String> measures, String target) {
        return measures.stream().filter(measure -> normalize(measure).equals(normalize(target)))
                .findFirst().orElse(null);
    }

    private Map<String, Object> rowForOracle(CubeVerifierGroup group, List<String> measures) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int index = 0; index < measures.size(); index++) {
            row.put(measures.get(index), group.measurePresent()[index] ? group.measureValues()[index] : null);
        }
        return row;
    }

    private Double averageValue(Map<String, Object> measures, CubeAverageColumn average) {
        Object sum = measures.get(average.sumColumn());
        Object count = measures.get(average.countColumn());
        if (sum == null || count == null) {
            return null;
        }
        long exactCount = exactAverageCount(count);
        if (exactCount == 0) {
            return null;
        }
        return ((Number) sum).doubleValue() / exactCount;
    }

    private long exactAverageCount(Object count) {
        double value = ((Number) count).doubleValue();
        if (!Double.isFinite(value) || value < 0.0d || value > 9_007_199_254_740_992.0d
                || value != Math.rint(value)) {
            throw new CubeRollUpUnavailableException("AVG count ingredient is not an exactly representable count");
        }
        return (long) value;
    }

    private boolean matchesAll(ResultFrame frame, Map<String, Object> row, Set<String> filters) {
        for (String filter : filters) {
            if (!matchesFilter(frame, row, filter)) {
                return false;
            }
        }
        return true;
    }

    private boolean matchesFilter(ResultFrame frame, Map<String, Object> row, String filter) {
        Matcher in = IN_FILTER.matcher(filter);
        if (in.matches()) {
            String column = in.group(1);
            if (!row.containsKey(column)) {
                throw new CubeRollUpUnavailableException("IN filter on absent column '" + column + "'");
            }
            for (CubeFilterLiteral literal : parseInList(in.group(2))) {
                if (matchesLiteral(row.get(column), frame.columnType(column), literal)) {
                    return true;
                }
            }
            return false;
        }
        Matcher equality = EQUALITY_FILTER.matcher(filter);
        if (!equality.matches()) {
            throw new CubeRollUpUnavailableException("filter '" + filter + "' cannot be evaluated in memory");
        }
        String column = equality.group(1);
        if (!row.containsKey(column)) {
            throw new CubeRollUpUnavailableException("equality filter on absent column '" + column + "'");
        }
        return matchesLiteral(row.get(column), frame.columnType(column), literal(equality.group(2)));
    }

    private List<CubeFilterLiteral> parseInList(String raw) {
        List<CubeFilterLiteral> literals = new ArrayList<>();
        StringBuilder token = new StringBuilder();
        boolean inQuote = false;
        boolean quoted = false;
        boolean quoteClosed = false;
        for (int index = 0; index < raw.length(); index++) {
            char current = raw.charAt(index);
            if (current == '\'') {
                if (inQuote && index + 1 < raw.length() && raw.charAt(index + 1) == '\'') {
                    token.append('\'');
                    index++;
                } else if (inQuote) {
                    inQuote = false;
                    quoteClosed = true;
                } else {
                    if (quoteClosed || !token.toString().trim().isEmpty()) {
                        throw new CubeRollUpUnavailableException("malformed quoted IN member");
                    }
                    inQuote = true;
                    quoted = true;
                }
                continue;
            }
            if (!inQuote && current == ',') {
                literals.add(literal(token.toString(), quoted));
                token.setLength(0);
                quoted = false;
                quoteClosed = false;
                continue;
            }
            if (quoteClosed && !Character.isWhitespace(current)) {
                throw new CubeRollUpUnavailableException("characters follow quoted IN member");
            }
            if (inQuote || !Character.isWhitespace(current)) {
                token.append(current);
            }
        }
        if (inQuote) {
            throw new CubeRollUpUnavailableException("unterminated quoted IN member");
        }
        literals.add(literal(token.toString(), quoted));
        return literals;
    }

    private CubeFilterLiteral literal(String raw) {
        String value = raw.trim();
        if (value.startsWith("'")) {
            if (value.length() < 2 || !value.endsWith("'")) {
                throw new CubeRollUpUnavailableException("malformed quoted filter literal");
            }
            return literal(value.substring(1, value.length() - 1).replace("''", "'"), true);
        }
        return literal(value, false);
    }

    private CubeFilterLiteral literal(String value, boolean quoted) {
        String normalized = quoted ? value : value.trim();
        return new CubeFilterLiteral(normalized, quoted, !quoted && normalized.equalsIgnoreCase("NULL"));
    }

    private boolean matchesLiteral(Object value, ColumnType type, CubeFilterLiteral literal) {
        if (value == null || literal.sqlNull()) {
            return false;
        }
        try {
            return switch (type) {
                case STRING -> {
                    if (!literal.quoted()) {
                        throw new CubeRollUpUnavailableException("unquoted literal for STRING dimension");
                    }
                    yield value.equals(literal.value());
                }
                case LONG -> {
                    if (literal.quoted()) {
                        throw new CubeRollUpUnavailableException("quoted literal for numeric dimension");
                    }
                    yield BigDecimal.valueOf(((Number) value).longValue())
                            .compareTo(new BigDecimal(literal.value())) == 0;
                }
                case DECIMAL -> {
                    if (literal.quoted()) {
                        throw new CubeRollUpUnavailableException("quoted literal for numeric dimension");
                    }
                    yield ((BigDecimal) value).compareTo(new BigDecimal(literal.value())) == 0;
                }
                case DOUBLE -> {
                    if (literal.quoted()) {
                        throw new CubeRollUpUnavailableException("quoted literal for numeric dimension");
                    }
                    double left = ((Number) value).doubleValue();
                    double right = Double.parseDouble(literal.value());
                    yield left == right || Double.isNaN(left) && Double.isNaN(right);
                }
            };
        } catch (NumberFormatException malformed) {
            throw new CubeRollUpUnavailableException("numeric filter literal cannot be parsed for " + type);
        }
    }

    private Object canonicalDimension(ColumnType type, Object value) {
        if (value == null) {
            return null;
        }
        return switch (type) {
            case STRING, LONG -> value;
            case DOUBLE -> {
                double number = ((Number) value).doubleValue();
                yield number == 0.0d ? 0.0d : number;
            }
            case DECIMAL -> ((BigDecimal) value).stripTrailingZeros();
        };
    }

    private Object combineIndependently(Object left, Object right, ColumnType type, AggregateFunction function) {
        return switch (type) {
            case LONG -> {
                long leftValue = ((Number) left).longValue();
                long rightValue = ((Number) right).longValue();
                yield switch (function) {
                    case SUM, COUNT -> Math.addExact(leftValue, rightValue);
                    case MINIMUM -> Math.min(leftValue, rightValue);
                    case MAXIMUM -> Math.max(leftValue, rightValue);
                };
            }
            case DOUBLE -> switch (function) {
                case SUM, COUNT -> ((Number) left).doubleValue() + ((Number) right).doubleValue();
                case MINIMUM -> {
                    double leftValue = ((Number) left).doubleValue();
                    double rightValue = ((Number) right).doubleValue();
                    yield Double.isNaN(leftValue) ? rightValue
                            : Double.isNaN(rightValue) ? leftValue : Math.min(leftValue, rightValue);
                }
                case MAXIMUM -> {
                    double leftValue = ((Number) left).doubleValue();
                    double rightValue = ((Number) right).doubleValue();
                    yield Double.isNaN(leftValue) || Double.isNaN(rightValue)
                            ? Double.NaN : Math.max(leftValue, rightValue);
                }
            };
            case DECIMAL -> {
                BigDecimal leftValue = (BigDecimal) left;
                BigDecimal rightValue = (BigDecimal) right;
                BigDecimal combined = switch (function) {
                    case SUM, COUNT -> leftValue.add(rightValue);
                    case MINIMUM -> leftValue.min(rightValue);
                    case MAXIMUM -> leftValue.max(rightValue);
                };
                validateDecimal(type, combined);
                yield combined;
            }
            case STRING -> throw new CubeRollUpUnavailableException("string aggregate output is unsupported");
        };
    }

    private void validateDecimal(ColumnType type, Object value) {
        if (type == ColumnType.DECIMAL) {
            BigDecimal decimal = (BigDecimal) value;
            long integerDigits = (long) decimal.precision() - Math.min(0, decimal.scale());
            if (decimal.precision() > 38 || integerDigits > 38 || Math.abs((long) decimal.scale()) > 38) {
                throw new CubeRollUpUnavailableException("DECIMAL value exceeds Spark's supported precision/scale");
            }
        }
    }

    private boolean valueEquals(Object planned, Object truth, ColumnType type) {
        if (planned == null || truth == null) {
            return planned == truth;
        }
        return switch (type) {
            case STRING -> planned.equals(truth);
            case LONG -> ((Number) planned).longValue() == ((Number) truth).longValue();
            case DOUBLE -> closeEnough(((Number) planned).doubleValue(), ((Number) truth).doubleValue());
            case DECIMAL -> ((BigDecimal) planned).compareTo((BigDecimal) truth) == 0;
        };
    }

    private boolean closeEnough(double left, double right) {
        if (Double.isNaN(left) || Double.isNaN(right)) {
            return Double.isNaN(left) && Double.isNaN(right);
        }
        if (left == right) {
            return true; // also handles matching infinities and signed zero
        }
        double difference = Math.abs(left - right);
        return difference <= TOLERANCE || difference <= TOLERANCE * Math.max(Math.abs(left), Math.abs(right));
    }

    private boolean projectionMatchesColumn(String projection, String column) {
        String expected = projection.trim();
        Matcher alias = PROJECTION_ALIAS.matcher(expected);
        if (alias.matches() && expected.toUpperCase(Locale.ROOT).contains(" AS ")) {
            expected = alias.group(1);
        }
        expected = expected.replace("`", "").replace("\"", "").trim();
        if (expected.contains(".")) {
            expected = expected.substring(expected.lastIndexOf('.') + 1);
        }
        return normalize(expected).equals(normalize(column));
    }

    private String normalize(String value) {
        return value.replace("`", "").replaceAll("\\s+", "").toUpperCase(Locale.ROOT);
    }

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

        public String reason() {
            return reason;
        }
    }
}
