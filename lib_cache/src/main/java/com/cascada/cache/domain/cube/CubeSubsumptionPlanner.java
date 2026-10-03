package com.cascada.cache.domain.cube;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.merge.AggregateFunction;
import com.cascada.cache.domain.merge.AggregateFunctionResolver;
import com.cascada.cache.domain.merge.AverageReconstructionService;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Plans a conservative, typed roll-up of a cached cube frame. */
public final class CubeSubsumptionPlanner {

    private static final Pattern AGGREGATE_OF_COLUMN =
            Pattern.compile("(?i)\\s*(SUM|COUNT|MIN|MAX|AVG)\\s*\\(\\s*([^)]*?)\\s*\\)\\s*");
    private static final Pattern PROJECTION_ALIAS = Pattern.compile(
            "(?is)^.*?\\s+(?:AS\\s+)?([A-Za-z_][A-Za-z0-9_]*)\\s*$");
    private static final Set<String> HOLISTIC_MARKERS = Set.of("DISTINCT", "MEDIAN", "PERCENTILE");

    private final AverageReconstructionService averageReconstructionService = new AverageReconstructionService();
    private final CubeFilterEvaluator filterEvaluator = new CubeFilterEvaluator();

    public Optional<CachedShapeEntry> findSubsumingCacheEntryForQuery(QueryShape query,
                                                                      List<CachedShapeEntry> candidates) {
        for (CachedShapeEntry candidate : candidates) {
            if (subsumes(candidate.shape(), query)) {
                return Optional.of(candidate);
            }
        }
        return Optional.empty();
    }

    public Optional<CachedShapeEntry> findBestSubsumingCacheEntryForQuery(QueryShape query,
                                                                          List<CachedShapeEntry> candidates) {
        return candidates.stream()
                .filter(candidate -> subsumes(candidate.shape(), query))
                .min(java.util.Comparator
                        .<CachedShapeEntry>comparingInt(candidate -> candidate.shape().groupBy().size())
                        .thenComparing(candidate -> -candidate.shape().filters().size()));
    }

    public boolean subsumes(QueryShape candidate, QueryShape query) {
        return candidate.sources().equals(query.sources())
                && candidate.outputAggregates().equals(query.outputAggregates())
                && hasSameKnownProjection(candidate, query)
                && candidate.groupBy().containsAll(query.groupBy())
                && areAggregatesCompatible(query)
                && areAggregatesDerivable(candidate, query)
                && filterEvaluator.canNarrow(candidate, query);
    }

    private boolean hasSameKnownProjection(QueryShape candidate, QueryShape query) {
        if (candidate.projectionSignature().isEmpty() || query.projectionSignature().isEmpty()) {
            return candidate.projectionSignature().isEmpty() && query.projectionSignature().isEmpty();
        }
        return candidate.projectionSignature().equals(query.projectionSignature());
    }

    public boolean isGroupBySuperset(QueryShape candidate, QueryShape query) {
        return candidate.groupBy().containsAll(query.groupBy());
    }

    public boolean areAggregatesCompatible(QueryShape query) {
        for (String aggregate : query.aggregates()) {
            String upper = aggregate.toUpperCase(Locale.ROOT);
            if (HOLISTIC_MARKERS.stream().anyMatch(upper::contains)) {
                return false;
            }
        }
        return true;
    }

    public boolean areAggregatesDerivable(QueryShape candidate, QueryShape query) {
        Set<String> available = new java.util.HashSet<>();
        candidate.aggregates().forEach(aggregate -> available.add(normalize(aggregate)));
        for (String aggregate : query.aggregates()) {
            Matcher matcher = AGGREGATE_OF_COLUMN.matcher(aggregate);
            if (!matcher.matches()) {
                if (!available.contains(normalize(aggregate))) {
                    return false;
                }
                continue;
            }
            String function = matcher.group(1).toUpperCase(Locale.ROOT);
            String column = matcher.group(2);
            if (function.equals("AVG")) {
                if (!available.contains(normalize("SUM(" + column + ")"))
                        || !available.contains(normalize("COUNT(" + column + ")"))) {
                    return false;
                }
            } else if (!available.contains(normalize(aggregate))) {
                return false;
            }
        }
        return true;
    }

    public boolean isFilterNarrowable(QueryShape candidate, QueryShape query) {
        return filterEvaluator.isFilterNarrowable(candidate, query);
    }

    public boolean extraFiltersAreOnGroupedColumns(QueryShape candidate, QueryShape query) {
        return filterEvaluator.extraFiltersAreOnGroupedColumns(candidate, query);
    }

    /** Roll up with original dimension and measure types; unsupported semantics are rejected. */
    public ResultFrame rollUpAndFilterDown(CachedShapeEntry candidate, QueryShape query) {
        ResultFrame frame = candidate.frame();
        List<String> dimensionColumns = frame.columnNames().stream()
                .filter(query.groupBy()::contains)
                .toList();
        if (!dimensionColumns.containsAll(query.groupBy())) {
            throw new CubeRollUpUnavailableException("a requested group-by column is absent from the cached frame");
        }

        List<String> measureColumns = measureColumns(candidate);
        List<CubeAverageColumn> averages = requestedAverages(query, measureColumns, frame);
        validateProjectionContract(candidate, query, averages);

        Map<String, AggregateFunction> declared = declaredAggregateFunctions(candidate.shape(), query);
        validateMeasureCombiners(measureColumns, declared);

        Set<String> extraFilters = filterEvaluator.extraFilters(candidate.shape(), query);
        Map<List<Object>, CubePlannerGroup> groups = new LinkedHashMap<>();
        for (Map<String, Object> row : frame.rows()) {
            if (!filterEvaluator.matchesAll(frame, row, extraFilters)) {
                continue;
            }
            List<Object> keyValues = new ArrayList<>(dimensionColumns.size());
            Object[] rawDimensions = new Object[dimensionColumns.size()];
            for (int index = 0; index < dimensionColumns.size(); index++) {
                String dimension = dimensionColumns.get(index);
                Object value = row.get(dimension);
                rawDimensions[index] = value;
                keyValues.add(canonicalDimension(frame.columnType(dimension), value));
            }
            List<Object> key = Collections.unmodifiableList(keyValues);
            CubePlannerGroup group = groups.computeIfAbsent(key, ignored -> new CubePlannerGroup(rawDimensions, measureColumns.size()));
            for (int measureIndex = 0; measureIndex < measureColumns.size(); measureIndex++) {
                String measure = measureColumns.get(measureIndex);
                Object value = row.get(measure);
                if (value == null) {
                    continue;
                }
                if (!group.measurePresent()[measureIndex]) {
                    validateDecimalValue(frame.columnType(measure), value);
                    group.measureValues()[measureIndex] = value;
                    group.measurePresent()[measureIndex] = true;
                } else {
                    AggregateFunction function = declared.get(normalize(measure));
                    if (function == null) {
                        function = AggregateFunctionResolver.resolve(measure, declared);
                    }
                    group.measureValues()[measureIndex] = combine(group.measureValues()[measureIndex], value,
                            frame.columnType(measure), function);
                }
            }
        }

        List<String> outputColumns = outputColumns(candidate, query, dimensionColumns, measureColumns, averages);
        ResultFrame.Builder result = ResultFrame.builder();
        for (String column : outputColumns) {
            if (dimensionColumns.contains(column)) {
                result.column(column, frame.columnType(column));
            } else if (measureColumns.contains(column)) {
                result.column(column, frame.columnType(column));
            } else if (averages.stream().anyMatch(average -> average.alias().equals(column))) {
                result.column(column, ColumnType.DOUBLE);
            } else {
                throw new CubeRollUpUnavailableException("output column '" + column + "' cannot be reconstructed");
            }
        }

        for (CubePlannerGroup group : groups.values()) {
            Map<String, Object> output = new LinkedHashMap<>();
            for (int index = 0; index < dimensionColumns.size(); index++) {
                output.put(dimensionColumns.get(index), group.dimensionValues()[index]);
            }
            for (int index = 0; index < measureColumns.size(); index++) {
                output.put(measureColumns.get(index), group.measurePresent()[index] ? group.measureValues()[index] : null);
            }
            for (CubeAverageColumn average : averages) {
                Object sum = output.get(average.sumColumn());
                Object count = output.get(average.countColumn());
                Object value = null;
                if (sum != null && count != null) {
                    long exactCount = exactAverageCount(count);
                    if (exactCount > 0) {
                        value = averageReconstructionService.reconstructAverageFromStoredSumAndCount(
                                ((Number) sum).doubleValue(), exactCount);
                    }
                }
                output.put(average.alias(), value);
            }
            Map<String, Object> projected = new LinkedHashMap<>();
            outputColumns.forEach(column -> projected.put(column, output.get(column)));
            result.row(projected);
        }
        return result.build();
    }

    private List<String> measureColumns(CachedShapeEntry candidate) {
        ResultFrame frame = candidate.frame();
        List<String> measures = new ArrayList<>();
        for (String column : frame.columnNames()) {
            if (candidate.shape().groupBy().contains(column)) {
                continue;
            }
            if (frame.columnType(column) == ColumnType.STRING) {
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

    private List<String> outputColumns(CachedShapeEntry candidate, QueryShape query,
                                      List<String> dimensions, List<String> measures,
                                      List<CubeAverageColumn> averages) {
        if (!query.projectionSignature().isEmpty()) {
            // The production shape carries the ordered SELECT contract. Matching signatures make
            // the candidate's physical column order the requested order; hidden/rolled-away output
            // dimensions or unrecognized expression names are conservatively refused.
            List<String> columns = candidate.frame().columnNames();
            if (columns.size() != query.projectionSignature().size()) {
                throw new CubeRollUpUnavailableException("projection and cached result schema have different widths");
            }
            for (int index = 0; index < columns.size(); index++) {
                if (!projectionMatchesColumn(query.projectionSignature().get(index), columns.get(index))) {
                    throw new CubeRollUpUnavailableException("projection order does not match the cached result schema");
                }
                if (candidate.shape().groupBy().contains(columns.get(index))
                        && !query.groupBy().contains(columns.get(index))) {
                    throw new CubeRollUpUnavailableException("a rolled-away grouped output column cannot be projected");
                }
                if (!dimensions.contains(columns.get(index)) && !measures.contains(columns.get(index))) {
                    throw new CubeRollUpUnavailableException("projection column is not available after roll-up");
                }
            }
            return columns;
        }

        List<String> columns = new ArrayList<>(dimensions);
        columns.addAll(requestedMeasureColumns(query, measures, averages));
        averages.forEach(average -> columns.add(average.alias()));
        return columns;
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
                if (!normalize(aggregate).isBlank() && findMeasure(measures, aggregate) != null) {
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

    private String findMeasure(List<String> measures, String target) {
        String normalized = normalize(target);
        for (String measure : measures) {
            if (normalize(measure).equals(normalized)) {
                return measure;
            }
        }
        return null;
    }

    private void validateProjectionContract(CachedShapeEntry candidate, QueryShape query,
                                            List<CubeAverageColumn> averages) {
        if (query.projectionSignature().isEmpty()) {
            return;
        }
        if (!averages.isEmpty()) {
            throw new CubeRollUpUnavailableException("AVG projection cannot be proven from this cached schema");
        }
        for (String column : candidate.frame().columnNames()) {
            if (candidate.shape().groupBy().contains(column) && !query.groupBy().contains(column)) {
                throw new CubeRollUpUnavailableException(
                        "candidate exposes a dimension that is not in the requested grouping");
            }
        }
    }

    private List<CubeAverageColumn> requestedAverages(QueryShape query, List<String> measures, ResultFrame frame) {
        List<CubeAverageColumn> averages = new ArrayList<>();
        for (String aggregate : query.aggregates()) {
            Matcher matcher = AGGREGATE_OF_COLUMN.matcher(aggregate);
            if (!matcher.matches() || !matcher.group(1).equalsIgnoreCase("AVG")) {
                continue;
            }
            String column = matcher.group(2);
            String sum = resolveMeasureColumn(measures, "SUM(" + column + ")");
            String count = resolveMeasureColumn(measures, "COUNT(" + column + ")");
            if (sum == null || count == null) {
                throw new CubeRollUpUnavailableException("AVG(" + column + ") needs stored SUM and COUNT ingredients");
            }
            // Spark AVG over DECIMAL has a decimal result type. This cache shape has no precision/scale
            // metadata for reconstructing that contract, so only the established DOUBLE path is used.
            if (frame.columnType(sum) != ColumnType.DOUBLE || frame.columnType(count) != ColumnType.DOUBLE) {
                throw new CubeRollUpUnavailableException("AVG ingredients are not DOUBLE columns");
            }
            averages.add(new CubeAverageColumn(aggregate.trim(), sum, count));
        }
        return averages;
    }

    private String resolveMeasureColumn(List<String> measures, String target) {
        String normalized = normalize(target);
        return measures.stream().filter(measure -> normalize(measure).equals(normalized)).findFirst().orElse(null);
    }

    private long exactAverageCount(Object count) {
        double value = ((Number) count).doubleValue();
        if (!Double.isFinite(value) || value < 0.0d || value > 9_007_199_254_740_992.0d
                || value != Math.rint(value)) {
            throw new CubeRollUpUnavailableException("AVG count ingredient is not an exactly representable count");
        }
        return (long) value;
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

    private Object combine(Object left, Object right, ColumnType type, AggregateFunction function) {
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
            case DOUBLE -> function.combine(((Number) left).doubleValue(), ((Number) right).doubleValue());
            case DECIMAL -> {
                BigDecimal leftValue = (BigDecimal) left;
                BigDecimal rightValue = (BigDecimal) right;
                BigDecimal combined = switch (function) {
                    case SUM, COUNT -> leftValue.add(rightValue);
                    case MINIMUM -> leftValue.min(rightValue);
                    case MAXIMUM -> leftValue.max(rightValue);
                };
                validateDecimalValue(type, combined);
                yield combined;
            }
            case STRING -> throw new CubeRollUpUnavailableException("string aggregate output is unsupported");
        };
    }

    private void validateDecimalValue(ColumnType type, Object value) {
        if (type == ColumnType.DECIMAL) {
            BigDecimal decimal = (BigDecimal) value;
            long integerDigits = (long) decimal.precision() - Math.min(0, decimal.scale());
            if (decimal.precision() > 38 || integerDigits > 38 || Math.abs((long) decimal.scale()) > 38) {
                throw new CubeRollUpUnavailableException("DECIMAL value exceeds Spark's supported precision/scale");
            }
        }
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

}
