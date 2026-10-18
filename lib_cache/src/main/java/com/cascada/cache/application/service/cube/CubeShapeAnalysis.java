package com.cascada.cache.application.service.cube;

import com.cascada.cache.application.service.merge.AggregateFunctionResolver;
import com.cascada.cache.application.service.merge.AggregateOperation;
import com.cascada.cache.application.service.merge.SparkValues;
import com.cascada.cache.domain.model.cube.CachedShapeEntry;
import com.cascada.cache.domain.model.cube.QueryShape;
import com.cascada.cache.domain.frame.ColumnNames;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.merge.AggregateFunction;
import com.cascada.cache.domain.model.safety.CacheConfiguration;
import com.cascada.cache.exception.CubeRollUpUnavailableException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * How a cached cube shape is read: which columns are dimensions and measures, how each measure
 * combines, which outputs and AVGs a query asks for, and how its projection binds to the cached frame.
 * The planner and the consistency verifier both use these rules; the verifier's independence lies in
 * its own regrouping pass and in comparing that against the planner's answer cell by cell.
 */
final class CubeShapeAnalysis {

    static final Pattern AGGREGATE_OF_COLUMN =
            Pattern.compile("(?i)\\s*(SUM|COUNT|MIN|MAX|AVG)\\s*\\(\\s*([^)]*?)\\s*\\)\\s*");
    private static final Pattern PROJECTION_ALIAS = Pattern.compile(
            "(?is)^.*?\\s+(?:AS\\s+)?([A-Za-z_][A-Za-z0-9_]*)\\s*$");

    /** Fewest group-by columns first (the least regrouping), then the most already-applied filters. */
    static final Comparator<CachedShapeEntry> CHEAPEST_FIRST = Comparator
            .<CachedShapeEntry>comparingInt(entry -> entry.shape().groupBy().size())
            .thenComparing(entry -> -entry.shape().filters().size());

    private CubeShapeAnalysis() {
    }

    static String normalize(String value) {
        return ColumnNames.identity(value);
    }

    /** The first aggregate that cannot be rolled up exactly (COUNT DISTINCT, MEDIAN, PERCENTILE). */
    static Optional<String> holisticAggregate(QueryShape query) {
        for (String aggregate : query.aggregates()) {
            String upper = aggregate.toUpperCase(Locale.ROOT);
            if (CacheConfiguration.HOLISTIC_AGGREGATE_MARKERS.stream().anyMatch(upper::contains)) {
                return Optional.of(aggregate);
            }
        }
        return Optional.empty();
    }

    static List<String> measureColumns(CachedShapeEntry candidate) {
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

    static Map<String, AggregateFunction> declaredAggregateFunctions(QueryShape candidate, QueryShape query) {
        Map<String, AggregateFunction> declared = new LinkedHashMap<>();
        addAggregateSpecs(declared, candidate.aggregates());
        addAggregateSpecs(declared, query.aggregates());
        addAggregateMappings(declared, candidate.outputAggregates());
        addAggregateMappings(declared, query.outputAggregates());
        return declared;
    }

    /** Every measure needs a declared mergeable function or a raw {@code FUNC(col)} name. */
    static AggregateFunction[] measureFunctions(List<String> measures, Map<String, AggregateFunction> declared) {
        AggregateFunction[] functions = new AggregateFunction[measures.size()];
        for (int index = 0; index < measures.size(); index++) {
            String measure = measures.get(index);
            AggregateFunction function = declared.get(normalize(measure));
            if (function == null) {
                Matcher rawAggregate = AGGREGATE_OF_COLUMN.matcher(measure);
                if (!rawAggregate.matches() || rawAggregate.group(1).equalsIgnoreCase("AVG")) {
                    throw new CubeRollUpUnavailableException("measure '" + measure
                            + "' has no declared mergeable aggregate function");
                }
                function = AggregateFunctionResolver.resolve(measure, declared);
            }
            functions[index] = function;
        }
        return functions;
    }

    private static void addAggregateSpecs(Map<String, AggregateFunction> declared, Set<String> specifications) {
        for (String specification : specifications) {
            addAggregateMappings(declared, AggregateFunctionResolver.fromAggregateSpecs(List.of(specification)));
        }
    }

    private static void addAggregateMappings(Map<String, AggregateFunction> declared,
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

    /** The cached measure columns a query without a projection signature asks for, in frame order. */
    static List<String> requestedMeasureColumns(QueryShape query, List<String> measures,
                                                List<CubeAverageColumn> averages) {
        Set<String> requestedNames = new HashSet<>();
        Set<AggregateFunction> requestedFunctions = new HashSet<>();
        for (String aggregate : query.aggregates()) {
            Matcher matcher = AGGREGATE_OF_COLUMN.matcher(aggregate);
            if (matcher.matches() && matcher.group(1).equalsIgnoreCase("AVG")) {
                continue;
            }
            Map<String, AggregateFunction> parsed = AggregateFunctionResolver.fromAggregateSpecs(List.of(aggregate));
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

    /** {@code AVG(col)} outputs, each backed by stored DOUBLE SUM and LONG or DOUBLE COUNT ingredients. */
    static List<CubeAverageColumn> averages(QueryShape query, List<String> measures, ResultFrame frame) {
        List<CubeAverageColumn> averages = new ArrayList<>();
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
            // Spark AVG over DECIMAL has a decimal result type. This cache shape has no precision/scale
            // metadata for reconstructing that contract, so only the established DOUBLE path is used.
            if (frame.columnType(sum) != ColumnType.DOUBLE
                    || (frame.columnType(count) != ColumnType.DOUBLE && frame.columnType(count) != ColumnType.LONG)) {
                throw new CubeRollUpUnavailableException("AVG requires DOUBLE SUM and LONG or DOUBLE COUNT ingredients");
            }
            averages.add(new CubeAverageColumn(aggregate.trim(), sum, count));
        }
        return averages;
    }

    static String findMeasure(List<String> measures, String target) {
        String normalized = normalize(target);
        for (String measure : measures) {
            if (normalize(measure).equals(normalized)) {
                return measure;
            }
        }
        return null;
    }

    /**
     * Binds the query's projection to cached frame columns. The cached projection must describe its
     * frame position by position, each requested entry must name exactly one cached position, and the
     * bound column must survive the regrouping.
     */
    static List<String> projectedColumns(CachedShapeEntry candidate, QueryShape query,
                                         List<String> dimensions, List<String> measures) {
        List<String> sourceProjection = candidate.shape().projectionSignature();
        List<String> sourceColumns = candidate.frame().columnNames();
        if (sourceProjection.size() != sourceColumns.size()) {
            throw new CubeRollUpUnavailableException("cached projection and schema widths differ");
        }
        for (int index = 0; index < sourceColumns.size(); index++) {
            if (!projectionMatchesColumn(sourceProjection.get(index), sourceColumns.get(index))) {
                throw new CubeRollUpUnavailableException("cached projection does not describe its frame");
            }
        }
        List<String> columns = new ArrayList<>();
        for (String requested : query.projectionSignature()) {
            int index = sourceProjection.indexOf(requested);
            if (index < 0) {
                throw new CubeRollUpUnavailableException("requested projection is unavailable");
            }
            if (sourceProjection.lastIndexOf(requested) != index) {
                throw new CubeRollUpUnavailableException("requested projection names several cached columns");
            }
            String column = sourceColumns.get(index);
            if (!dimensions.contains(column) && !measures.contains(column)) {
                throw new CubeRollUpUnavailableException("projection is unavailable after roll-up");
            }
            columns.add(column);
        }
        return columns;
    }

    private static boolean projectionMatchesColumn(String projection, String column) {
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

    /** Grouping identity of a dimension value, using Spark's key normalisation. */
    static Object canonicalDimension(ColumnType type, Object value) {
        if (value == null) {
            return null;
        }
        return switch (type) {
            case STRING, LONG -> value;
            case DOUBLE -> SparkValues.canonicalDouble(((Number) value).doubleValue());
            case DECIMAL -> SparkValues.canonicalDecimal((BigDecimal) value);
        };
    }

    /** Combines two partial values of one measure the way the cache merge does. */
    static Object combine(Object left, Object right, ColumnType type, AggregateFunction function) {
        return switch (type) {
            case LONG -> {
                long leftValue = ((Number) left).longValue();
                long rightValue = ((Number) right).longValue();
                yield switch (function) {
                    case SUM, COUNT -> Math.addExact(leftValue, rightValue);
                    case MINIMUM -> Math.min(leftValue, rightValue);
                    case MAXIMUM -> Math.max(leftValue, rightValue);
                    case AVERAGE -> throw new CubeRollUpUnavailableException("derived measures are not rolled up");
                };
            }
            case DOUBLE -> {
                if (function == AggregateFunction.AVERAGE) {
                    throw new CubeRollUpUnavailableException("derived measures are not rolled up");
                }
                yield AggregateOperation.fromFunction(function)
                        .combine(((Number) left).doubleValue(), ((Number) right).doubleValue());
            }
            case DECIMAL -> {
                BigDecimal leftValue = (BigDecimal) left;
                BigDecimal rightValue = (BigDecimal) right;
                BigDecimal combined = switch (function) {
                    case SUM, COUNT -> leftValue.add(rightValue);
                    case MINIMUM -> leftValue.min(rightValue);
                    case MAXIMUM -> leftValue.max(rightValue);
                    case AVERAGE -> throw new CubeRollUpUnavailableException("derived measures are not rolled up");
                };
                validateDecimal(type, combined);
                yield combined;
            }
            case STRING -> throw new CubeRollUpUnavailableException("string aggregate output is unsupported");
        };
    }

    static void validateDecimal(ColumnType type, Object value) {
        if (type != ColumnType.DECIMAL) {
            return;
        }
        try {
            SparkValues.requireDecimal((BigDecimal) value, "cube measure");
        } catch (IllegalArgumentException unrepresentable) {
            throw new CubeRollUpUnavailableException(unrepresentable.getMessage());
        }
    }
}
