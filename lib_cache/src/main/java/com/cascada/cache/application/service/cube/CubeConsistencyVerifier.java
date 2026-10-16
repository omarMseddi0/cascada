package com.cascada.cache.application.service.cube;

import com.cascada.cache.application.service.merge.SparkAverage;
import com.cascada.cache.domain.cube.CachedShapeEntry;
import com.cascada.cache.domain.cube.QueryShape;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.merge.AggregateFunction;
import com.cascada.cache.exception.CubeRollUpUnavailableException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Checks a proposed cube roll-up against a direct regrouping of its input. Reading the cached shape
 * follows {@link CubeShapeAnalysis}; this class independently regroups every input row and compares the
 * planner's answer with that regrouping group by group and cell by cell.
 */
public final class CubeConsistencyVerifier {

    private static final double TOLERANCE = 1e-9;

    private final CubeFilterEvaluator filterEvaluator = new CubeFilterEvaluator();

    /** Any exception while checking a candidate is a cache miss, never an exception to the query. */
    public CubeVerificationResult verifyRollUp(CachedShapeEntry candidate, QueryShape query,
                                           ResultFrame plannerAnswer) {
        try {
            return verifyRollUpUnchecked(candidate, query, plannerAnswer);
        } catch (RuntimeException unverifiable) {
            String message = unverifiable.getMessage();
            return CubeVerificationResult.inconsistent(message == null
                    ? "candidate roll-up could not be verified" : message);
        }
    }

    private CubeVerificationResult verifyRollUpUnchecked(CachedShapeEntry candidate, QueryShape query,
                                                      ResultFrame plannerAnswer) {
        Optional<String> holistic = CubeShapeAnalysis.holisticAggregate(query);
        if (holistic.isPresent()) {
            return CubeVerificationResult.inconsistent(
                    "holistic aggregate '" + holistic.get() + "' cannot be rolled up exactly");
        }

        ResultFrame frame = candidate.frame();
        List<String> dimensions = frame.columnNames().stream().filter(query.groupBy()::contains).toList();
        for (String grouped : query.groupBy()) {
            if (!frame.columnNames().contains(grouped)) {
                return CubeVerificationResult.inconsistent("query group-by column '" + grouped
                        + "' is absent from the cached frame");
            }
        }
        List<String> measures = CubeShapeAnalysis.measureColumns(candidate);
        List<CubeAverageColumn> averages = CubeShapeAnalysis.averages(query, measures, frame);
        if (!query.projectionSignature().isEmpty() && !averages.isEmpty()) {
            return CubeVerificationResult.inconsistent("AVG projection cannot be proven from this cached schema");
        }
        CubeExpectedSchema expectedSchema = expectedSchema(candidate, query, dimensions, measures, averages);
        if (!plannerAnswer.columnNames().equals(expectedSchema.names())
                || !plannerAnswer.columnTypes().equals(expectedSchema.types())) {
            return CubeVerificationResult.inconsistent("planner output schema or column order differs from the query");
        }

        Set<String> extraFilters = filterEvaluator.extraFilters(candidate.shape(), query);
        Map<String, AggregateFunction> declared = CubeShapeAnalysis.declaredAggregateFunctions(candidate.shape(), query);
        AggregateFunction[] measureFunctions = CubeShapeAnalysis.measureFunctions(measures, declared);

        int[] dimensionIndexes = new int[dimensions.size()];
        ColumnType[] dimensionTypes = new ColumnType[dimensions.size()];
        for (int index = 0; index < dimensions.size(); index++) {
            dimensionIndexes[index] = frame.columnIndex(dimensions.get(index));
            dimensionTypes[index] = frame.columnTypeAt(dimensionIndexes[index]);
        }
        int[] measureIndexes = new int[measures.size()];
        ColumnType[] measureTypes = new ColumnType[measures.size()];
        for (int index = 0; index < measures.size(); index++) {
            measureIndexes[index] = frame.columnIndex(measures.get(index));
            measureTypes[index] = frame.columnTypeAt(measureIndexes[index]);
        }

        Map<List<Object>, CubeVerifierGroup> oracle = new LinkedHashMap<>();
        for (int rowIndex = 0; rowIndex < frame.rowCount(); rowIndex++) {
            if (!extraFilters.isEmpty()
                    && !filterEvaluator.matchesAll(frame, frame.rows().get(rowIndex), extraFilters)) {
                continue;
            }
            List<Object> parts = new ArrayList<>(dimensions.size());
            for (int index = 0; index < dimensions.size(); index++) {
                Object value = frame.valueAt(rowIndex, dimensionIndexes[index]);
                parts.add(CubeShapeAnalysis.canonicalDimension(dimensionTypes[index], value));
            }
            List<Object> key = Collections.unmodifiableList(parts);
            CubeVerifierGroup group = oracle.computeIfAbsent(key, ignored -> new CubeVerifierGroup(measures.size()));
            for (int index = 0; index < measures.size(); index++) {
                Object value = frame.valueAt(rowIndex, measureIndexes[index]);
                if (value == null) {
                    continue;
                }
                ColumnType type = measureTypes[index];
                CubeShapeAnalysis.validateDecimal(type, value);
                if (!group.measurePresent()[index]) {
                    group.measureValues()[index] = value;
                    group.measurePresent()[index] = true;
                } else {
                    group.measureValues()[index] = CubeShapeAnalysis.combine(group.measureValues()[index], value, type,
                            measureFunctions[index]);
                }
            }
        }

        if (plannerAnswer.rowCount() != oracle.size()) {
            return CubeVerificationResult.inconsistent("row count mismatch: planner=" + plannerAnswer.rowCount()
                    + " oracle=" + oracle.size());
        }
        Set<List<Object>> seenGroups = new HashSet<>();
        for (Map<String, Object> row : plannerAnswer.rows()) {
            List<Object> parts = new ArrayList<>(dimensions.size());
            for (String dimension : dimensions) {
                if (!row.containsKey(dimension)) {
                    return CubeVerificationResult.inconsistent("planner row missing dimension '" + dimension + "'");
                }
                parts.add(CubeShapeAnalysis.canonicalDimension(frame.columnType(dimension), row.get(dimension)));
            }
            List<Object> key = Collections.unmodifiableList(parts);
            if (!seenGroups.add(key)) {
                return CubeVerificationResult.inconsistent("planner emitted a duplicate group");
            }
            CubeVerifierGroup truth = oracle.get(key);
            if (truth == null) {
                return CubeVerificationResult.inconsistent("planner produced an unexpected group");
            }
            for (int index = 0; index < measures.size(); index++) {
                String measure = measures.get(index);
                if (!expectedSchema.names().contains(measure)) {
                    continue;
                }
                Object expected = truth.measurePresent()[index] ? truth.measureValues()[index] : null;
                if (!valueEquals(row.get(measure), expected, frame.columnType(measure))) {
                    return CubeVerificationResult.inconsistent("measure '" + measure + "' disagrees with direct roll-up");
                }
            }
            for (CubeAverageColumn average : averages) {
                Double expected = SparkAverage.average(truthValue(truth, measures, average.sumColumn()),
                        truthValue(truth, measures, average.countColumn()));
                Object actual = row.get(average.alias());
                boolean matchesAverage = expected == null
                        ? actual == null
                        : actual instanceof Number number && closeEnough(number.doubleValue(), expected);
                if (!matchesAverage) {
                    return CubeVerificationResult.inconsistent("AVG '" + average.alias()
                            + "' disagrees with direct roll-up");
                }
            }
        }
        return CubeVerificationResult.consistent();
    }

    private CubeExpectedSchema expectedSchema(CachedShapeEntry candidate, QueryShape query,
                                  List<String> dimensions, List<String> measures,
                                  List<CubeAverageColumn> averages) {
        ResultFrame frame = candidate.frame();
        if (candidate.shape().projectionSignature().isEmpty() != query.projectionSignature().isEmpty()) {
            throw new CubeRollUpUnavailableException("one side has no projection signature");
        }
        List<String> names = new ArrayList<>();
        Map<String, ColumnType> types = new LinkedHashMap<>();
        if (!query.projectionSignature().isEmpty()) {
            for (String column : CubeShapeAnalysis.projectedColumns(candidate, query, dimensions, measures)) {
                names.add(column);
                types.put(column, frame.columnType(column));
            }
            return new CubeExpectedSchema(List.copyOf(names), Map.copyOf(types));
        }
        for (String dimension : dimensions) {
            names.add(dimension);
            types.put(dimension, frame.columnType(dimension));
        }
        for (String measure : CubeShapeAnalysis.requestedMeasureColumns(query, measures, averages)) {
            names.add(measure);
            types.put(measure, frame.columnType(measure));
        }
        for (CubeAverageColumn average : averages) {
            names.add(average.alias());
            types.put(average.alias(), ColumnType.DOUBLE);
        }
        return new CubeExpectedSchema(List.copyOf(names), Map.copyOf(types));
    }

    private Object truthValue(CubeVerifierGroup group, List<String> measures, String measure) {
        int index = measures.indexOf(measure);
        return group.measurePresent()[index] ? group.measureValues()[index] : null;
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
}
