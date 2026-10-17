package com.cascada.cache.application.service.cube;

import com.cascada.cache.application.model.cube.CubePlannerGroup;
import com.cascada.cache.application.model.cube.CubeAverageColumn;
import com.cascada.cache.application.service.merge.SparkAverage;
import com.cascada.cache.domain.model.cube.CachedShapeEntry;
import com.cascada.cache.domain.model.cube.QueryShape;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.frame.ResultFrameBuilder;
import com.cascada.cache.domain.merge.AggregateFunction;
import com.cascada.cache.exception.CubeRollUpUnavailableException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;

import static com.cascada.cache.application.service.cube.CubeShapeAnalysis.AGGREGATE_OF_COLUMN;
import static com.cascada.cache.application.service.cube.CubeShapeAnalysis.normalize;

/** Plans a conservative, typed roll-up of a cached cube frame. */
public final class CubeSubsumptionPlanner {

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
                .min(CubeShapeAnalysis.CHEAPEST_FIRST);
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
        return candidate.projectionSignature().containsAll(query.projectionSignature());
    }

    public boolean isGroupBySuperset(QueryShape candidate, QueryShape query) {
        return candidate.groupBy().containsAll(query.groupBy());
    }

    public boolean areAggregatesCompatible(QueryShape query) {
        return CubeShapeAnalysis.holisticAggregate(query).isEmpty();
    }

    public boolean areAggregatesDerivable(QueryShape candidate, QueryShape query) {
        Set<String> available = new HashSet<>();
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

        List<String> measureColumns = CubeShapeAnalysis.measureColumns(candidate);
        List<CubeAverageColumn> averages = CubeShapeAnalysis.averages(query, measureColumns, frame);
        if (!query.projectionSignature().isEmpty() && !averages.isEmpty()) {
            throw new CubeRollUpUnavailableException("AVG projection cannot be proven from this cached schema");
        }
        Map<String, AggregateFunction> declared = CubeShapeAnalysis.declaredAggregateFunctions(candidate.shape(), query);
        AggregateFunction[] measureFunctions = CubeShapeAnalysis.measureFunctions(measureColumns, declared);

        Set<String> extraFilters = filterEvaluator.extraFilters(candidate.shape(), query);
        int[] dimensionIndexes = new int[dimensionColumns.size()];
        ColumnType[] dimensionTypes = new ColumnType[dimensionColumns.size()];
        for (int index = 0; index < dimensionColumns.size(); index++) {
            dimensionIndexes[index] = frame.columnIndex(dimensionColumns.get(index));
            dimensionTypes[index] = frame.columnTypeAt(dimensionIndexes[index]);
        }
        int[] measureIndexes = new int[measureColumns.size()];
        ColumnType[] measureTypes = new ColumnType[measureColumns.size()];
        for (int index = 0; index < measureColumns.size(); index++) {
            measureIndexes[index] = frame.columnIndex(measureColumns.get(index));
            measureTypes[index] = frame.columnTypeAt(measureIndexes[index]);
        }

        Map<List<Object>, CubePlannerGroup> groups = new LinkedHashMap<>();
        for (int rowIndex = 0; rowIndex < frame.rowCount(); rowIndex++) {
            if (!extraFilters.isEmpty()
                    && !filterEvaluator.matchesAll(frame, frame.rows().get(rowIndex), extraFilters)) {
                continue;
            }
            List<Object> keyValues = new ArrayList<>(dimensionColumns.size());
            Object[] rawDimensions = new Object[dimensionColumns.size()];
            for (int index = 0; index < dimensionColumns.size(); index++) {
                Object value = frame.valueAt(rowIndex, dimensionIndexes[index]);
                rawDimensions[index] = value;
                keyValues.add(CubeShapeAnalysis.canonicalDimension(dimensionTypes[index], value));
            }
            List<Object> key = Collections.unmodifiableList(keyValues);
            CubePlannerGroup group = groups.computeIfAbsent(key, ignored -> new CubePlannerGroup(rawDimensions, measureColumns.size()));
            for (int measureIndex = 0; measureIndex < measureColumns.size(); measureIndex++) {
                Object value = frame.valueAt(rowIndex, measureIndexes[measureIndex]);
                if (value == null) {
                    continue;
                }
                CubeShapeAnalysis.validateDecimal(measureTypes[measureIndex], value);
                if (!group.measurePresent()[measureIndex]) {
                    group.measureValues()[measureIndex] = value;
                    group.measurePresent()[measureIndex] = true;
                } else {
                    group.measureValues()[measureIndex] = CubeShapeAnalysis.combine(group.measureValues()[measureIndex],
                            value, measureTypes[measureIndex], measureFunctions[measureIndex]);
                }
            }
        }

        List<String> outputColumns = query.projectionSignature().isEmpty()
                ? defaultOutputColumns(query, dimensionColumns, measureColumns, averages)
                : CubeShapeAnalysis.projectedColumns(candidate, query, dimensionColumns, measureColumns);
        ResultFrameBuilder result = ResultFrame.builder();
        for (String column : outputColumns) {
            if (dimensionColumns.contains(column) || measureColumns.contains(column)) {
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
                output.put(average.alias(),
                        SparkAverage.average(output.get(average.sumColumn()), output.get(average.countColumn())));
            }
            Map<String, Object> projected = new LinkedHashMap<>();
            outputColumns.forEach(column -> projected.put(column, output.get(column)));
            result.row(projected);
        }
        return result.build();
    }

    private static List<String> defaultOutputColumns(QueryShape query, List<String> dimensions,
                                                     List<String> measures, List<CubeAverageColumn> averages) {
        List<String> columns = new ArrayList<>(dimensions);
        columns.addAll(CubeShapeAnalysis.requestedMeasureColumns(query, measures, averages));
        averages.forEach(average -> columns.add(average.alias()));
        return columns;
    }
}
