package com.cascada.cache.application;

import com.cascada.cache.domain.CanonicalQueryObject;
import com.cascada.cache.domain.OrderByClause;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.merge.AggregateFunction;
import com.cascada.cache.domain.merge.AggregationRow;
import com.cascada.cache.domain.merge.AverageReconstructionService;
import com.cascada.cache.domain.merge.GlobalAggregateMerger;
import com.cascada.cache.domain.merge.TimeSeriesBucketResampler;
import com.cascada.cache.domain.merge.TimeSeriesBucketResampler.TimeSeriesRow;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Bridges {@link ResultFrame}s to the framework-free merge math, reproducing the merge step of
 * {@code cache_execution_engine.py} / {@code merging.py}: pick the time-series vs global-aggregate
 * strategy, drop exact-duplicate rows (RC3), combine measures additively (COUNT included),
 * resample/roll-up, then reconstruct {@code AVG} from {@code SUM}/{@code COUNT} (RC4) and apply the
 * deferred {@code ORDER BY}/{@code LIMIT}.
 */
public final class FrameMergeService {

    private static final Pattern AVG_SPEC =
            Pattern.compile("(?i)AVG\\s*\\(\\s*([^)]+?)\\s*\\)(?:\\s+AS\\s+([A-Za-z_][A-Za-z0-9_]*))?");

    private final GlobalAggregateMerger globalAggregateMerger = new GlobalAggregateMerger();
    private final TimeSeriesBucketResampler timeSeriesResampler = new TimeSeriesBucketResampler();
    private final AverageReconstructionService averageReconstructionService = new AverageReconstructionService();
    private final int fixedStepSeconds;
    private final String timeColumnName;

    public FrameMergeService(int fixedStepSeconds, String timeColumnName) {
        this.fixedStepSeconds = fixedStepSeconds;
        this.timeColumnName = timeColumnName;
    }

    public ResultFrame mergeAndReconstruct(List<ResultFrame> frames, CanonicalQueryObject canonicalObject) {
        List<ResultFrame> nonEmpty = frames.stream().filter(frame -> !frame.isEmpty()).toList();
        if (nonEmpty.isEmpty()) {
            return ResultFrame.empty();
        }

        ResultFrame merged = canonicalObject.metadata().isTimeSeries()
                ? mergeTimeSeries(nonEmpty, canonicalObject)
                : mergeGlobalAggregate(nonEmpty, canonicalObject);

        ResultFrame reconstructed = reconstructAverages(merged, canonicalObject);
        return applyPostProcessing(reconstructed, canonicalObject);
    }

    // --- time-series path -----------------------------------------------------------------------

    private ResultFrame mergeTimeSeries(List<ResultFrame> frames, CanonicalQueryObject canonicalObject) {
        List<String> dimensionColumns = dimensionColumns(frames.get(0), canonicalObject, true);
        List<String> measureColumns = measureColumns(frames.get(0), canonicalObject, dimensionColumns, true);

        List<TimeSeriesRow> rows = new ArrayList<>();
        for (ResultFrame frame : frames) {
            for (Map<String, Object> row : frame.rows()) {
                long bucketStart = asLong(row.get(timeColumnName));
                Map<String, String> dimensions = new LinkedHashMap<>();
                for (String dimension : dimensionColumns) {
                    dimensions.put(dimension, String.valueOf(row.get(dimension)));
                }
                Map<String, Double> measures = new LinkedHashMap<>();
                for (String measure : measureColumns) {
                    measures.put(measure, asDouble(row.get(measure)));
                }
                rows.add(new TimeSeriesRow(bucketStart, dimensions, measures));
            }
        }

        List<TimeSeriesRow> deduplicated = rows.stream().distinct().toList(); // RC3 guard
        int userStep = canonicalObject.userStepSeconds().orElse(fixedStepSeconds);
        Map<String, AggregateFunction> aggregations = resolveAggregations(measureColumns);
        List<TimeSeriesRow> resampled =
                timeSeriesResampler.resampleToUserStep(deduplicated, fixedStepSeconds, userStep, aggregations);

        ResultFrame.Builder builder = ResultFrame.builder().column(timeColumnName, ColumnType.LONG);
        dimensionColumns.forEach(dimension -> builder.column(dimension, ColumnType.STRING));
        measureColumns.forEach(measure -> builder.column(measure, ColumnType.DOUBLE));
        for (TimeSeriesRow row : resampled) {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put(timeColumnName, row.bucketStartSeconds());
            values.putAll(row.dimensions());
            values.putAll(row.measures());
            builder.row(values);
        }
        return builder.build();
    }

    // --- global-aggregate path ------------------------------------------------------------------

    private ResultFrame mergeGlobalAggregate(List<ResultFrame> frames, CanonicalQueryObject canonicalObject) {
        List<String> dimensionColumns = dimensionColumns(frames.get(0), canonicalObject, false);
        List<String> measureColumns = measureColumns(frames.get(0), canonicalObject, dimensionColumns, false);

        List<AggregationRow> rows = new ArrayList<>();
        for (ResultFrame frame : frames) {
            for (Map<String, Object> row : frame.rows()) {
                Map<String, String> dimensions = new LinkedHashMap<>();
                for (String dimension : dimensionColumns) {
                    dimensions.put(dimension, String.valueOf(row.get(dimension)));
                }
                Map<String, Double> measures = new LinkedHashMap<>();
                for (String measure : measureColumns) {
                    measures.put(measure, asDouble(row.get(measure)));
                }
                rows.add(new AggregationRow(dimensions, measures));
            }
        }

        Map<String, AggregateFunction> aggregations = resolveAggregations(measureColumns);
        List<AggregationRow> merged = globalAggregateMerger.merge(rows, aggregations, true);

        ResultFrame.Builder builder = ResultFrame.builder();
        dimensionColumns.forEach(dimension -> builder.column(dimension, ColumnType.STRING));
        measureColumns.forEach(measure -> builder.column(measure, ColumnType.DOUBLE));
        for (AggregationRow row : merged) {
            Map<String, Object> values = new LinkedHashMap<>(row.dimensions());
            values.putAll(row.measures());
            builder.row(values);
        }
        return builder.build();
    }

    // --- AVG reconstruction (RC4) ----------------------------------------------------------------

    private ResultFrame reconstructAverages(ResultFrame frame, CanonicalQueryObject canonicalObject) {
        List<String> originalAggregates = canonicalObject.metadata().originalAggregates();
        if (originalAggregates.isEmpty() || frame.isEmpty()) {
            return frame;
        }

        Map<String, String> averageAliasToColumn = new LinkedHashMap<>();
        for (String aggregate : originalAggregates) {
            Matcher matcher = AVG_SPEC.matcher(aggregate);
            if (matcher.find()) {
                String column = matcher.group(1).trim();
                String alias = matcher.group(2) != null ? matcher.group(2) : "AVG(" + column + ")";
                averageAliasToColumn.put(alias, column);
            }
        }
        if (averageAliasToColumn.isEmpty()) {
            return frame;
        }

        List<String> sumCountColumnsToDrop = new ArrayList<>();
        ResultFrame.Builder builder = ResultFrame.builder();
        for (String column : frame.columnNames()) {
            builder.column(column, frame.columnType(column));
        }
        averageAliasToColumn.keySet().forEach(alias -> builder.column(alias, ColumnType.DOUBLE));

        List<Map<String, Object>> rebuiltRows = new ArrayList<>();
        for (Map<String, Object> row : frame.rows()) {
            Map<String, Object> rebuilt = new LinkedHashMap<>(row);
            averageAliasToColumn.forEach((alias, column) -> {
                String sumColumn = "SUM(" + column + ")";
                String countColumn = "COUNT(" + column + ")";
                if (row.containsKey(sumColumn) && row.containsKey(countColumn)) {
                    double sum = asDouble(row.get(sumColumn));
                    long count = (long) asDouble(row.get(countColumn));
                    rebuilt.put(alias, averageReconstructionService
                            .reconstructAverageFromStoredSumAndCount(sum, count));
                    sumCountColumnsToDrop.add(sumColumn);
                    sumCountColumnsToDrop.add(countColumn);
                }
            });
            rebuiltRows.add(rebuilt);
        }

        // Re-project, dropping the SUM/COUNT ingredients now folded into the AVG alias.
        List<String> finalColumns = new ArrayList<>(frame.columnNames());
        finalColumns.addAll(averageAliasToColumn.keySet());
        finalColumns.removeIf(sumCountColumnsToDrop::contains);

        ResultFrame.Builder finalBuilder = ResultFrame.builder();
        for (String column : finalColumns) {
            ColumnType type = averageAliasToColumn.containsKey(column) ? ColumnType.DOUBLE : frame.columnType(column);
            finalBuilder.column(column, type);
        }
        for (Map<String, Object> row : rebuiltRows) {
            finalBuilder.row(row);
        }
        return finalBuilder.build();
    }

    // --- deferred ORDER BY / LIMIT ---------------------------------------------------------------

    private ResultFrame applyPostProcessing(ResultFrame frame, CanonicalQueryObject canonicalObject) {
        if (frame.isEmpty()) {
            return frame;
        }
        List<Map<String, Object>> rows = new ArrayList<>(frame.rows());

        List<OrderByClause> orderBy = canonicalObject.postProcessing().orderBy();
        // Apply stable sorts from lowest priority to highest (mirrors merging.apply_post_processing).
        for (int index = orderBy.size() - 1; index >= 0; index--) {
            OrderByClause clause = orderBy.get(index);
            if (clause.column().isEmpty()) {
                continue; // expression ordering is left to Spark (bypass), as in the Python guard
            }
            String column = clause.column().get();
            Comparator<Map<String, Object>> comparator = Comparator.comparing(
                    row -> (Comparable) toComparable(row.get(column)),
                    Comparator.nullsFirst(Comparator.naturalOrder()));
            if (!clause.ascending()) {
                comparator = comparator.reversed();
            }
            rows.sort(comparator);
        }

        canonicalObject.postProcessing().limit().ifPresent(limit -> {
            while (rows.size() > limit) {
                rows.remove(rows.size() - 1);
            }
        });

        ResultFrame.Builder builder = ResultFrame.builder();
        for (String column : frame.columnNames()) {
            builder.column(column, frame.columnType(column));
        }
        rows.forEach(builder::row);
        return builder.build();
    }

    // --- helpers ---------------------------------------------------------------------------------

    private List<String> dimensionColumns(ResultFrame frame, CanonicalQueryObject canonicalObject,
                                          boolean isTimeSeries) {
        List<String> dimensions = new ArrayList<>();
        for (String groupByColumn : canonicalObject.hashComponents().groupBy()) {
            if (frame.columnNames().contains(groupByColumn)
                    && !(isTimeSeries && groupByColumn.equals(timeColumnName))) {
                dimensions.add(groupByColumn);
            }
        }
        return dimensions;
    }

    private List<String> measureColumns(ResultFrame frame, CanonicalQueryObject canonicalObject,
                                        List<String> dimensionColumns, boolean isTimeSeries) {
        List<String> measures = new ArrayList<>();
        for (String column : frame.columnNames()) {
            if (column.equals(timeColumnName) && isTimeSeries) {
                continue;
            }
            if (dimensionColumns.contains(column)) {
                continue;
            }
            ColumnType type = frame.columnType(column);
            if (type == ColumnType.LONG || type == ColumnType.DOUBLE) {
                measures.add(column);
            }
        }
        return measures;
    }

    private Map<String, AggregateFunction> resolveAggregations(List<String> measureColumns) {
        Map<String, AggregateFunction> aggregations = new LinkedHashMap<>();
        for (String measure : measureColumns) {
            aggregations.put(measure, resolveAggregateForColumn(measure));
        }
        return aggregations;
    }

    private AggregateFunction resolveAggregateForColumn(String column) {
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

    private long asLong(Object value) {
        return ((Number) value).longValue();
    }

    private double asDouble(Object value) {
        return ((Number) value).doubleValue();
    }

    private Object toComparable(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        return value == null ? null : value.toString();
    }
}
