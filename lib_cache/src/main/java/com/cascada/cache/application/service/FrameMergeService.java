package com.cascada.cache.application.service;

import com.cascada.cache.application.service.merge.AggregateFunctionResolver;
import com.cascada.cache.application.service.merge.AverageReconstructionService;
import com.cascada.cache.application.service.merge.SparkAverage;
import com.cascada.cache.application.service.merge.TypedFrameAccumulator;
import com.cascada.cache.application.service.merge.TypedFrameAggregator;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.frame.ResultFrameBuilder;
import com.cascada.cache.domain.merge.AggregateFunction;
import com.cascada.cache.domain.merge.AverageIngredients;
import com.cascada.cache.domain.query.CanonicalQueryObject;
import com.cascada.cache.domain.query.OrderByClause;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.IntBinaryOperator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Coordinates typed frame aggregation, AVG/composite reconstruction, and deferred post-processing. */
public final class FrameMergeService {

    private static final Logger LOGGER = Logger.getLogger(FrameMergeService.class.getName());

    private static final Pattern AVG_SPEC =
            Pattern.compile("(?i)AVG\\s*\\(\\s*([^)]+?)\\s*\\)(?:\\s+AS\\s+([A-Za-z_][A-Za-z0-9_]*))?");

    private final AverageReconstructionService averageReconstructionService = new AverageReconstructionService();
    private final int fixedStepSeconds;
    private final String timeColumnName;

    public FrameMergeService(int fixedStepSeconds, String timeColumnName) {
        this.fixedStepSeconds = fixedStepSeconds;
        this.timeColumnName = timeColumnName;
    }

    /** Same ordered merge and final reconstruction, without retaining each completed input frame. */
    public IncrementalFrameMerge incremental(CanonicalQueryObject canonicalObject) {
        return new IncrementalFrameMerge(this, canonicalObject);
    }

    public ResultFrame mergeAndReconstruct(List<ResultFrame> frames, CanonicalQueryObject canonicalObject) {
        if (frames.isEmpty()) {
            return ResultFrame.empty();
        }
        // Validate even zero-row partials. A schema mismatch must not be hidden just because the
        // mismatching cached frame happened to contain no rows for this particular bucket.
        TypedFrameAggregator.validateCompatibleSchemas(frames);
        List<ResultFrame> nonEmpty = frames.stream().filter(frame -> !frame.isEmpty()).toList();
        if (nonEmpty.isEmpty()) {
            // Empty Spark results still carry their projected schema. Keep it intact so callers
            // receive the same columns and types as they would from a non-empty merge.
            return finishMergedFrame(frames.get(0), canonicalObject);
        }

        boolean monitor = LOGGER.isLoggable(Level.FINE);
        long started = monitor ? System.nanoTime() : 0L;
        ResultFrame merged = canonicalObject.metadata().isTimeSeries()
                ? mergeTimeSeries(nonEmpty, canonicalObject)
                : mergeGlobalAggregate(nonEmpty, canonicalObject);

        long aggregated = monitor ? System.nanoTime() : 0L;
        ResultFrame result = finishMergedFrame(merged, canonicalObject);
        if (monitor) {
            long finished = System.nanoTime();
            long inputRows = 0;
            for (ResultFrame frame : frames) inputRows += frame.rowCount();
            LOGGER.fine("Cache merge: input_rows=" + inputRows + " output_rows=" + result.rowCount()
                    + " aggregate_ms=" + (aggregated - started) / 1_000_000.0
                    + " reconstruct_and_sort_ms=" + (finished - aggregated) / 1_000_000.0);
        }
        return result;
    }

    TypedFrameAccumulator createAccumulator(ResultFrame schema, CanonicalQueryObject canonicalObject) {
        List<String> dimensions = new ArrayList<>();
        boolean timeSeries = canonicalObject.metadata().isTimeSeries();
        if (timeSeries) dimensions.add(timeColumnName);
        dimensions.addAll(dimensionColumns(schema, canonicalObject, timeSeries));
        int step = timeSeries ? timeSeriesStep(canonicalObject) : fixedStepSeconds;
        return new TypedFrameAccumulator(schema, dimensions, resolvedFunctions(canonicalObject), timeColumnName, step);
    }

    ResultFrame finishMergedFrame(ResultFrame merged, CanonicalQueryObject canonicalObject) {
        List<String> averages = averageAliases(canonicalObject);
        // Cached AVG measures carry their own SUM/COUNT ingredients; the legacy SUM(col)/COUNT(col)
        // folding would otherwise also consume a user's explicit SUM(col) and COUNT(col) columns.
        ResultFrame reconstructed = averages.isEmpty()
                ? reconstructAverages(merged, canonicalObject)
                : SparkAverage.finish(merged, averages);
        ResultFrame composites = reconstructCompositeAliases(reconstructed, canonicalObject);
        return projectOutput(applyPostProcessing(composites, canonicalObject), canonicalObject);
    }

    // --- composite aliases (e.g. SUM(a) + SUM(b) AS total_bytes) ---------------------------------


    /** Evaluates typed arithmetic plans after ingredient aggregation. SQL parsing belongs to lib_sql. */
    private ResultFrame reconstructCompositeAliases(ResultFrame frame, CanonicalQueryObject canonicalObject) {
        Map<String, String> compositeAliases = canonicalObject.metadata().compositeAliases();
        if (compositeAliases.isEmpty()) return frame;
        Map<String, CompositeExpressionNode> formulas = new LinkedHashMap<>();
        compositeAliases.forEach((alias, formula) -> {
            if (!frame.columnNames().contains(alias)) {
                var compiled = canonicalObject.metadata().compositeFormulas().get(alias);
                if (compiled == null) throw new IllegalArgumentException("missing compiled composite formula " + alias);
                formulas.put(alias, CompositeFrameExpression.bind(compiled, frame));
            }
        });
        if (formulas.isEmpty()) return frame;
        ResultFrameBuilder builder = ResultFrame.builder().expectedRows(frame.rowCount());
        for (String column : frame.columnNames()) builder.column(column, frame.columnType(column));
        formulas.forEach((alias, formula) -> builder.column(alias, formula.type()));
        for (int row = 0; row < frame.rowCount(); row++) {
            for (int column = 0; column < frame.columnNames().size(); column++) builder.appendCell(frame, row, column);
            for (CompositeExpressionNode formula : formulas.values()) {
                Number value = formula.value(frame, row);
                if (value == null) builder.appendNull();
                else switch (formula.type()) {
                    case LONG -> builder.appendLong(value.longValue());
                    case DOUBLE -> builder.appendDouble(value.doubleValue());
                    case DECIMAL -> builder.appendDecimal((java.math.BigDecimal) value);
                    default -> throw new IllegalArgumentException("non-numeric composite result");
                }
            }
        }
        return builder.build();
    }

    private ResultFrame projectOutput(ResultFrame frame, CanonicalQueryObject canonical) {
        if (canonical.outputColumns().isEmpty() || canonical.outputColumns().equals(frame.columnNames())) return frame;
        ResultFrameBuilder builder = ResultFrame.builder().expectedRows(frame.rowCount());
        int[] columns = canonical.outputColumns().stream().mapToInt(name -> {
            String resolved = resolveColumn(name, frame.columnNames());
            if (resolved == null) throw new IllegalArgumentException("missing output column " + name);
            return frame.columnIndex(resolved);
        }).toArray();
        for (int index = 0; index < columns.length; index++)
            builder.column(canonical.outputColumns().get(index), frame.columnTypeAt(columns[index]));
        for (int row = 0; row < frame.rowCount(); row++)
            for (int column : columns) builder.appendCell(frame, row, column);
        return builder.build();
    }

    private String resolveColumn(String token, List<String> availableColumns) {
        if (availableColumns.contains(token)) return token;
        String normalizedToken = token.replace("`", "").replace(" ", "").toLowerCase(Locale.ROOT);
        String resolved = null;
        for (String column : availableColumns) {
            if (column.replace("`", "").replace(" ", "").toLowerCase(Locale.ROOT).equals(normalizedToken)) {
                if (resolved != null) throw new IllegalArgumentException("Ambiguous result column: " + token);
                resolved = column;
            }
        }
        return resolved;
    }

    // --- time-series path -----------------------------------------------------------------------

    private ResultFrame mergeTimeSeries(List<ResultFrame> frames, CanonicalQueryObject canonicalObject) {
        List<String> dimensions = new ArrayList<>();
        dimensions.add(timeColumnName);
        dimensions.addAll(dimensionColumns(frames.get(0), canonicalObject, true));
        return typedMerge(frames, canonicalObject, dimensions,
                timeSeriesStep(canonicalObject));
    }

    private int timeSeriesStep(CanonicalQueryObject canonicalObject) {
        // A raw GROUP BY ts is already at epoch-second granularity. Rounding it to the internal
        // storage step merges distinct SQL groups and changes their timestamps and aggregates.
        return canonicalObject.metadata().preserveRawTimeSeries()
                ? 1 : canonicalObject.userStepSeconds().orElse(fixedStepSeconds);
    }

    private ResultFrame mergeGlobalAggregate(List<ResultFrame> frames, CanonicalQueryObject canonicalObject) {
        return typedMerge(frames, canonicalObject, dimensionColumns(frames.get(0), canonicalObject, false), fixedStepSeconds);
    }

    private ResultFrame typedMerge(List<ResultFrame> frames, CanonicalQueryObject canonicalObject,
                                   List<String> dimensions, int step) {
        return TypedFrameAggregator.aggregate(frames, dimensions, resolvedFunctions(canonicalObject), timeColumnName, step);
    }

    private Map<String, AggregateFunction> resolvedFunctions(CanonicalQueryObject canonicalObject) {
        Map<String, AggregateFunction> functions = new LinkedHashMap<>(
                AggregateFunctionResolver.fromAggregateSpecs(canonicalObject.metadata().aggregateSpecs()));
        canonicalObject.metadata().measureAggregates().forEach((column, function) ->
                functions.put(normalizedName(column), function));
        for (String alias : averageAliases(canonicalObject)) {
            functions.put(normalizedName(AverageIngredients.sumColumn(alias)), AggregateFunction.SUM);
            functions.put(normalizedName(AverageIngredients.countColumn(alias)), AggregateFunction.COUNT);
            functions.put(normalizedName(AverageIngredients.typeColumn(alias)), AggregateFunction.MINIMUM);
        }
        return functions;
    }

    private static List<String> averageAliases(CanonicalQueryObject canonicalObject) {
        List<String> aliases = new ArrayList<>();
        canonicalObject.metadata().measureAggregates().forEach((column, function) -> {
            if (function == AggregateFunction.AVERAGE) aliases.add(column);
        });
        return aliases;
    }

    private static String normalizedName(String column) {
        return column.replace("`", "").replaceAll("\\s+", "").toUpperCase(Locale.ROOT);
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

        java.util.Set<String> sumCountColumnsToDrop = new java.util.HashSet<>();
        Map<String, int[]> ingredients = new LinkedHashMap<>();
        for (var entry : averageAliasToColumn.entrySet()) {
            String sum = "SUM(" + entry.getValue() + ")", count = "COUNT(" + entry.getValue() + ")";
            if (frame.columnNames().contains(sum) && frame.columnNames().contains(count)) {
                ingredients.put(entry.getKey(), new int[]{frame.columnIndex(sum), frame.columnIndex(count)});
                sumCountColumnsToDrop.add(sum);
                sumCountColumnsToDrop.add(count);
            }
        }

        // Re-project, dropping the SUM/COUNT ingredients now folded into the AVG alias.
        List<String> finalColumns = new ArrayList<>(frame.columnNames());
        finalColumns.addAll(averageAliasToColumn.keySet());
        finalColumns.removeIf(sumCountColumnsToDrop::contains);

        ResultFrameBuilder finalBuilder = ResultFrame.builder().expectedRows(frame.rowCount());
        for (String column : finalColumns) {
            ColumnType type = averageAliasToColumn.containsKey(column) ? ColumnType.DOUBLE : frame.columnType(column);
            finalBuilder.column(column, type);
        }
        int[] sourceColumns = finalColumns.stream().mapToInt(column ->
                frame.columnNames().contains(column) ? frame.columnIndex(column) : -1).toArray();
        for (int row = 0; row < frame.rowCount(); row++) {
            for (int column = 0; column < finalColumns.size(); column++) {
                int[] pair = ingredients.get(finalColumns.get(column));
                if (pair != null) {
                    if (frame.isNullAt(row, pair[1])) {
                        finalBuilder.appendNull();
                        continue;
                    }
                    long count = frame.columnTypeAt(pair[1]) == ColumnType.LONG
                            ? frame.longAt(row, pair[1]) : Math.round(asDouble(frame.valueAt(row, pair[1])));
                    if (count < 0) throw new IllegalArgumentException("AVG count ingredient cannot be negative");
                    if (count == 0 || frame.isNullAt(row, pair[0])) finalBuilder.appendNull();
                    else finalBuilder.appendDouble(averageReconstructionService.reconstructAverageFromStoredSumAndCount(
                            asDouble(frame.valueAt(row, pair[0])), count));
                } else if (sourceColumns[column] >= 0) {
                    finalBuilder.appendCell(frame, row, sourceColumns[column]);
                } else finalBuilder.appendNull();
            }
        }
        return finalBuilder.build();
    }

    // --- deferred ORDER BY / LIMIT ---------------------------------------------------------------

    private ResultFrame applyPostProcessing(ResultFrame frame, CanonicalQueryObject canonicalObject) {
        if (frame.isEmpty()) {
            return frame;
        }
        List<OrderByClause> orderBy = canonicalObject.postProcessing().orderBy();
        if (orderBy.isEmpty() && canonicalObject.postProcessing().limit().isEmpty()) {
            // The merge already emits its deterministic group order. Rebuilding every row here when
            // there is no deferred SQL clause only adds map allocations and a second frame copy.
            return frame;
        }
        int limit = Math.min(frame.rowCount(), canonicalObject.postProcessing().limit().orElse(frame.rowCount()));
        int[] sortColumns = new int[orderBy.size()];
        for (int index = 0; index < orderBy.size(); index++) {
            OrderByClause clause = orderBy.get(index);
            if (clause.column().isEmpty()) {
                throw new IllegalArgumentException("expression ordering requires direct execution");
            }
            String column = clause.column().get();
            String resolved = frame.columnNames().contains(column) ? column : resolveColumn(column, frame.columnNames());
            if (resolved == null) {
                throw new IllegalArgumentException("ORDER BY column is absent from the result: " + column);
            }
            sortColumns[index] = frame.columnIndex(resolved);
        }
        IntBinaryOperator comparator = (left, right) -> {
            for (int index = 0; index < orderBy.size(); index++) {
                OrderByClause clause = orderBy.get(index);
                int column = sortColumns[index];
                boolean aNull = column < 0 || frame.isNullAt(left, column);
                boolean bNull = column < 0 || frame.isNullAt(right, column);
                int compared;
                if (aNull || bNull) {
                    compared = aNull == bNull ? 0 : (aNull ? -1 : 1) * (clause.nullsFirst() ? 1 : -1);
                } else {
                    compared = switch (frame.columnTypeAt(column)) {
                        case LONG -> Long.compare(frame.longAt(left, column), frame.longAt(right, column));
                        case DOUBLE -> Double.compare(frame.doubleAt(left, column), frame.doubleAt(right, column));
                        case STRING -> TypedFrameAggregator.compareSparkStrings(
                                frame.stringAt(left, column), frame.stringAt(right, column));
                        case DECIMAL -> ((java.math.BigDecimal) frame.valueAt(left, column))
                                .compareTo((java.math.BigDecimal) frame.valueAt(right, column));
                    };
                    if (!clause.ascending()) compared = -compared;
                }
                if (compared != 0) return compared;
            }
            return Integer.compare(left, right); // Stable SQL ties retain merge group order.
        };
        int[] rows = new int[limit];
        if (limit > 0 && !orderBy.isEmpty() && limit < frame.rowCount()) {
            // ORDER BY LIMIT needs only the best K rows, rather than sorting the entire result.
            int size = 0;
            for (int row = 0; row < frame.rowCount(); row++) {
                if (size < limit) {
                    int slot = size++;
                    while (slot > 0) {
                        int parent = (slot - 1) >>> 1;
                        if (comparator.applyAsInt(row, rows[parent]) <= 0) break;
                        rows[slot] = rows[parent];
                        slot = parent;
                    }
                    rows[slot] = row;
                } else if (comparator.applyAsInt(row, rows[0]) < 0) {
                    int slot = 0;
                    while (slot < size / 2) {
                        int child = slot * 2 + 1;
                        if (child + 1 < size && comparator.applyAsInt(rows[child + 1], rows[child]) > 0) child++;
                        if (comparator.applyAsInt(row, rows[child]) >= 0) break;
                        rows[slot] = rows[child];
                        slot = child;
                    }
                    rows[slot] = row;
                }
            }
            sortRows(rows, comparator);
        } else if (limit > 0) {
            for (int row = 0; row < limit; row++) rows[row] = row;
            if (!orderBy.isEmpty()) sortRows(rows, comparator);
        }

        ResultFrameBuilder builder = ResultFrame.builder().expectedRows(limit);
        for (String column : frame.columnNames()) {
            builder.column(column, frame.columnType(column));
        }
        for (int row : rows) {
            for (int column = 0; column < frame.columnNames().size(); column++) builder.appendCell(frame, row, column);
        }
        return builder.build();
    }

    /** Stable primitive-index merge sort; avoids one boxed index or row adapter per output row. */
    private static void sortRows(int[] rows, IntBinaryOperator comparator) {
        if (rows.length < 2) return;
        int[] source = rows, target = new int[rows.length];
        for (long width = 1; width < rows.length; width *= 2) {
            for (long start = 0; start < rows.length; start += width * 2) {
                int left = (int) start, middle = (int) Math.min(start + width, rows.length);
                int right = middle, end = (int) Math.min(start + width * 2, rows.length), out = left;
                while (left < middle && right < end) {
                    target[out++] = comparator.applyAsInt(source[left], source[right]) <= 0 ? source[left++] : source[right++];
                }
                while (left < middle) target[out++] = source[left++];
                while (right < end) target[out++] = source[right++];
            }
            int[] swap = source; source = target; target = swap;
        }
        if (source != rows) System.arraycopy(source, 0, rows, 0, rows.length);
    }

    // --- helpers ---------------------------------------------------------------------------------

    private List<String> dimensionColumns(ResultFrame frame, CanonicalQueryObject canonicalObject,
                                          boolean isTimeSeries) {
        List<String> dimensions = new ArrayList<>();
        for (String groupByColumn : canonicalObject.hashComponents().groupBy()) {
            String resolved = resolveColumn(groupByColumn, frame.columnNames());
            if (resolved != null && !(isTimeSeries && resolved.equalsIgnoreCase(timeColumnName))) {
                dimensions.add(resolved);
            }
        }
        return dimensions;
    }

    private double asDouble(Object value) {
        return ((Number) value).doubleValue();
    }
}
