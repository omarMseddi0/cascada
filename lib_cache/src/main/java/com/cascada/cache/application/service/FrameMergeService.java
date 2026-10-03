package com.cascada.cache.application.service;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.merge.AggregateFunction;
import com.cascada.cache.domain.merge.AggregateFunctionResolver;
import com.cascada.cache.domain.merge.AverageReconstructionService;
import com.cascada.cache.domain.merge.TypedFrameAggregator;
import com.cascada.cache.domain.query.CanonicalQueryObject;
import com.cascada.cache.domain.query.OrderByClause;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.IntBinaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.logging.Level;
import java.util.logging.Logger;

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
    public IncrementalMerge incremental(CanonicalQueryObject canonicalObject) {
        return new IncrementalMerge(canonicalObject);
    }

    public final class IncrementalMerge {
        private final CanonicalQueryObject canonical;
        private ResultFrame schema;
        private ResultFrame firstEmpty;
        private TypedFrameAggregator.Accumulator accumulator;
        private RuntimeException failure;
        private long inputRows;

        private IncrementalMerge(CanonicalQueryObject canonical) { this.canonical = canonical; }

        /** Deferred merge errors preserve authoritative fallback after the original fetch/query phases. */
        public void add(ResultFrame frame) {
            if (failure != null) return;
            try {
                java.util.Objects.requireNonNull(frame, "frame");
                if (schema == null) {
                    ResultFrame.Builder descriptor = ResultFrame.builder();
                    for (String column : frame.columnNames()) descriptor.column(column, frame.columnType(column));
                    schema = descriptor.build();
                    if (frame.isEmpty()) firstEmpty = frame;
                } else TypedFrameAggregator.validateCompatibleSchemas(List.of(schema, frame));
                inputRows += frame.rowCount();
                if (frame.isEmpty()) return;
                if (accumulator == null) {
                    List<String> dimensions = new ArrayList<>();
                    boolean timeSeries = canonical.metadata().isTimeSeries();
                    if (timeSeries) dimensions.add(timeColumnName);
                    dimensions.addAll(dimensionColumns(schema, canonical, timeSeries));
                    int step = timeSeries ? canonical.userStepSeconds().orElse(fixedStepSeconds) : fixedStepSeconds;
                    accumulator = new TypedFrameAggregator.Accumulator(schema, dimensions,
                            resolvedFunctions(canonical), timeColumnName, step);
                }
                accumulator.accept(frame);
            } catch (RuntimeException invalidMerge) { failure = invalidMerge; }
        }

        public ResultFrame finish() {
            if (failure != null) throw failure;
            if (schema == null) return ResultFrame.empty();
            if (accumulator == null) return firstEmpty == null ? schema : firstEmpty;
            boolean monitor = LOGGER.isLoggable(Level.FINE);
            long started = monitor ? System.nanoTime() : 0L;
            ResultFrame merged = accumulator.finish();
            ResultFrame reconstructed = reconstructAverages(merged, canonical);
            ResultFrame composites = reconstructCompositeAliases(reconstructed, canonical);
            ResultFrame answer = applyPostProcessing(composites, canonical);
            if (monitor) LOGGER.fine("Incremental cache merge: input_rows=" + inputRows
                    + " output_rows=" + answer.rowCount() + " finish_ms=" + (System.nanoTime() - started) / 1_000_000.0);
            return answer;
        }
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
            return frames.get(0);
        }

        boolean monitor = LOGGER.isLoggable(Level.FINE);
        long started = monitor ? System.nanoTime() : 0L;
        ResultFrame merged = canonicalObject.metadata().isTimeSeries()
                ? mergeTimeSeries(nonEmpty, canonicalObject)
                : mergeGlobalAggregate(nonEmpty, canonicalObject);

        long aggregated = monitor ? System.nanoTime() : 0L;
        ResultFrame reconstructed = reconstructAverages(merged, canonicalObject);
        ResultFrame withComposites = reconstructCompositeAliases(reconstructed, canonicalObject);
        ResultFrame result = applyPostProcessing(withComposites, canonicalObject);
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

    // --- composite aliases (e.g. SUM(a) + SUM(b) AS total_bytes) ---------------------------------

    /** A composite-alias term: an aggregate column reference and the +/- sign it is combined with. */
    private record CompositeTerm(String columnReference, boolean add) {
    }

    private static final Pattern COMPOSITE_TERM =
            Pattern.compile("\\s*([+-])?\\s*((?:SUM|COUNT|MIN|MAX|AVG)\\s*\\([^)]*\\)|[A-Za-z_][A-Za-z0-9_]*)",
                    Pattern.CASE_INSENSITIVE);

    /**
     * Rebuilds composite aliases after the merge, porting {@code reconstruct_composite_aliases} in
     * {@code merging.py}. Spark/the cache stores the raw ingredient columns (e.g. {@code SUM(a)},
     * {@code SUM(b)}); the canonical object carries the formula (e.g. {@code SUM(a) + SUM(b)}) and the
     * alias (e.g. {@code total_bytes}). Without this, a query like {@code SUM(a)+SUM(b) AS total_bytes}
     * would come back from cache WITHOUT the {@code total_bytes} column (the KeyError the Python fix
     * targets). Only additive composites ({@code +}/{@code -} of aggregate-of-column terms) are
     * reconstructed in memory; anything more exotic was never cacheable (it would have bypassed).
     */
    private ResultFrame reconstructCompositeAliases(ResultFrame frame, CanonicalQueryObject canonicalObject) {
        Map<String, String> compositeAliases = canonicalObject.metadata().compositeAliases();
        if (compositeAliases.isEmpty() || frame.isEmpty()) {
            return frame;
        }

        // Resolve each alias's formula into terms whose column actually exists in the frame.
        Map<String, List<CompositeTerm>> resolved = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : compositeAliases.entrySet()) {
            String alias = entry.getKey();
            if (frame.columnNames().contains(alias)) {
                continue; // already present (Bug #5 guard from merging.py)
            }
            List<CompositeTerm> terms = parseCompositeFormula(entry.getValue(), frame.columnNames());
            if (terms != null) {
                resolved.put(alias, terms);
            }
        }
        if (resolved.isEmpty()) {
            return frame;
        }

        ResultFrame.Builder builder = ResultFrame.builder().expectedRows(frame.rowCount());
        for (String column : frame.columnNames()) {
            builder.column(column, frame.columnType(column));
        }
        resolved.keySet().forEach(alias -> builder.column(alias, ColumnType.DOUBLE));

        List<List<CompositeTerm>> formulas = new ArrayList<>(resolved.values());
        int[][] termColumns = formulas.stream().map(terms -> terms.stream()
                .mapToInt(term -> frame.columnIndex(term.columnReference())).toArray()).toArray(int[][]::new);
        for (int row = 0; row < frame.rowCount(); row++) {
            for (int column = 0; column < frame.columnNames().size(); column++) builder.appendCell(frame, row, column);
            for (int formula = 0; formula < formulas.size(); formula++) {
                double value = 0.0;
                List<CompositeTerm> terms = formulas.get(formula);
                for (int termIndex = 0; termIndex < terms.size(); termIndex++) {
                    CompositeTerm term = terms.get(termIndex);
                    double termValue = asDouble(frame.valueAt(row, termColumns[formula][termIndex]));
                    value += term.add() ? termValue : -termValue;
                }
                builder.appendDouble(value);
            }
        }
        return builder.build();
    }

    /**
     * Parse an additive composite formula into terms, mapping each aggregate reference to the actual
     * frame column (case/space-insensitive, matching {@code _spark_formula_to_pandas}). Returns
     * {@code null} if any term cannot be resolved to a present column — then the alias is left absent
     * rather than computed from a wrong column.
     */
    private List<CompositeTerm> parseCompositeFormula(String formula, List<String> availableColumns) {
        List<CompositeTerm> terms = new ArrayList<>();
        Matcher matcher = COMPOSITE_TERM.matcher(formula);
        int matchedTo = 0;
        while (matcher.find()) {
            boolean add = !"-".equals(matcher.group(1));
            String token = matcher.group(2);
            String resolvedColumn = resolveColumn(token, availableColumns);
            if (resolvedColumn == null) {
                return null;
            }
            terms.add(new CompositeTerm(resolvedColumn, add));
            matchedTo = matcher.end();
        }
        // Reject anything we did not fully consume (e.g. *, /, parenthesised sub-expressions).
        if (terms.isEmpty() || formula.substring(matchedTo).trim().length() > 0) {
            return null;
        }
        return terms;
    }

    private String resolveColumn(String token, List<String> availableColumns) {
        String normalizedToken = token.replace("`", "").replace(" ", "").toLowerCase(Locale.ROOT);
        for (String column : availableColumns) {
            if (column.replace("`", "").replace(" ", "").toLowerCase(Locale.ROOT).equals(normalizedToken)) {
                return column;
            }
        }
        return null;
    }

    // --- time-series path -----------------------------------------------------------------------

    private ResultFrame mergeTimeSeries(List<ResultFrame> frames, CanonicalQueryObject canonicalObject) {
        List<String> dimensions = new ArrayList<>();
        dimensions.add(timeColumnName);
        dimensions.addAll(dimensionColumns(frames.get(0), canonicalObject, true));
        return typedMerge(frames, canonicalObject, dimensions,
                canonicalObject.userStepSeconds().orElse(fixedStepSeconds));
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
                functions.put(column.replace("`", "").replaceAll("\\s+", "").toUpperCase(Locale.ROOT), function));
        return functions;
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

        ResultFrame.Builder finalBuilder = ResultFrame.builder().expectedRows(frame.rowCount());
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
                    double sum = asDouble(frame.valueAt(row, pair[0]));
                    long count = Math.round(asDouble(frame.valueAt(row, pair[1])));
                    finalBuilder.appendDouble(averageReconstructionService.reconstructAverageFromStoredSumAndCount(sum, count));
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
            sortColumns[index] = frame.columnNames().contains(column) ? frame.columnIndex(column) : -1;
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
                        case STRING -> frame.stringAt(left, column).compareTo(frame.stringAt(right, column));
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

        ResultFrame.Builder builder = ResultFrame.builder().expectedRows(limit);
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
            if (frame.columnNames().contains(groupByColumn)
                    && !(isTimeSeries && groupByColumn.equals(timeColumnName))) {
                dimensions.add(groupByColumn);
            }
        }
        return dimensions;
    }

    private double asDouble(Object value) {
        return ((Number) value).doubleValue();
    }
}
