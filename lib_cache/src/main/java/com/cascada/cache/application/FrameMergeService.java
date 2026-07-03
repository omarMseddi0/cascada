package com.cascada.cache.application;

import com.cascada.cache.domain.CanonicalQueryObject;
import com.cascada.cache.domain.OrderByClause;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.merge.AggregateFunction;
import com.cascada.cache.domain.merge.AggregateFunctionResolver;
import com.cascada.cache.domain.merge.AverageReconstructionService;
import com.cascada.cache.domain.merge.columnar.ColumnarHashAggregator;
import com.cascada.cache.domain.merge.columnar.ColumnarHashAggregator.GroupedResult;
import com.cascada.cache.domain.merge.columnar.DictionaryEncoder;

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
 *
 * <p>Both merge strategies extract each frame straight into columnar primitive arrays (dictionary
 * codes + {@code double[]}) and run {@link ColumnarHashAggregator}, the shared vectorized group-by —
 * no per-row map objects exist anywhere on the hot path.
 */
public final class FrameMergeService {

    private static final Pattern AVG_SPEC =
            Pattern.compile("(?i)AVG\\s*\\(\\s*([^)]+?)\\s*\\)(?:\\s+AS\\s+([A-Za-z_][A-Za-z0-9_]*))?");

    private final ColumnarHashAggregator aggregator = new ColumnarHashAggregator();
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
        ResultFrame withComposites = reconstructCompositeAliases(reconstructed, canonicalObject);
        return applyPostProcessing(withComposites, canonicalObject);
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

        ResultFrame.Builder builder = ResultFrame.builder();
        for (String column : frame.columnNames()) {
            builder.column(column, frame.columnType(column));
        }
        resolved.keySet().forEach(alias -> builder.column(alias, ColumnType.DOUBLE));

        for (Map<String, Object> row : frame.rows()) {
            Map<String, Object> rebuilt = new LinkedHashMap<>(row);
            resolved.forEach((alias, terms) -> {
                double value = 0.0;
                for (CompositeTerm term : terms) {
                    double termValue = asDouble(row.get(term.columnReference()));
                    value += term.add() ? termValue : -termValue;
                }
                rebuilt.put(alias, value);
            });
            builder.row(rebuilt);
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
        List<String> dimensionColumns = dimensionColumns(frames.get(0), canonicalObject, true);
        List<String> measureColumns = measureColumns(frames.get(0), canonicalObject, dimensionColumns, true);

        // Extract every frame straight into columnar primitives — one dictionary lookup per cell,
        // no row objects.
        int rowCount = totalRowCount(frames);
        long[] fixedBuckets = new long[rowCount];
        DictionaryEncoder encoder = new DictionaryEncoder();
        int[][] dimensionCodes = new int[dimensionColumns.size()][rowCount];
        double[][] measures = new double[measureColumns.size()][rowCount];
        int r = 0;
        for (ResultFrame frame : frames) {
            for (Map<String, Object> row : frame.rows()) {
                fixedBuckets[r] = asLong(row.get(timeColumnName));
                for (int d = 0; d < dimensionCodes.length; d++) {
                    dimensionCodes[d][r] = encoder.encode(String.valueOf(row.get(dimensionColumns.get(d))));
                }
                for (int m = 0; m < measures.length; m++) {
                    measures[m][r] = asDouble(row.get(measureColumns.get(m)));
                }
                r++;
            }
        }

        // RC3 guard: exact duplicates at the fixed step (same bucket, dimensions AND measures).
        boolean[] keep = aggregator.deduplicateExactRows(rowCount, fixedBuckets, dimensionCodes, measures);

        ResultFrame.Builder builder = ResultFrame.builder().column(timeColumnName, ColumnType.LONG);
        dimensionColumns.forEach(dimension -> builder.column(dimension, ColumnType.STRING));
        measureColumns.forEach(measure -> builder.column(measure, ColumnType.DOUBLE));

        int userStep = canonicalObject.userStepSeconds().orElse(fixedStepSeconds);
        if (userStep <= fixedStepSeconds) {
            // RC1: never re-derive a finer step — emit the surviving rows unchanged, in arrival order.
            for (int row = 0; row < rowCount; row++) {
                if (keep[row]) {
                    builder.row(rowValues(fixedBuckets[row], dimensionColumns, dimensionCodes,
                            measureColumns, measures, row, encoder));
                }
            }
            return builder.build();
        }

        // RC2/RC5: re-bucket on epoch-second longs, then roll up with the shared columnar group-by.
        long[] userBuckets = new long[rowCount];
        for (int row = 0; row < rowCount; row++) {
            userBuckets[row] = Math.floorDiv(fixedBuckets[row], userStep) * (long) userStep;
        }
        GroupedResult grouped = aggregator.aggregate(rowCount, userBuckets, dimensionCodes, measures,
                aggregateFunctions(measureColumns, canonicalObject), keep);

        int[] order = sortedGroupOrder(grouped, dimensionColumns, encoder, true);
        for (int g : order) {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put(timeColumnName, grouped.groupBuckets()[g]);
            for (int d = 0; d < dimensionColumns.size(); d++) {
                values.put(dimensionColumns.get(d), encoder.decode(grouped.dimensionCode(g, d)));
            }
            for (int m = 0; m < measureColumns.size(); m++) {
                values.put(measureColumns.get(m), grouped.measureAccumulators()[m][g]);
            }
            builder.row(values);
        }
        return builder.build();
    }

    // --- global-aggregate path ------------------------------------------------------------------

    private ResultFrame mergeGlobalAggregate(List<ResultFrame> frames, CanonicalQueryObject canonicalObject) {
        List<String> dimensionColumns = dimensionColumns(frames.get(0), canonicalObject, false);
        List<String> measureColumns = measureColumns(frames.get(0), canonicalObject, dimensionColumns, false);

        // RC (Bug #3 in merging.py): floor the time column by the fixed step BEFORE grouping, so a
        // cached bucket row (already aligned) and a Spark gap row in the same fixed-step window collapse
        // to the same group key instead of being counted as two distinct groups. Only when the time
        // column is itself a grouped dimension here.
        boolean floorTime = dimensionColumns.contains(timeColumnName);

        int rowCount = totalRowCount(frames);
        DictionaryEncoder encoder = new DictionaryEncoder();
        int[][] dimensionCodes = new int[dimensionColumns.size()][rowCount];
        double[][] measures = new double[measureColumns.size()][rowCount];
        int r = 0;
        for (ResultFrame frame : frames) {
            for (Map<String, Object> row : frame.rows()) {
                for (int d = 0; d < dimensionCodes.length; d++) {
                    String dimension = dimensionColumns.get(d);
                    if (floorTime && dimension.equals(timeColumnName)) {
                        long flooredTs = Math.floorDiv(asLong(row.get(dimension)), fixedStepSeconds)
                                * (long) fixedStepSeconds;
                        dimensionCodes[d][r] = encoder.encode(String.valueOf(flooredTs));
                    } else {
                        dimensionCodes[d][r] = encoder.encode(String.valueOf(row.get(dimension)));
                    }
                }
                for (int m = 0; m < measures.length; m++) {
                    measures[m][r] = asDouble(row.get(measureColumns.get(m)));
                }
                r++;
            }
        }

        // RC3 dedup on (dimensions incl. floored time, measures), then the columnar group-by.
        boolean[] keep = aggregator.deduplicateExactRows(rowCount, null, dimensionCodes, measures);
        GroupedResult grouped = aggregator.aggregate(rowCount, null, dimensionCodes, measures,
                aggregateFunctions(measureColumns, canonicalObject), keep);

        ResultFrame.Builder builder = ResultFrame.builder();
        dimensionColumns.forEach(dimension -> builder.column(dimension, ColumnType.STRING));
        measureColumns.forEach(measure -> builder.column(measure, ColumnType.DOUBLE));
        int[] order = sortedGroupOrder(grouped, dimensionColumns, encoder, false);
        for (int g : order) {
            Map<String, Object> values = new LinkedHashMap<>();
            for (int d = 0; d < dimensionColumns.size(); d++) {
                values.put(dimensionColumns.get(d), encoder.decode(grouped.dimensionCode(g, d)));
            }
            for (int m = 0; m < measureColumns.size(); m++) {
                values.put(measureColumns.get(m), grouped.measureAccumulators()[m][g]);
            }
            builder.row(values);
        }
        return builder.build();
    }

    // --- columnar helpers -------------------------------------------------------------------------

    private int totalRowCount(List<ResultFrame> frames) {
        int total = 0;
        for (ResultFrame frame : frames) {
            total += frame.rowCount();
        }
        return total;
    }

    /**
     * The combine op per measure column. The parsed map from canonicalization is authoritative
     * (README caveat 4: {@code MAX(latency) AS peak_latency} must combine as MAX, but the alias hides
     * the {@code max} signal from any name heuristic). When the canonical object predates the map —
     * or a column, like AVG's decomposed SUM/COUNT ingredients, is not in it — we derive a second map
     * from the aggregate specs and only then fall back to name sniffing, which for unaliased columns
     * is exactly the historical behaviour.
     */
    private AggregateFunction[] aggregateFunctions(List<String> measureColumns,
                                                   CanonicalQueryObject canonicalObject) {
        Map<String, AggregateFunction> parsed =
                new LinkedHashMap<>(AggregateFunctionResolver.fromAggregateSpecs(
                        canonicalObject.metadata().aggregateSpecs()));
        canonicalObject.metadata().measureAggregates()
                .forEach((column, function) -> parsed.put(
                        column.replace("`", "").replaceAll("\\s+", "").toUpperCase(Locale.ROOT), function));
        AggregateFunction[] functions = new AggregateFunction[measureColumns.size()];
        for (int m = 0; m < measureColumns.size(); m++) {
            functions[m] = AggregateFunctionResolver.resolve(measureColumns.get(m), parsed);
        }
        return functions;
    }

    private Map<String, Object> rowValues(long bucket, List<String> dimensionColumns, int[][] dimensionCodes,
                                          List<String> measureColumns, double[][] measures, int row,
                                          DictionaryEncoder encoder) {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put(timeColumnName, bucket);
        for (int d = 0; d < dimensionColumns.size(); d++) {
            values.put(dimensionColumns.get(d), encoder.decode(dimensionCodes[d][row]));
        }
        for (int m = 0; m < measureColumns.size(); m++) {
            values.put(measureColumns.get(m), measures[m][row]);
        }
        return values;
    }

    /**
     * Deterministic output order, matching the previous row-object implementation: bucket start
     * first (time series only), then the canonical grouping-key string — dimension {@code name=value}
     * pairs in sorted-name order, exactly as a {@code TreeMap#toString()} rendered them.
     */
    private int[] sortedGroupOrder(GroupedResult grouped, List<String> dimensionColumns,
                                   DictionaryEncoder encoder, boolean byBucketFirst) {
        int groupCount = grouped.groupCount();
        Integer[] sortedDimensionIndexes = new Integer[dimensionColumns.size()];
        for (int d = 0; d < sortedDimensionIndexes.length; d++) {
            sortedDimensionIndexes[d] = d;
        }
        java.util.Arrays.sort(sortedDimensionIndexes, Comparator.comparing(dimensionColumns::get));

        String[] sortKeys = new String[groupCount];
        for (int g = 0; g < groupCount; g++) {
            StringBuilder key = new StringBuilder("{");
            for (int i = 0; i < sortedDimensionIndexes.length; i++) {
                int d = sortedDimensionIndexes[i];
                if (i > 0) {
                    key.append(", ");
                }
                key.append(dimensionColumns.get(d)).append('=')
                        .append(encoder.decode(grouped.dimensionCode(g, d)));
            }
            sortKeys[g] = key.append('}').toString();
        }

        Integer[] order = new Integer[groupCount];
        for (int g = 0; g < groupCount; g++) {
            order[g] = g;
        }
        Comparator<Integer> comparator = byBucketFirst
                ? Comparator.<Integer>comparingLong(g -> grouped.groupBuckets()[g])
                        .thenComparing(g -> sortKeys[g])
                : Comparator.comparing(g -> sortKeys[g]);
        java.util.Arrays.sort(order, comparator);
        int[] primitive = new int[groupCount];
        for (int g = 0; g < groupCount; g++) {
            primitive[g] = order[g];
        }
        return primitive;
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
                    long count = Math.round(asDouble(row.get(countColumn)));
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
            // O(1) truncation instead of removing tail rows one-by-one (O(n*k)); mirrors df.head(limit).
            if (rows.size() > limit) {
                rows.subList(limit, rows.size()).clear();
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

    private long asLong(Object value) {
        if (!(value instanceof Number number)) {
            throw new IllegalStateException(
                    "expected a numeric time-bucket value but got "
                            + (value == null ? "null" : value.getClass().getSimpleName())
                            + " — a frame is missing or corrupting its time column");
        }
        return number.longValue();
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
