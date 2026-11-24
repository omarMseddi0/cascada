package com.cascada.cache.application.service.cube;

import com.cascada.cache.application.execution.QueryCancellation;
import com.cascada.cache.application.service.merge.SparkAverage;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.model.cube.ExactCubeQuery;
import com.cascada.cache.domain.model.cube.CubeMeasure;
import com.cascada.cache.domain.model.cube.CubeKeyFilter;
import com.cascada.cache.domain.model.cube.ExactCubeSource;
import com.cascada.cache.application.service.merge.SparkValues;
import com.cascada.cache.application.service.merge.TypedFrameAccumulator;
import com.cascada.cache.domain.merge.AggregateFunction;
import java.util.concurrent.Executor;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/** Checked integral state reduced using the normal bucket merge workers. */
final class ExactCubeState {
    private static final long MAX_PRESENT = 1L << 22;
    private final ExactCubeQuery query;
    private final ExactCubeSource source;
    private final int workers;
    private final Executor mergeExecutor;
    private final List<String> dimensions;
    private final Map<List<CubeMeasure>, TypedFrameAccumulator> accumulators = new HashMap<>();

    ExactCubeState(ExactCubeQuery query, ExactCubeSource source, int workers, Executor mergeExecutor) {
        this.query = query;
        this.source = source;
        this.workers = workers;
        this.mergeExecutor = mergeExecutor;
        this.dimensions = java.util.stream.IntStream.range(0, query.grain().size())
                .mapToObj(index -> "cube_k" + index).toList();
    }

    void add(ResultFrame frame, List<String> grain, List<String> keyColumns, String presenceColumn,
             Map<CubeMeasure, String> valueColumns, List<CubeKeyFilter> filters) {
        require(!frame.hasLossyStrings());
        var sourceKeys = query.grain().stream().map(key -> keyColumns.get(grain.indexOf(key))).toList();
        int[] filterIndexes = filters.stream().mapToInt(filter ->
                frame.columnIndex(keyColumns.get(grain.indexOf(filter.column())))).toArray();
        var measures = valueColumns.keySet().stream().sorted().toList();
        String certificate = "cube_rows";
        var inputs = new ArrayList<>(sourceKeys);
        inputs.add(presenceColumn);
        var outputs = new ArrayList<>(dimensions);
        outputs.add(certificate);
        for (CubeMeasure measure : measures) {
            inputs.add(valueColumns.get(measure));
            outputs.add(measure.stateLabel());
        }
        ResultFrame selected = frame.project(inputs, outputs);
        if (!filters.isEmpty()) {
            long[] integerLiterals = new long[filters.size()];
            for (int i = 0; i < filters.size(); i++) {
                var filter = filters.get(i);
                if (filter.operator().isIntegralComparison()) integerLiterals[i] = Long.parseLong(filter.literal());
            }
            var builder = ResultFrame.builder();
            for (String name : selected.columnNames()) builder.column(name, selected.columnType(name));
            for (int row = 0; row < selected.rowCount(); row++) {
                QueryCancellation.requireCurrentActive();
                boolean keep = true;
                for (int i = 0; i < filters.size() && keep; i++) {
                    var filter = filters.get(i);
                    int column = filterIndexes[i];
                    keep = switch (filter.operator()) {
                        case IS_NULL -> frame.isNullAt(row, column);
                        case IS_NOT_NULL -> !frame.isNullAt(row, column);
                        case STRING_EQUALS -> !frame.isNullAt(row, column)
                                && filter.literal().equals(frame.stringAt(row, column));
                        default -> !frame.isNullAt(row, column)
                                && filter.operator().matchesInteger(frame.longAt(row, column), integerLiterals[i]);
                    };
                }
                if (keep) {
                    for (int column = 0; column < selected.columnNames().size(); column++) {
                        builder.appendCell(selected, row, column);
                    }
                }
            }
            selected = builder.build();
        }
        var accumulator = accumulators.get(measures);
        if (accumulator == null) {
            var functions = new HashMap<String, AggregateFunction>();
            functions.put(certificate, AggregateFunction.SUM);
            measures.forEach(measure -> functions.put(measure.stateLabel(), AggregateFunction.SUM));
            accumulator = new TypedFrameAccumulator(selected, dimensions, functions, null, 1, workers, mergeExecutor);
            accumulators.put(measures, accumulator);
        }
        accumulator.accept(selected);
    }

    ResultFrame finish() {
        Map<CubeMeasure, Map<List<Object>, CubeAccumulator>> measures = new EnumMap<>(CubeMeasure.class);
        for (var entry : accumulators.entrySet()) {
            var merged = entry.getValue().finish();
            int presence = merged.columnIndex("cube_rows");
            for (CubeMeasure measure : entry.getKey()) {
                int value = merged.columnIndex(measure.stateLabel());
                var states = measures.computeIfAbsent(measure, ignored -> new HashMap<>());
                for (int row = 0; row < merged.rowCount(); row++) {
                    var key = new ArrayList<Object>(dimensions.size());
                    for (int column = 0; column < dimensions.size(); column++) key.add(merged.valueAt(row, column));
                    var state = states.computeIfAbsent(key, ignored -> new CubeAccumulator());
                    state.presence = Math.addExact(state.presence, merged.longAt(row, presence));
                    require(state.presence >= 0 && state.presence <= MAX_PRESENT);
                    if (!merged.isNullAt(row, value)) {
                        state.nonNull = true;
                        state.value = Math.addExact(state.value, merged.longAt(row, value));
                    }
                }
            }
        }
        if (query.grain().isEmpty()) {
            for (var groups : measures.values()) groups.computeIfAbsent(List.of(), key -> new CubeAccumulator());
        }
        var groups = measures.values().iterator().next().keySet();
        require(measures.values().stream().allMatch(map -> map.keySet().equals(groups)));
        int size = query.limit() == null ? groups.size() : Math.min(groups.size(), query.limit());
        var top = size > 0 && size <= groups.size() / 4
                ? new PriorityQueue<Object[]>(size, (left, right) -> compare(right, left)) : null;
        var rows = new ArrayList<Object[]>(top == null && size > 0 ? groups.size() : size);
        var sums = measures.get(CubeMeasure.SUM);
        var counts = measures.get(CubeMeasure.COUNT);
        for (var key : groups) {
            QueryCancellation.requireCurrentActive();
            CubeAccumulator sum = sums == null ? null : sums.get(key);
            CubeAccumulator count = counts == null ? null : counts.get(key);
            require(sum == null || count == null
                    || sum.nonNull == (count.value != 0));
            if (size == 0) continue;
            Object[] row = new Object[query.projections().size()];
            for (int i = 0; i < row.length; i++) {
                var projection = query.projections().get(i);
                row[i] = switch (projection.operation()) {
                    case KEY -> key.get(query.grain().indexOf(projection.column()));
                    case SUM -> sum.nonNull ? sum.value : null;
                    case COUNT -> count.value;
                    case AVG -> SparkAverage.average(sum.nonNull ? sum.value : null, count.value);
                };
            }
            if (top == null) {
                rows.add(row);
            } else if (top.size() < size) {
                top.add(row);
            } else if (compare(row, top.element()) < 0) {
                top.remove();
                top.add(row);
            }
        }
        if (top != null) rows.addAll(top);
        if (!query.order().isEmpty()) rows.sort(this::compare);
        var builder = ResultFrame.builder().expectedRows(size);
        for (var projection : query.projections()) {
            ColumnType type = switch (projection.operation()) {
                case KEY -> keyType(projection.column());
                case AVG -> ColumnType.DOUBLE;
                case SUM, COUNT -> ColumnType.LONG;
            };
            builder.column(projection.label(), type);
        }
        for (int row = 0; row < size; row++) {
            for (Object value : rows.get(row)) {
                builder.appendValue(value);
            }
        }
        return builder.build();
    }

    private int compare(Object[] left, Object[] right) {
        for (var sort : query.order()) {
            int column = 0;
            while (!query.projections().get(column).label().equals(sort.label())) column++;
            Object a = left[column];
            Object b = right[column];
            if (a == null && b == null) continue;
            if (a == null) return sort.nullsFirst() ? -1 : 1;
            if (b == null) return sort.nullsFirst() ? 1 : -1;
            int result = switch (a) {
                case Long value -> value.compareTo((Long) b);
                case Double value -> value.compareTo((Double) b);
                case String value -> SparkValues.compareStrings(value, (String) b);
                default -> throw new IllegalStateException("unsupported cube sort key");
            };
            if (result != 0) return sort.descending() ? -result : result;
        }
        return 0;
    }

    private ColumnType keyType(String column) {
        return source.types().get(column).equals("string") ? ColumnType.STRING : ColumnType.LONG;
    }

    private static void require(boolean condition) {
        if (!condition) throw new IllegalStateException("cube state violates its typed completeness or bounds contract");
    }
}
