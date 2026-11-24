package com.cascada.cache.application.service.cube;

import com.cascada.cache.application.execution.QueryCancellation;
import com.cascada.cache.application.service.merge.SparkAverage;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.model.cube.ExactCubeQuery;
import com.cascada.cache.domain.model.cube.CubeMeasure;
import com.cascada.cache.domain.model.cube.CubeKeyFilter;
import com.cascada.cache.domain.model.cube.ExactCubeSource;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/** Checked integral aggregate state. String keys use Spark's binary UTF-8 bytes. */
final class ExactCubeState {
    // INT_MIN * this bound fits in both LONG and the exact integer range of IEEE-754 DOUBLE.
    private static final long MAX_PRESENT = 1L << 22;
    private final ExactCubeQuery query;
    private final ExactCubeSource source;
    private final Map<CubeMeasure, Map<List<Object>, CubeAccumulator>> measures = new EnumMap<>(CubeMeasure.class);

    ExactCubeState(ExactCubeQuery query, ExactCubeSource source) {
        this.query = query;
        this.source = source;
        query.measures().forEach(measure -> measures.put(measure, new HashMap<>()));
    }

    void add(ResultFrame frame, List<String> grain, Set<CubeMeasure> columns, Set<CubeMeasure> use) {
        add(frame, grain, columns, use, List.of());
    }

    void add(ResultFrame frame, List<String> grain, Set<CubeMeasure> columns, Set<CubeMeasure> use,
             List<CubeKeyFilter> filters) {
        var expected = new ArrayList<String>();
        for (int i = 0; i < grain.size(); i++) {
            expected.add("cube_k" + i);
            require(frame.columnNames().size() > i && frame.columnTypeAt(i) == keyType(grain.get(i)));
        }
        expected.add("cube_rows");
        columns.stream().sorted().map(CubeMeasure::stateLabel).forEach(expected::add);
        require(frame.columnNames().equals(expected));
        for (int i = grain.size(); i < expected.size(); i++) require(frame.columnTypeAt(i) == ColumnType.LONG);
        require(!grain.isEmpty() || frame.rowCount() == 1);
        int[] keyIndexes = query.grain().stream().mapToInt(grain::indexOf).toArray();
        var used = use.toArray(CubeMeasure[]::new);
        int[] measureIndexes = Arrays.stream(used).mapToInt(measure -> frame.columnIndex(measure.stateLabel())).toArray();
        int[] filterIndexes = filters.stream().mapToInt(filter -> grain.indexOf(filter.column())).toArray();
        var filterValues = filters.stream().map(filter -> filter.literal() == null ? null
                : HexFormat.of().withUpperCase().formatHex(filter.literal().getBytes(StandardCharsets.UTF_8))).toList();
        Set<List<Object>> fineKeys = new HashSet<>();
        for (int row = 0; row < frame.rowCount(); row++) {
            QueryCancellation.requireCurrentActive();
            Object[] fineKey = new Object[grain.size()];
            for (int k = 0; k < fineKey.length; k++) {
                Object value = frame.valueAt(row, k);
                if (value instanceof String string) validateHex(string);
                fineKey[k] = value;
            }
            require(fineKeys.add(Arrays.asList(fineKey)));
            boolean include = true;
            for (int i = 0; i < filterIndexes.length && include; i++) {
                Object value = fineKey[filterIndexes[i]];
                include = switch (filters.get(i).operator()) {
                    case STRING_EQUALS -> filterValues.get(i).equals(value);
                    case IS_NULL -> value == null;
                    case IS_NOT_NULL -> value != null;
                };
            }
            if (!include) continue;
            Object[] coarseKey = new Object[keyIndexes.length];
            for (int k = 0; k < coarseKey.length; k++) coarseKey[k] = fineKey[keyIndexes[k]];
            List<Object> key = Arrays.asList(coarseKey);
            require(!frame.isNullAt(row, grain.size()));
            long present = frame.longAt(row, grain.size());
            require(present >= (grain.isEmpty() ? 0 : 1) && present <= MAX_PRESENT);

            for (int i = 0; i < used.length; i++) {
                CubeMeasure measure = used[i];
                int column = measureIndexes[i];
                boolean hasValue = !frame.isNullAt(row, column);
                long value = hasValue ? frame.longAt(row, column) : 0;
                if (measure == CubeMeasure.COUNT) {
                    require(hasValue && value >= 0 && value <= present);
                } else if (hasValue) {
                    long bound = present << 31;
                    require(present > 0 && value >= -bound && value <= bound);
                }
                Map<List<Object>, CubeAccumulator> groups = measures.get(measure);
                var aggregate = groups.computeIfAbsent(key, ignored -> new CubeAccumulator());
                aggregate.presence += present;
                require(aggregate.presence <= MAX_PRESENT);
                aggregate.value = Math.addExact(aggregate.value, value);
                aggregate.nonNull |= hasValue;
            }
        }
    }

    ResultFrame finish() {
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
                    || sum.presence == count.presence && sum.nonNull == (count.value != 0));
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
                builder.appendValue(value instanceof String string ? decode(string) : value);
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
                case String value -> value.compareTo((String) b);
                default -> throw new IllegalStateException("unsupported cube sort key");
            };
            if (result != 0) return sort.descending() ? -result : result;
        }
        return 0;
    }

    private ColumnType keyType(String column) {
        return source.types().get(column).equals("string") ? ColumnType.STRING : ColumnType.LONG;
    }

    private static void validateHex(String value) {
        require((value.length() & 1) == 0);
        for (int i = 0; i < value.length(); i++) {
            char digit = value.charAt(i);
            require(digit >= '0' && digit <= '9' || digit >= 'A' && digit <= 'F');
        }
    }

    private static String decode(String hex) {
        try {
            return StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(HexFormat.of().parseHex(hex))).toString();
        } catch (CharacterCodingException | IllegalArgumentException invalid) {
            throw new IllegalStateException("invalid or oversized cube key", invalid);
        }
    }

    private static void require(boolean condition) {
        if (!condition) throw new IllegalStateException("cube state violates its typed completeness or bounds contract");
    }
}
