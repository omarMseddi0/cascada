package com.cascada.cache.application.service.cube;

import com.cascada.cache.application.execution.QueryCancellation;
import com.cascada.cache.application.service.merge.SparkAverage;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.model.cube.ExactCubeQuery;
import com.cascada.cache.domain.model.cube.CubeMeasure;
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
            Object[] coarseKey = new Object[keyIndexes.length];
            for (int k = 0; k < coarseKey.length; k++) coarseKey[k] = fineKey[keyIndexes[k]];
            List<Object> key = Arrays.asList(coarseKey);
            require(!frame.isNullAt(row, grain.size()));
            long present = frame.longAt(row, grain.size());
            require(present >= (grain.isEmpty() ? 0 : 1) && present <= MAX_PRESENT);

            for (CubeMeasure measure : use) {
                int column = frame.columnIndex(measure.stateLabel());
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
        var rows = new ArrayList<Object[]>(groups.size());
        for (var key : groups) {
            var sums = measures.get(CubeMeasure.SUM);
            var counts = measures.get(CubeMeasure.COUNT);
            CubeAccumulator sum = sums == null ? null : sums.get(key);
            CubeAccumulator count = counts == null ? null : counts.get(key);
            require(sum == null || count == null
                    || sum.presence == count.presence && sum.nonNull == (count.value != 0));
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
            rows.add(row);
        }
        if (!query.order().isEmpty()) rows.sort(this::compare);
        int size = query.limit() == null ? rows.size() : Math.min(rows.size(), query.limit());
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
        require(value.equals(value.toUpperCase(java.util.Locale.ROOT)));
        decode(value);
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
