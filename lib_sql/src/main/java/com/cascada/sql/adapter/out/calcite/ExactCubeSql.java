package com.cascada.sql.adapter.out.calcite;

import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.model.cube.ExactCubeQuery;
import com.cascada.cache.domain.model.cube.ExactCubeLimits;
import com.cascada.cache.domain.model.cube.CubeInterval;
import com.cascada.cache.domain.model.cube.CubeMeasure;
import com.cascada.cache.domain.model.cube.ExactCubeSource;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.Set;

final class ExactCubeSql {
    private ExactCubeSql() {}

    static String direct(ExactCubeQuery query, ExactCubeSource source) {
        return query.pinnedSqlTemplate().replace(query.sourceToken(), relation(source));
    }

    static String state(ExactCubeQuery query, ExactCubeSource source, CubeInterval range, Set<CubeMeasure> measures) {
        var select = new ArrayList<String>();
        var grain = query.grain().stream().map(ExactCubeSql::quote).toList();
        for (int i = 0; i < grain.size(); i++) {
            String key = grain.get(i);
            if (source.types().get(query.grain().get(i)).equals("string")) {
                String bytes = "CAST(" + key + " AS BINARY)";
                key = "CASE WHEN length(" + bytes + ") > " + ExactCubeLimits.MAX_KEY_BYTES
                        + " THEN '!' ELSE hex(" + bytes + ") END";
            }
            select.add(key + " AS cube_k" + i);
        }
        select.add("COUNT(*) AS cube_rows");
        for (var measure : measures.stream().sorted().toList()) {
            select.add(measure.name() + "(" + quote(query.input()) + ") AS " + measure.stateLabel());
        }
        String time = quote(query.timeColumn());
        return "SELECT " + String.join(", ", select) + " FROM " + relation(source)
                + " WHERE " + time + " >= " + range.start() + " AND " + time + " < " + range.end()
                + (query.predicate().isEmpty() ? "" : " AND " + query.predicate())
                + (grain.isEmpty() ? "" : " GROUP BY " + String.join(", ", grain))
                + " LIMIT " + (ExactCubeLimits.MAX_ROWS + 1);
    }

    static String outer(ExactCubeQuery query, ExactCubeSource source, ResultFrame frame) {
        // Every expression is generated from a typed cell. Hex keeps data separate from SQL syntax.
        int count = Math.max(1, frame.rowCount());
        var rows = new ArrayList<String>(count);
        var names = query.projections().stream().map(p -> quote(p.label())).toList();
        int length = 0;
        for (int row = 0; row < count; row++) {
            var values = new ArrayList<String>(names.size());
            for (int column = 0; column < names.size(); column++) {
                var projection = query.projections().get(column);
                String type = switch (projection.operation()) {
                    case KEY -> source.types().get(projection.column());
                    case SUM, COUNT -> "bigint";
                    case AVG -> "double";
                };
                Object value = frame.isEmpty() ? null : frame.valueAt(row, column);
                values.add(literal(value, type));
            }
            String tuple = "(" + String.join(", ", values) + ")";
            length += tuple.length();
            if (length > 64 * 1024) throw new IllegalStateException("outer SQL exceeds cube materialization budget");
            rows.add(tuple);
        }
        String derived = "(SELECT * FROM VALUES " + String.join(", ", rows) + " AS cube_values ("
                + String.join(", ", names) + ")" + (frame.isEmpty() ? " WHERE FALSE" : "") + ")";
        return query.outerSql().replace(query.outerRelation(), derived);
    }

    private static String literal(Object value, String type) {
        if (value instanceof String text) {
            return "CAST(X'" + HexFormat.of().formatHex(text.getBytes(StandardCharsets.UTF_8)) + "' AS STRING)";
        }
        // Use an exact, round-trippable numeric string, including Long.MIN_VALUE.
        return "CAST(" + (value == null ? "NULL" : "'" + value + "'") + " AS " + type + ")";
    }

    private static String relation(ExactCubeSource source) {
        return "delta." + quote(source.path()) + " VERSION AS OF " + source.version();
    }

    private static String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }
}
