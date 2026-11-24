package com.cascada.sql.adapter.out.calcite;

import com.cascada.cache.domain.model.cube.ExactCubeQuery;
import com.cascada.cache.domain.model.cube.CubeInterval;
import com.cascada.cache.domain.model.cube.CubeMeasure;
import com.cascada.cache.domain.model.cube.ExactCubeSource;
import java.util.ArrayList;
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
                key = "hex(CAST(" + key + " AS BINARY))";
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
                + (grain.isEmpty() ? "" : " GROUP BY " + String.join(", ", grain));
    }

    static String outer(ExactCubeQuery query, ExactCubeSource source) {
        return query.outerSql().replace(query.sourceToken(), relation(source));
    }

    private static String relation(ExactCubeSource source) {
        return "delta." + quote(source.path()) + " VERSION AS OF " + source.version();
    }

    private static String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }
}
