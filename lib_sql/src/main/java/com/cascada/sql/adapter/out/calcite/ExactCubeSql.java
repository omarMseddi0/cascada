package com.cascada.sql.adapter.out.calcite;

import com.cascada.cache.domain.model.cube.ExactCubeQuery;
import com.cascada.cache.domain.model.cube.CubeInterval;
import com.cascada.cache.domain.model.cube.CubeMeasure;
import java.util.ArrayList;
import java.util.Set;

final class ExactCubeSql {
    private ExactCubeSql() {}

    static String state(ExactCubeQuery query, CubeInterval range, Set<CubeMeasure> measures) {
        var select = new ArrayList<String>();
        var grain = query.grain().stream().map(ExactCubeSql::quote).toList();
        for (int i = 0; i < grain.size(); i++) {
            String key = grain.get(i);
            select.add(key + " AS cube_k" + i);
        }
        select.add("COUNT(*) AS cube_rows");
        for (var measure : measures.stream().sorted().toList()) {
            select.add(measure.name() + "(" + quote(query.input()) + ") AS " + measure.stateLabel());
        }
        String time = quote(query.timeColumn());
        return "SELECT " + String.join(", ", select) + " FROM delta." + quote(query.path())
                + " WHERE " + time + " >= " + range.start() + " AND " + time + " < " + range.end()
                + (query.predicate().isEmpty() ? "" : " AND " + query.predicate())
                + (grain.isEmpty() ? "" : " GROUP BY " + String.join(", ", grain));
    }

    private static String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }
}
