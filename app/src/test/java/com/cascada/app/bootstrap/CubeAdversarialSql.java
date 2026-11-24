package com.cascada.app.bootstrap;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;

/** Varies window, population, grain, measures and outer operators independently. */
final class CubeAdversarialSql {
    private CubeAdversarialSql() {}

    static Map<String, Boolean> cases(String source, long day) {
        int[][] windows = {{0,12}, {1,11}, {2,10}, {3,9}, {4,8}, {4,9}, {0,9}, {3,12}};
        var filters = List.of("region IS NULL", "device = 'x'", "device IS NOT NULL",
                "region IS NOT NULL AND device = 'x'");
        var cases = new LinkedHashMap<String, Boolean>();
        for (int[] window : windows) {
            for (String filter : filters) {
                String facts = " FROM " + source + " WHERE ts >= " + window[0] * day
                        + " AND ts < " + window[1] * day + " AND " + filter;
                String group = " GROUP BY region";
                String avg = "SELECT region, AVG(amount) AS metric" + facts + group;
                String cte = "WITH measured AS (" + avg + ") ";
                cases.put("SELECT region, SUM(amount) AS metric" + facts + group
                        + " ORDER BY metric DESC NULLS LAST, region NULLS FIRST LIMIT 4", true);
                cases.put(avg + " ORDER BY region NULLS FIRST", true);
                cases.put("SELECT AVG(amount) AS metric" + facts, false);
                cases.put("SELECT region, device, COUNT(amount) AS metric" + facts
                        + " GROUP BY region, device ORDER BY region NULLS FIRST, device NULLS LAST", true);
                cases.put(cte + "SELECT l.region, l.metric AS left_amount, r.metric AS right_amount "
                        + "FROM measured l JOIN measured r ON l.region <=> r.region", false);
                cases.put(cte + "SELECT q.region, q.metric FROM measured q WHERE EXISTS (SELECT 1 FROM "
                        + source + " f WHERE f.region <=> q.region AND f.amount IS NULL)", false);
                cases.put("SELECT region, CAST(COALESCE(metric, 0) AS DECIMAL(26,5)) AS amount "
                        + "FROM (" + avg + ") q", false);
                cases.put(cte + "SELECT region, metric, LAG(metric) OVER (ORDER BY region NULLS FIRST) AS prior "
                        + "FROM measured", false);
            }
        }
        return cases;
    }
}
