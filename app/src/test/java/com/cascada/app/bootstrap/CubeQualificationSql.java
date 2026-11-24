package com.cascada.app.bootstrap;

import java.util.List;

/** Independently chosen producer and consumer operators for the real SQL qualification. */
final class CubeQualificationSql {
    private CubeQualificationSql() {}

    static List<String> reusable(String source) {
        String facts = " FROM " + source + " WHERE ts >= 0 AND ts < 777600";
        return List.of(
                "SELECT region AS area, SUM(amount) AS metric" + facts + " GROUP BY region",
                "SELECT region AS area, COUNT(amount) AS metric" + facts + " GROUP BY region",
                "SELECT region AS area, AVG(amount) AS metric" + facts + " GROUP BY region",
                "SELECT region AS area, SUM(amount) AS metric" + facts + " GROUP BY region, device",
                "SELECT region AS area, SUM(amount) AS metric" + facts + " AND region IS NULL GROUP BY region",
                "SELECT region AS area, SUM(amount) AS metric" + facts + " AND device = 'x' GROUP BY region",
                "SELECT region AS area, AVG(amount) AS metric" + facts + " AND region IS NOT NULL GROUP BY region");
    }

    static List<String> additionalReusable(String source) {
        String facts = " FROM " + source + " WHERE ts >= 0 AND ts < 777600";
        return List.of(
                "SELECT src.region AS area, AVG(src.amount) AS metric FROM (SELECT *" + facts
                        + ") src WHERE src.device = 'x' GROUP BY src.region",
                "WITH filtered AS (SELECT *" + facts + " AND device = 'y') "
                        + "SELECT region AS area, COUNT(amount) AS metric FROM filtered GROUP BY region",
                "SELECT region AS area, SUM(amount) AS metric" + facts
                        + " AND region IS NOT NULL AND device = 'x' GROUP BY region",
                "SELECT CAST(NULL AS STRING) AS area, totals.metric FROM (SELECT SUM(amount) AS metric"
                        + facts + ") totals",
                "SELECT region AS area, COUNT(amount) AS metric" + facts + " GROUP BY area");
    }

    static List<String> outsideCube(String source) {
        String facts = " FROM " + source + " WHERE ts >= 0 AND ts < 777600";
        return List.of(
                "SELECT region AS area, SUM(amount) AS metric" + facts + " GROUP BY region HAVING SUM(amount) <> 0",
                "SELECT region AS area, SUM(DISTINCT amount) AS metric" + facts + " GROUP BY region",
                "SELECT region AS area, SUM(CAST(amount AS DECIMAL(20,3))) AS metric" + facts + " GROUP BY region",
                "SELECT region AS area, COUNT(*) AS metric" + facts + " GROUP BY region",
                "SELECT region AS area, COUNT(amount) AS metric" + facts + " GROUP BY GROUPING SETS ((region), ())");
    }

    static List<String> consumers(String source) {
        String dimensions = "SELECT region, amount FROM " + source + " WHERE ts = 0";
        return List.of(
                "SELECT area, metric FROM cube_input WHERE metric IS NOT NULL",
                "SELECT area, CASE WHEN metric < 0 THEN 'debit' ELSE 'credit' END AS direction FROM cube_input",
                "SELECT area, CAST(COALESCE(metric, 0) AS DECIMAL(24,3)) AS exact_amount FROM cube_input",
                "SELECT area, SUM(metric) AS subtotal FROM cube_input GROUP BY area",
                "SELECT COUNT(*) AS group_total, COUNT(metric) AS present FROM cube_input",
                "SELECT area, MAX(metric) AS peak FROM cube_input GROUP BY area HAVING MAX(metric) >= 0",
                "SELECT DISTINCT area, metric FROM cube_input",
                "SELECT area FROM (WITH picked AS (SELECT area, metric FROM cube_input WHERE metric >= 0) SELECT area FROM picked) scoped",
                "SELECT l.area, l.metric AS left_metric, r.metric AS right_metric FROM cube_input l JOIN cube_input r ON l.area <=> r.area",
                "SELECT q.area, d.amount FROM cube_input q LEFT JOIN (" + dimensions + ") d ON q.area = d.region WHERE d.amount IS NULL",
                "SELECT q.area, d.amount FROM cube_input q RIGHT JOIN (" + dimensions + ") d ON q.area = d.region",
                "SELECT q.area, d.region FROM cube_input q FULL JOIN (" + dimensions + ") d ON q.area <=> d.region",
                "SELECT q.area, q.metric FROM cube_input q WHERE EXISTS (" + dimensions.replace("ts = 0", "ts = 0 AND region = q.area") + ")",
                "SELECT q.area, q.metric FROM cube_input q WHERE NOT EXISTS (" + dimensions.replace("ts = 0", "ts = 0 AND region <=> q.area") + ")",
                "SELECT area, metric FROM cube_input WHERE area IN (SELECT region FROM " + source + " WHERE ts = 0)",
                "SELECT area, metric FROM cube_input WHERE area NOT IN (SELECT region FROM " + source + " WHERE ts = 0)",
                "SELECT area, metric FROM cube_input UNION ALL SELECT area, metric FROM cube_input WHERE metric < 0",
                "SELECT area, metric FROM cube_input UNION SELECT area, metric FROM cube_input WHERE metric IS NULL",
                "SELECT area, metric FROM cube_input INTERSECT ALL SELECT area, metric FROM cube_input WHERE metric <= 0",
                "SELECT area, metric FROM cube_input EXCEPT ALL SELECT area, metric FROM cube_input WHERE area IS NULL",
                "SELECT area, metric, ROW_NUMBER() OVER (PARTITION BY area ORDER BY metric NULLS LAST) AS row_no FROM cube_input",
                "SELECT area, metric, RANK() OVER (ORDER BY metric DESC NULLS LAST) AS rank_no FROM cube_input",
                "SELECT area, metric, LAG(metric) OVER (PARTITION BY area ORDER BY metric NULLS FIRST) AS previous FROM cube_input",
                "SELECT area, metric, SUM(metric) OVER (PARTITION BY area ORDER BY metric RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount FROM cube_input",
                "SELECT area, metric FROM cube_input ORDER BY metric DESC NULLS LAST, area NULLS FIRST LIMIT 5 OFFSET 1");
    }
}
