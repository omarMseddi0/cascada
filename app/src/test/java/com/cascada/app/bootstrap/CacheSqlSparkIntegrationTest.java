package com.cascada.app.bootstrap;

import com.cascada.app.model.config.WarmingSettings;
import com.cascada.app.model.config.TableSettings;
import com.cascada.app.model.config.EngineSettings;
import com.cascada.app.model.config.ApplicationSettings;
import com.cascada.app.config.CascadaEngineFactory;
import com.cascada.app.config.*;
import com.cascada.cache.application.model.config.CacheExecutionConfiguration;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.spark.adapter.out.spark.SparkDeltaQueryExecutor;
import java.time.Duration;
import java.util.*;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import static org.assertj.core.api.Assertions.*;

/** Real Spark SQL differential check. A temporary view substitutes only for the Delta storage location. */
@EnabledIfSystemProperty(named="cascada.cache.spark.integration", matches="true")
class CacheSqlSparkIntegrationTest {
    @Test void coldMixedAndWarmAnswersMatchSparkForThePythonCoreShapes() {
        SparkSession session = LocalSparkFixtures.localSession().master("local[2]").appName("cascada-cache-parity")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.sql.ansi.enabled", "false").getOrCreate();
        try (var spark = new SparkDeltaQueryExecutor(session)) {
            session.sparkContext().setLogLevel("ERROR");
            session.sql("CREATE OR REPLACE TEMP VIEW traffic AS SELECT CAST(id % 2 AS STRING) AS app, "
                    + "CAST(id % 3 AS STRING) AS device, id * 60 AS ts, id AS bytes, "
                    + "CAST(id % 7 AS DECIMAL(12,2)) AS x FROM range(0,2880,1,2)");
            var settings = new EngineSettings(CacheExecutionConfiguration.defaults(), "redis://unused:6379",
                    "traffic", "/lake/raw", true, 20);
            var application = new ApplicationSettings(ServerSettings.defaults(),
                    new WarmingSettings(true, Duration.ofHours(1), Duration.ofDays(7), 100),
                    List.of(new TableSettings("traffic", "/lake/raw", Map.of("app", "app", "device", "device",
                            "ts", "ts", "bytes", "bytes", "x", "x"), Set.of("ts"))));
            try (var engine = new CascadaEngineFactory(settings,
                    sql -> spark.execute(sql.replace("delta.`/lake/raw`", "traffic")), application)) {
                List<String> queries = List.of(
                        "SELECT app,AVG(x) AS avg_x,SUM(bytes) AS total FROM traffic WHERE ts>=0 AND ts<=172799 GROUP BY app ORDER BY app",
                        "SELECT app,(SUM(bytes)+COUNT(*))*2 AS total FROM traffic WHERE ts>=0 AND ts<=172799 GROUP BY app ORDER BY app",
                        "SELECT CAST(FLOOR(ts/3600)*3600 AS BIGINT) AS ts,app,SUM(bytes) AS total FROM traffic WHERE ts>=0 AND ts<=172799 GROUP BY CAST(FLOOR(ts/3600)*3600 AS BIGINT),app ORDER BY ts,app",
                        "SELECT CAST(FLOOR(ts/300)*300 AS BIGINT) AS ts,app,SUM(bytes) AS total FROM traffic WHERE ts>=0 AND ts<=172799 GROUP BY CAST(FLOOR(ts/300)*300 AS BIGINT),app ORDER BY ts,app",
                        "SELECT app,SUM(value) AS total FROM (SELECT app,SUM(bytes) AS value FROM traffic WHERE ts>=0 AND ts<=172799 GROUP BY app) q GROUP BY app ORDER BY app",
                        "SELECT app,AVG(x) AS avg_x FROM traffic WHERE ts>=0 AND ts<=172799 AND app='missing' GROUP BY app",
                        "WITH totals AS (SELECT app,SUM(bytes) AS value FROM traffic WHERE ts>=0 AND ts<=172799 GROUP BY app) SELECT app,value FROM totals",
                        "SELECT t.app,SUM(t.bytes+u.bytes) AS total FROM traffic t JOIN traffic u ON t.ts=u.ts WHERE t.ts>=0 AND t.ts<=172799 GROUP BY t.app");
                for (String sql : queries) {
                    ResultFrame expected = spark.execute(sql);
                    equalFrames(engine.executeLogicalQueryUseCase().query(sql).frame(), expected);
                    engine.warmCacheUseCase().warmCycle(0, 86399, false);
                    equalFrames(engine.executeLogicalQueryUseCase().query(sql).frame(), expected);
                    engine.warmCacheUseCase().warmCycle(86400, 172799, false);
                    equalFrames(engine.executeLogicalQueryUseCase().query(sql).frame(), expected);
                }
            }
            session.sql("CREATE OR REPLACE TEMP VIEW mapped_traffic AS SELECT app AS D1,ts AS T1,bytes AS M1,x AS X1 FROM traffic");
            var mappedSettings = new EngineSettings(new CacheExecutionConfiguration(86400, 300, "T1"),
                    "redis://unused:6379", "mapped", "/lake/mapped", true, 10);
            var mappedApplication = new ApplicationSettings(ServerSettings.defaults(), WarmingSettings.defaults(),
                    List.of(new TableSettings("mapped", "/lake/mapped",
                            Map.of("app", "D1", "ts", "T1", "bytes", "M1", "x", "X1"), Set.of("ts"))));
            try (var engine = new CascadaEngineFactory(mappedSettings,
                    sql -> spark.execute(sql.replace("delta.`/lake/mapped`", "mapped_traffic")), mappedApplication)) {
                for (String sql : List.of(
                        "SELECT app,SUM(bytes) AS total FROM (SELECT * FROM mapped) q WHERE ts>=0 AND ts<=172799 GROUP BY app",
                        "WITH q AS (SELECT * FROM mapped) SELECT app,SUM(bytes) AS total FROM q WHERE ts>=0 AND ts<=172799 GROUP BY app")) {
                    equalFrames(engine.executeLogicalQueryUseCase().query(sql).frame(),
                            spark.execute("SELECT app,SUM(bytes) AS total FROM traffic WHERE ts>=0 AND ts<=172799 GROUP BY app"));
                }
                for (String sql : List.of(
                        "SELECT renamed,SUM(metric) AS total FROM (SELECT app,bytes FROM mapped) q(renamed,metric) GROUP BY renamed",
                        "WITH q(renamed,metric) AS (SELECT app,bytes FROM mapped) SELECT renamed,SUM(metric) AS total FROM q GROUP BY renamed")) {
                    equalFrames(engine.executeLogicalQueryUseCase().query(sql).frame(),
                            spark.execute("SELECT app AS renamed,SUM(bytes) AS total FROM traffic GROUP BY app"));
                }
            }
        } finally { session.stop(); }
    }

    private static void equalFrames(ResultFrame actual, ResultFrame expected) {
        assertThat(actual.columnNames()).isEqualTo(expected.columnNames());
        assertThat(actual.columnTypes()).isEqualTo(expected.columnTypes());
        assertThat(actual.rows().stream().map(Map::toString).sorted().toList())
                .isEqualTo(expected.rows().stream().map(Map::toString).sorted().toList());
    }
}
