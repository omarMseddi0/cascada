package com.cascada.app.bootstrap;

import com.cascada.spark.adapter.out.spark.SparkDeltaQueryExecutor;
import com.cascada.spark.application.configuration.SparkSessionConfigBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.scheduler.*;

/** Small, opt-in native Delta gate: transaction semantics and completed physical plans. */
public final class NativeDeltaValidation {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final List<Map<String,Object>> RESULTS = new ArrayList<>();
    private static final List<String> EVENTS = Collections.synchronizedList(new ArrayList<>());
    private static Path output;
    private static long seed;
    private static final List<String> COMPONENTS = new ArrayList<>();
    private static final Map<String,String> CONFIG = new LinkedHashMap<>();

    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("/validation/result.json");
        seed = new SecureRandom().nextLong() & Long.MAX_VALUE;
        output = Path.of(args[0]);
        var config = SparkSessionConfigBuilder.forLocal().master("local[6]").withEnvironment(System::getenv)
                .appName("cascada-native-delta-gate").withProperty("spark.ui.enabled","false")
                .withProperty("spark.driver.host","127.0.0.1").withProperty("spark.driver.bindAddress","127.0.0.1")
                .withProperty("spark.sql.shuffle.partitions","12").withProperty("spark.sql.session.timeZone","UTC")
                .withProperty("spark.sql.ansi.enabled","false")
                .withProperty("spark.gluten.sql.native.writer.enabled","false")
                .withProperty("spark.sql.warehouse.dir","/benchmark-data/smoke-warehouse").build();
        try (var executor = new SparkDeltaQueryExecutor(config)) {
            SparkSession nativeSession = SparkSession.active();
            var componentObject = Class.forName("org.apache.gluten.component.Component$");
            Object module = componentObject.getField("MODULE$").get(null);
            var components = (scala.collection.Seq<?>)componentObject.getMethod("sorted").invoke(module);
            for (Object component : scala.jdk.javaapi.CollectionConverters.asJava(components))
                COMPONENTS.add(component.getClass().getMethod("name").invoke(component).toString());
            if (!COMPONENTS.contains("velox-delta")) throw new IllegalStateException("velox-delta component inactive: "+COMPONENTS);
            System.out.println("NATIVE_DELTA_COMPONENTS "+COMPONENTS);
            for(String key:List.of("spark.master","spark.plugins","spark.sql.extensions","spark.shuffle.manager",
                    "spark.memory.offHeap.enabled","spark.memory.offHeap.size","spark.gluten.memory.isolation",
                    "spark.gluten.sql.columnar.backend.velox.spillStrategy","spark.sql.ansi.enabled"))
                CONFIG.put(key,nativeSession.sparkContext().getConf().get(key,"unset"));
            nativeSession.sparkContext().setLogLevel("WARN");
            nativeSession.sparkContext().addSparkListener(new SparkListener() {
                @Override public void onOtherEvent(SparkListenerEvent event) {
                    if (event.getClass().getName().startsWith("org.apache.gluten.events."))
                        EVENTS.add(event.toString());
                }
            });
            SparkSession oracle = nativeSession.newSession();
            oracle.conf().set("spark.gluten.enabled","false");
            String path = "/benchmark-data/native-delta-gate";
            String table = "delta.`"+path+"`";
            oracle.sql("SELECT id, CAST(1759968000 + id%604800 AS BIGINT) ts, "
                    + "CASE WHEN id%19=0 THEN NULL ELSE concat('G',CAST(id%31 AS STRING)) END g, "
                    + "CASE WHEN id%13=0 THEN NULL ELSE CAST(rand("+seed+")*20000-5000 AS DECIMAL(18,2)) END amount, "
                    + "CAST(id%101 AS BIGINT) units FROM range(20000)")
                    .write().format("delta").mode("overwrite").save(path);
            var queries = new LinkedHashMap<String,String>();
            queries.put("ordinary-aggregates","SELECT g,COUNT(*) n,COUNT(amount) present,SUM(amount) total,AVG(amount) mean,MIN(amount) lo,MAX(amount) hi FROM "+table+" GROUP BY g ORDER BY g NULLS FIRST");
            queries.put("filtered-nested-cte","WITH f AS (SELECT * FROM "+table+" WHERE ts>=1759969000 AND amount>0), a AS (SELECT g,SUM(amount) total,COUNT(*) n FROM f GROUP BY g HAVING COUNT(*)>2) SELECT * FROM a ORDER BY total DESC,g NULLS FIRST LIMIT 20");
            queries.put("aggregate-cte-join","WITH a AS (SELECT g,SUM(amount) total,COUNT(*) n FROM "+table+" GROUP BY g), b AS (SELECT g,MAX(units) hi FROM "+table+" GROUP BY g) SELECT a.*,b.hi FROM a JOIN b ON a.g <=> b.g ORDER BY a.g NULLS FIRST");
            queries.put("in-subquery","SELECT g,COUNT(*) n,SUM(amount) total FROM "+table+" WHERE units IN (SELECT units FROM "+table+" WHERE amount>1500 GROUP BY units HAVING COUNT(*)>1) GROUP BY g ORDER BY g NULLS FIRST");
            queries.put("cube","SELECT g,CAST(FLOOR(ts/86400)*86400 AS BIGINT) day,GROUPING_ID() gid,COUNT(*) n,SUM(amount) total FROM "+table+" GROUP BY CUBE(g,CAST(FLOOR(ts/86400)*86400 AS BIGINT)) ORDER BY gid,g NULLS FIRST,day NULLS FIRST");
            queries.put("empty-scalar","SELECT COUNT(*) n,COUNT(amount) present,SUM(amount) total,AVG(amount) mean,MIN(amount) lo,MAX(amount) hi FROM "+table+" WHERE id<0");
            for(var query:queries.entrySet()) compare(nativeSession,oracle,query.getKey(),query.getValue(),true);
            oracle.sql("SELECT CAST(20000 AS BIGINT) id,CAST(-1 AS BIGINT) ts,'G0' g,CAST(1.25 AS DECIMAL(18,2)) amount,CAST(1 AS BIGINT) units")
                    .write().format("delta").mode("append").save(path);
            compare(nativeSession,oracle,"snapshot-current", "SELECT COUNT(*) n,SUM(amount) total,MIN(ts) lo,MAX(ts) hi FROM "+table,true);
            compare(nativeSession,oracle,"time-travel-v0", "SELECT COUNT(*) n,SUM(amount) total FROM "+table+" VERSION AS OF 0",true);
            oracle.sql("ALTER TABLE "+table+" ADD COLUMNS (score BIGINT)").collectAsList();
            oracle.sql("UPDATE "+table+" SET score=units*2 WHERE id%5=0").collectAsList();
            compare(nativeSession,oracle,"schema-evolution", "SELECT g,COUNT(score) present,SUM(score) total FROM "+table+" GROUP BY g ORDER BY g NULLS FIRST",true);
            oracle.sql("DELETE FROM "+table+" WHERE id%7=0").collectAsList();
            compare(nativeSession,oracle,"delete-removed-files", "SELECT g,COUNT(*) n,SUM(amount) total FROM "+table+" GROUP BY g ORDER BY g NULLS FIRST",true);
            oracle.sql("MERGE INTO "+table+" t USING (SELECT CAST(1 AS BIGINT) id,CAST(9.75 AS DECIMAL(18,2)) amount) s ON t.id=s.id WHEN MATCHED THEN UPDATE SET t.amount=s.amount").collectAsList();
            compare(nativeSession,oracle,"merge-snapshot", "SELECT COUNT(*) n,SUM(amount) total FROM "+table,true);
            oracle.range(100).selectExpr("id","CAST(id AS BIGINT) ts", "CAST(id AS DECIMAL(18,2)) amount")
                    .write().format("delta").option("overwriteSchema","true").mode("overwrite").save(path);
            compare(nativeSession,oracle,"overwrite-snapshot", "SELECT COUNT(*) n,SUM(amount) total,MIN(ts) lo,MAX(ts) hi FROM "+table,true);
            compare(nativeSession,oracle,"time-travel-after-overwrite", "SELECT g,COUNT(*) n,SUM(amount) total FROM "+table+" VERSION AS OF 0 GROUP BY g ORDER BY g NULLS FIRST",true);
            String mapped="delta.`/benchmark-data/native-delta-mapped`";
            oracle.sql("CREATE TABLE "+mapped+" (`odd column` BIGINT, amount DECIMAL(18,2)) USING DELTA TBLPROPERTIES ('delta.columnMapping.mode'='name')").collectAsList();
            oracle.sql("INSERT INTO "+mapped+" VALUES (1,2.50),(2,3.75)").collectAsList();
            oracle.sql("ALTER TABLE "+mapped+" RENAME COLUMN `odd column` TO renamed").collectAsList();
            compare(nativeSession,oracle,"column-mapping-rename", "SELECT renamed,SUM(amount) total FROM "+mapped+" GROUP BY renamed ORDER BY renamed",false);
            String dv="delta.`/benchmark-data/native-delta-dv`";
            oracle.sql("CREATE TABLE "+dv+" (id BIGINT, amount BIGINT) USING DELTA TBLPROPERTIES ('delta.enableDeletionVectors'='true')").collectAsList();
            oracle.sql("INSERT INTO "+dv+" SELECT id,id*2 FROM range(1000)").collectAsList();
            oracle.sql("DELETE FROM "+dv+" WHERE id%3=0").collectAsList();
            compare(nativeSession,oracle,"deletion-vectors", "SELECT COUNT(*) n,SUM(amount) total,MIN(id) lo,MAX(id) hi FROM "+dv,false);
            nativeSession.sparkContext().listenerBus().waitUntilEmpty(10000);
            JSON.writerWithDefaultPrettyPrinter().writeValue(Path.of(args[0]).toFile(),
                    Map.of("seed",seed,"spark",nativeSession.version(),"delta","4.1.0","ansiEnabled",false,
                            "results",RESULTS,"glutenEvents",new ArrayList<>(EVENTS),"components",COMPONENTS,"config",CONFIG));
            System.out.println("NATIVE_DELTA_GATE_COMPLETE cases="+RESULTS.size()+" seed="+seed);
        }
    }

    private static void compare(SparkSession nativeSession,SparkSession oracle,String name,String sql,boolean requireNativeScan) throws Exception {
        System.out.println("NATIVE_DELTA_BEGIN "+name+" sql="+sql);
        Dataset<Row> expected=oracle.sql(sql), actual=nativeSession.sql(sql);
        List<Row> reference=expected.collectAsList(), rows=actual.collectAsList();
        if (!rows.equals(reference) || !actual.schema().equals(expected.schema()))
            throw new IllegalStateException("Delta correctness/schema mismatch: "+name+" native="+rows+" oracle="+reference);
        List<String> classes=classes(actual.queryExecution().executedPlan());
        if(classes(expected.queryExecution().executedPlan()).stream().anyMatch(c -> c.contains("Transformer")))
            throw new IllegalStateException("Oracle unexpectedly used native execution: "+name);
        long scans=classes.stream().filter(c -> c.endsWith("DeltaScanTransformer")).count();
        // An empty predicate can be optimized to a local relation without scanning any files.
        if(requireNativeScan && !name.equals("empty-scalar") && scans==0)
            throw new IllegalStateException("DeltaScanTransformer absent: "+name+" "+classes);
        String plan=actual.queryExecution().executedPlan().toString();
        RESULTS.add(Map.of("case",name,"sql",sql,"rows",rows.size(),"schema",actual.schema().json(),
                "nativeDeltaScans",scans,"operators",classes,"executedPlan",plan,"consistent",true));
        JSON.writerWithDefaultPrettyPrinter().writeValue(output.toFile(),
                Map.of("seed",seed,"spark",nativeSession.version(),"delta","4.1.0","ansiEnabled",false,
                        "results",RESULTS,"glutenEvents",new ArrayList<>(EVENTS),"components",COMPONENTS,"config",CONFIG));
        System.out.println("NATIVE_DELTA_PASS "+name+" nativeScans="+scans+"\n"+plan);
    }

    private static List<String> classes(SparkPlan root) {
        Set<SparkPlan> seen=Collections.newSetFromMap(new IdentityHashMap<>());
        var pending=new ArrayDeque<SparkPlan>();pending.add(root);
        var result=new ArrayList<String>();
        while(!pending.isEmpty()) {
            var node=pending.removeFirst();if(!seen.add(node))continue;result.add(node.getClass().getName());
            if(node instanceof org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec a) pending.add(a.executedPlan());
            if(node instanceof org.apache.spark.sql.execution.adaptive.QueryStageExec s) pending.add(s.plan());
            pending.addAll(scala.jdk.javaapi.CollectionConverters.asJava(node.children()));
            pending.addAll(scala.jdk.javaapi.CollectionConverters.asJava(node.subqueries()));
            if(node instanceof org.apache.spark.sql.execution.exchange.ReusedExchangeExec reused) pending.add(reused.child());
        }
        return result;
    }
}
