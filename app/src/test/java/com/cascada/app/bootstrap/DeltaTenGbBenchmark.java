package com.cascada.app.bootstrap;

import com.cascada.spark.adapter.out.spark.SparkDeltaQueryExecutor;
import com.cascada.spark.application.configuration.SparkSessionConfigBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.*;
import java.security.*;
import java.util.*;
import org.apache.spark.sql.*;
import org.apache.spark.scheduler.*;

/** Opt-in real Delta benchmark. The runner isolates JVM/native processes and owns all data. */
public final class DeltaTenGbBenchmark {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final List<String> EVENTS = Collections.synchronizedList(new ArrayList<>());
    private static final Path DATA = Path.of("/benchmark-data");
    private static final long BASE = 1759968000L, DAY = 86400;
    private static final long OFF_HEAP = Long.parseLong(System.getenv().getOrDefault("CASCADA_GLUTEN_OFFHEAP_BYTES", "3221225472"));
    private static final boolean MEMORY_ISOLATION = Boolean.parseBoolean(System.getenv().getOrDefault("CASCADA_BENCH_MEMORY_ISOLATION", "true"));

    public static void main(String[] args) throws Exception {
        if (args.length != 2 || !Set.of("generate", "measure").contains(args[0]))
            throw new IllegalArgumentException("generate|measure /validation/output.json");
        boolean nativeMode = Boolean.parseBoolean(System.getenv("CASCADA_GLUTEN_ENABLED"));
        var config = SparkSessionConfigBuilder.forLocal().master("local[6]")
                .appName("cascada-random-delta-10gb-" + args[0])
                .withEnvironment(System::getenv)
                .withProperty("spark.sql.ansi.enabled", "false")
                .withProperty("spark.ui.enabled", "false")
                .withProperty("spark.driver.host", "127.0.0.1")
                .withProperty("spark.driver.bindAddress", "127.0.0.1")
                .withProperty("spark.sql.session.timeZone", "UTC")
                .withProperty("spark.sql.shuffle.partitions", "48")
                .withProperty("spark.default.parallelism", "6")
                .withProperty("spark.memory.offHeap.enabled", "true")
                .withProperty("spark.memory.offHeap.size", Long.toString(OFF_HEAP))
                .withProperty("spark.gluten.memory.isolation",Boolean.toString(MEMORY_ISOLATION))
                .withProperty("spark.sql.warehouse.dir", DATA.resolve("warehouse").toUri().toString())
                .withProperty("spark.databricks.delta.snapshotPartitions", "6")
                .withProperty("spark.driver.maxResultSize", "512m")
                .build();
        try (var adapter = new SparkDeltaQueryExecutor(config)) {
            var spark = SparkSession.active();
            if(nativeMode) spark.sparkContext().addSparkListener(new SparkListener() {
                @Override public void onOtherEvent(SparkListenerEvent event) {
                    if(event.getClass().getName().startsWith("org.apache.gluten.events.")) EVENTS.add(event.toString());
                }
            });
            spark.sparkContext().setLogLevel("ERROR");
            if (!spark.sparkContext().master().equals("local[6]") || spark.conf().get("spark.sql.ansi.enabled").equals("true"))
                throw new IllegalStateException("Benchmark requires six Spark cores and ANSI disabled");
            if (args[0].equals("generate")) generate(spark, Path.of(args[1]));
            else measure(spark, nativeMode, Path.of(args[1]));
        }
    }

    private static void generate(SparkSession spark, Path output) throws Exception {
        long target = Long.parseLong(System.getenv().getOrDefault("CASCADA_BENCH_TARGET_BYTES", "10000000000"));
        long seed = Long.parseLong(System.getenv().getOrDefault("CASCADA_BENCH_SEED", Long.toString(new SecureRandom().nextLong() & Long.MAX_VALUE)));
        long rows = 0, bytes = 0, batchRows = 2_000_000;
        int batch = 0;
        Files.createDirectories(DATA);
        var seeds = new ArrayList<Long>();
        var batches = new ArrayList<Map<String,Object>>();
        long started = System.nanoTime();
        while (bytes < target) {
            long batchSeed = seed + 101L * batch;
            seeds.add(batchSeed);
            String sql = """
                SELECT id AS event_id,
                  %d + CAST(rand(%d)*%d AS BIGINT) AS ts,
                  CASE WHEN rand(%d)<0.015 THEN NULL WHEN rand(%d)<0.30 THEN 'R000'
                    ELSE concat('R',lpad(CAST(CAST(rand(%d)*200 AS INT) AS STRING),3,'0')) END AS region,
                  concat('D',lpad(CAST(CAST(rand(%d)*64 AS INT) AS STRING),2,'0')) AS device,
                  CAST(rand(%d)*10000 AS INT) AS campaign,
                  CAST(rand(%d)*10000000 AS BIGINT) AS customer_id,
                  CASE WHEN rand(%d)<0.02 THEN NULL ELSE CAST(rand(%d)*20000-5000 AS DECIMAL(18,2)) END AS amount,
                  CAST(rand(%d)*10000 AS DECIMAL(18,4)) AS cost,
                  CAST(rand(%d)*100 AS BIGINT)+1 AS units,
                  CASE WHEN rand(%d)<0.02 THEN NULL ELSE rand(%d)*5000 END AS latency,
                  rand(%d)*3600 AS duration,
                  rand(%d)*1000000 AS response_bytes,
                  rand(%d)*100 AS energy_kwh,
                  rand(%d)*80-20 AS temperature
                FROM range(%d,%d,1,24)
                """.formatted(BASE, batchSeed, 30 * DAY,
                    batchSeed+1,batchSeed+2,batchSeed+3,batchSeed+4,batchSeed+5,batchSeed+6,
                    batchSeed+7,batchSeed+8,batchSeed+9,batchSeed+10,batchSeed+11,batchSeed+12,
                    batchSeed+13,batchSeed+14,batchSeed+15,batchSeed+16,rows,rows+batchRows);
            long batchStarted = System.nanoTime();
            spark.sql(sql).write().format("delta").option("compression", "snappy")
                    .mode(batch == 0 ? "overwrite" : "append").save(DATA.resolve("events").toString());
            rows += batchRows;
            bytes = parquetBytes(DATA.resolve("events"));
            batch++;
            batches.add(Map.of("batch",batch,"seed",batchSeed,"rowsAdded",batchRows,"totalRows",rows,
                    "totalParquetBytes",bytes,"writeSeconds",(System.nanoTime()-batchStarted)/1e9));
            System.out.println("BENCH_DATA_PROGRESS rows="+rows+" parquetBytes="+bytes+" target="+target);
            if (bytes == 0 || rows > 350_000_000L) throw new IllegalStateException("Dataset size did not converge safely");
            long estimate = (long)Math.ceil((target-bytes)/(bytes/(double)rows)*1.01);
            batchRows = Math.max(250_000,Math.min(16_000_000,estimate));
        }
        spark.range(200).selectExpr("concat('R',lpad(CAST(id AS STRING),3,'0')) AS region",
                "CASE WHEN id%3=0 THEN 'north' WHEN id%3=1 THEN 'south' ELSE 'central' END AS market")
                .write().format("delta").mode("overwrite").save(DATA.resolve("regions").toString());
        if (!Files.isDirectory(DATA.resolve("events/_delta_log"))) throw new IllegalStateException("Missing Delta transaction log");
        var frame = spark.read().format("delta").load(DATA.resolve("events").toString());
        long observedRows = frame.count();
        if (observedRows != rows) throw new IllegalStateException("Delta row count mismatch");
        var report = new LinkedHashMap<String,Object>();
        report.put("spark", spark.version());report.put("delta", "4.1.0");report.put("seed", seed);
        report.put("batchSeeds", seeds);report.put("rows", rows);report.put("parquetBytes", bytes);
        report.put("batches",batches);
        report.put("targetBytes", target);report.put("schema", frame.schema().json());
        report.put("generationSeconds", (System.nanoTime()-started)/1e9);
        report.put("compression", "snappy");report.put("ansiEnabled", false);report.put("sparkCores",6);
        report.put("distribution", "independent seeded random measures/IDs/timestamps; 30% hot region; nullable groups and amounts");
        JSON.writerWithDefaultPrettyPrinter().writeValue(output.toFile(), report);
        JSON.writerWithDefaultPrettyPrinter().writeValue(DATA.resolve("dataset.json").toFile(), report);
        System.out.println("BENCH_DATA_COMPLETE rows="+rows+" parquetBytes="+bytes);
    }

    private static long parquetBytes(Path path) throws Exception {
        try(var files=Files.walk(path)) {
            long total=0;
            for(var file:files.filter(p -> p.getFileName().toString().endsWith(".parquet")).toList()) total+=Files.size(file);
            return total;
        }
    }

    private static LinkedHashMap<String,String> queries() {
        var queries = new LinkedHashMap<String,String>();
        queries.put("wide-group-aggregates", "SELECT region,device,COUNT(*) AS n,COUNT(amount) AS present,SUM(amount) AS total,AVG(amount) AS mean,MIN(amount) AS lo,MAX(amount) AS hi,SUM(cost) AS costs,SUM(units) AS units,ROUND(AVG(latency),4) AS latency,ROUND(AVG(duration),4) AS duration,ROUND(AVG(response_bytes),4) AS bytes,ROUND(AVG(energy_kwh),4) AS energy,ROUND(AVG(temperature),4) AS temperature FROM events GROUP BY region,device ORDER BY region NULLS FIRST,device");
        queries.put("hourly-group", "SELECT CAST(FLOOR(ts/3600)*3600 AS BIGINT) AS hour,region,COUNT(*) AS n,SUM(amount) AS total,AVG(amount) AS mean FROM events GROUP BY CAST(FLOOR(ts/3600)*3600 AS BIGINT),region ORDER BY hour,region NULLS FIRST");
        queries.put("filtered-cte-having", "WITH filtered AS (SELECT region,device,amount,units FROM events WHERE ts>="+(BASE+5*DAY)+" AND ts<"+(BASE+25*DAY)+" AND amount>0), grouped AS (SELECT region,device,SUM(amount) AS total,COUNT(*) AS n,SUM(units) AS units FROM filtered GROUP BY region,device HAVING COUNT(*)>100) SELECT * FROM grouped ORDER BY total DESC,region NULLS FIRST,device");
        queries.put("nested-cte-rollup", "WITH daily AS (SELECT CAST(FLOOR(ts/86400)*86400 AS BIGINT) AS day,region,device,SUM(amount) AS total,COUNT(*) AS n FROM events GROUP BY CAST(FLOOR(ts/86400)*86400 AS BIGINT),region,device), devices AS (SELECT region,device,SUM(total) AS total,SUM(n) AS n FROM daily GROUP BY region,device HAVING SUM(n)>20), regions AS (SELECT region,SUM(total) AS total,SUM(n) AS n,COUNT(*) AS devices FROM devices GROUP BY region) SELECT * FROM regions ORDER BY total DESC,region NULLS FIRST");
        queries.put("delta-dimension-join", "SELECT d.market,e.device,SUM(e.amount) AS total,COUNT(*) AS n,AVG(e.amount) AS mean FROM events e JOIN regions d ON e.region=d.region GROUP BY d.market,e.device ORDER BY d.market,e.device");
        queries.put("cte-aggregate-join", "WITH devices AS (SELECT region,device,SUM(amount) AS total,COUNT(*) AS n FROM events GROUP BY region,device), totals AS (SELECT region,SUM(total) AS total,SUM(n) AS n FROM devices GROUP BY region) SELECT d.region,d.device,d.total,d.n,t.total AS region_total,t.n AS region_n FROM devices d JOIN totals t ON d.region <=> t.region ORDER BY d.region NULLS FIRST,d.device");
        queries.put("in-subquery", "SELECT region,SUM(amount) AS total,COUNT(*) AS n FROM events WHERE device IN (SELECT device FROM events WHERE ts<"+(BASE+DAY)+" GROUP BY device HAVING AVG(units)>50) GROUP BY region ORDER BY region NULLS FIRST");
        queries.put("high-cardinality-topk", "SELECT campaign,CAST(FLOOR(customer_id/100000) AS BIGINT) AS customer_group,SUM(amount) AS total,COUNT(*) AS n FROM events WHERE ts<"+(BASE+3*DAY)+" GROUP BY campaign,CAST(FLOOR(customer_id/100000) AS BIGINT) HAVING COUNT(*)>=2 ORDER BY total DESC,campaign,customer_group LIMIT 100");
        return queries;
    }

    private static void measure(SparkSession spark, boolean nativeMode, Path output) throws Exception {
        spark.read().format("delta").load(DATA.resolve("events").toString()).createOrReplaceTempView("events");
        spark.read().format("delta").load(DATA.resolve("regions").toString()).createOrReplaceTempView("regions");
        var results = new ArrayList<Map<String,Object>>();
        for(var query:queries().entrySet()) {
            String selection=System.getenv("CASCADA_BENCH_QUERY");
            if(selection!=null && !selection.isBlank() && !selection.equals(query.getKey())) continue;
            int eventStart=EVENTS.size();
            var result=new LinkedHashMap<String,Object>();
            result.put("query",query.getKey());result.put("sql",query.getValue());result.put("status","warmup-started");
            results.add(result);
            save(output,spark,nativeMode,results);
            System.out.println("BENCH_QUERY_BEGIN mode="+(nativeMode?"velox":"jvm")+" query="+query.getKey()+" sql="+query.getValue());
            var warm=spark.sql(query.getValue());
            long warmStarted=System.nanoTime();
            var warmRows=warm.collectAsList();
            double warmSeconds=(System.nanoTime()-warmStarted)/1e9;
            String warmDigest=digest(warmRows);
            result.put("warmupSeconds",warmSeconds);result.put("warmupRows",warmRows.size());result.put("warmupSha256",warmDigest);
            result.put("status","measurement-started");
            save(output,spark,nativeMode,results);
            System.out.println("BENCH_WARMUP query="+query.getKey()+" seconds="+warmSeconds+" sha256="+warmDigest);
            var measured=spark.sql(query.getValue());
            long start=System.nanoTime();
            var rows=measured.collectAsList();
            double seconds=(System.nanoTime()-start)/1e9;
            String checksum=digest(rows);
            result.put("seconds",seconds);result.put("rows",rows.size());result.put("sha256",checksum);
            result.put("schema",measured.schema().json());
            if(!checksum.equals(warmDigest)) {
                var differences=new ArrayList<Map<String,Object>>();
                for(int r=0;r<Math.min(rows.size(),warmRows.size());r++) {
                    for(int c=0;c<rows.get(r).size();c++) if(!Objects.equals(rows.get(r).get(c),warmRows.get(r).get(c))) {
                        var difference=new LinkedHashMap<String,Object>();difference.put("row",r);difference.put("column",measured.columns()[c]);
                        difference.put("warm",warmRows.get(r).get(c));difference.put("measured",rows.get(r).get(c));
                        difference.put("warmRow",warmRows.get(r).toString());difference.put("measuredRow",rows.get(r).toString());
                        differences.add(difference);if(differences.size()>=30)break;
                    }
                    if(differences.size()>=30)break;
                }
                result.put("differences",differences);result.put("status","mismatch");
                result.put("executedPlan",measured.queryExecution().executedPlan().toString());
                save(output,spark,nativeMode,results);
                System.out.println("BENCH_MISMATCH "+JSON.writeValueAsString(result));
                throw new IllegalStateException("Warm/measured results differ: "+query.getKey());
            }
            var operators=executedClasses(measured.queryExecution().executedPlan());
            long stages=operators.stream().filter(name -> name.contains("WholeStageTransformer")).count();
            long deltaScans=operators.stream().filter(name -> name.endsWith("DeltaScanTransformer")).count();
            if(nativeMode && deltaScans==0)
                throw new IllegalStateException("Native Delta data scan absent: "+query.getKey()+" operators="+operators);
            if(!nativeMode && stages>0) throw new IllegalStateException("JVM baseline contains native execution");
            result.put("seconds",seconds);result.put("status","completed");
            result.put("rows",rows.size());result.put("sha256",checksum);result.put("schema",measured.schema().json());
            result.put("nativeStages",stages);result.put("operators",operators);
            result.put("nativeDeltaScans",deltaScans);
            result.put("executedPlan",measured.queryExecution().executedPlan().toString());
            spark.sparkContext().listenerBus().waitUntilEmpty(10000);
            result.put("metrics",executedMetrics(measured.queryExecution().executedPlan()));
            synchronized(EVENTS){result.put("glutenEvents",new ArrayList<>(EVENTS.subList(eventStart,EVENTS.size())));}
            System.out.println("BENCH_QUERY mode="+(nativeMode?"velox":"jvm")+" query="+query.getKey()+" seconds="+seconds+" rows="+rows.size()+" nativeStages="+stages+" sha256="+checksum);
            // Save each completed query so diagnostics survive a later failure.
            save(output,spark,nativeMode,results);
        }
        if(nativeMode && results.stream().noneMatch(result -> ((Number)result.get("nativeStages")).longValue()>0))
            throw new IllegalStateException("Native benchmark did not execute any native stages");
    }

    private static void save(Path output,SparkSession spark,boolean nativeMode,List<Map<String,Object>> results) throws Exception {
        JSON.writerWithDefaultPrettyPrinter().writeValue(output.toFile(),Map.of("nativeEnabled",nativeMode,
                "spark",spark.version(),"sparkCores",6,"ansiEnabled",false,"heapBytes",Runtime.getRuntime().maxMemory(),
                "offHeapBytes",OFF_HEAP,"memoryIsolation",MEMORY_ISOLATION,"results",results));
    }

    private static String digest(List<Row> rows) throws Exception {
        var digest=MessageDigest.getInstance("SHA-256");
        for(var row:rows) {
            var values=new ArrayList<Object>();
            for(int column=0;column<row.size();column++) values.add(row.get(column));
            digest.update(JSON.writeValueAsBytes(values));digest.update((byte)'\n');
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static List<String> executedClasses(org.apache.spark.sql.execution.SparkPlan root) {
        var seen=Collections.newSetFromMap(new IdentityHashMap<org.apache.spark.sql.execution.SparkPlan,Boolean>());
        var pending=new ArrayDeque<org.apache.spark.sql.execution.SparkPlan>();pending.add(root);
        var classes=new ArrayList<String>();
        while(!pending.isEmpty()) {
            var node=pending.removeFirst();if(!seen.add(node))continue;classes.add(node.getClass().getName());
            if(node instanceof org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec adaptive) pending.add(adaptive.executedPlan());
            if(node instanceof org.apache.spark.sql.execution.adaptive.QueryStageExec stage) pending.add(stage.plan());
            pending.addAll(scala.jdk.javaapi.CollectionConverters.asJava(node.children()));
            pending.addAll(scala.jdk.javaapi.CollectionConverters.asJava(node.subqueries()));
            if(node instanceof org.apache.spark.sql.execution.exchange.ReusedExchangeExec reused) pending.add(reused.child());
        }
        return classes;
    }

    private static List<Map<String,Object>> executedMetrics(org.apache.spark.sql.execution.SparkPlan root) {
        var seen=Collections.newSetFromMap(new IdentityHashMap<org.apache.spark.sql.execution.SparkPlan,Boolean>());
        var pending=new ArrayDeque<org.apache.spark.sql.execution.SparkPlan>();pending.add(root);
        var records=new ArrayList<Map<String,Object>>();
        while(!pending.isEmpty()) {
            var node=pending.removeFirst();if(!seen.add(node))continue;
            var values=new LinkedHashMap<String,Long>();
            scala.jdk.javaapi.CollectionConverters.asJava(node.metrics()).forEach((name,metric) -> values.put(name,metric.value()));
            records.add(Map.of("operator",node.getClass().getName(),"metrics",values));
            if(node instanceof org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec adaptive) pending.add(adaptive.executedPlan());
            if(node instanceof org.apache.spark.sql.execution.adaptive.QueryStageExec stage) pending.add(stage.plan());
            pending.addAll(scala.jdk.javaapi.CollectionConverters.asJava(node.children()));
            pending.addAll(scala.jdk.javaapi.CollectionConverters.asJava(node.subqueries()));
            if(node instanceof org.apache.spark.sql.execution.exchange.ReusedExchangeExec reused) pending.add(reused.child());
        }
        return records;
    }
}
