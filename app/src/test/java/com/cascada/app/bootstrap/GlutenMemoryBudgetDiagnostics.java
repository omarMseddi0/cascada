package com.cascada.app.bootstrap;

import com.cascada.spark.adapter.out.spark.SparkDeltaQueryExecutor;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.*;
import java.util.*;
import org.apache.spark.sql.*;

/** Small, opt-in memory sweep on an existing benchmark table; not another full benchmark round. */
public final class GlutenMemoryBudgetDiagnostics {
    public static void main(String[] args) throws Exception {
        if(args.length!=2)throw new IllegalArgumentException("queries.json results.json");
        var json=new ObjectMapper();
        long offHeap=Long.parseLong(System.getenv("CASCADA_GLUTEN_OFFHEAP_BYTES"));
        boolean isolation=Boolean.parseBoolean(System.getenv().getOrDefault("CASCADA_BENCH_MEMORY_ISOLATION","false"));
        var config=LocalSparkFixtures.localConfig().master("local[6]").withEnvironment(System::getenv)
                .withProperty("spark.sql.ansi.enabled","false")
                .withProperty("spark.sql.shuffle.partitions","48").withProperty("spark.default.parallelism","6")
                .withProperty("spark.databricks.delta.snapshotPartitions","6")
                .withProperty("spark.memory.offHeap.enabled","true").withProperty("spark.memory.offHeap.size",Long.toString(offHeap))
                .withProperty("spark.gluten.memory.isolation",Boolean.toString(isolation))
                .withProperty("spark.gluten.sql.columnar.backend.velox.spillStrategy","auto")
                .withProperty("spark.gluten.sql.columnar.maxBatchSize","4096")
                .withProperty("spark.driver.maxResultSize","512m").build();
        try(var executor=new SparkDeltaQueryExecutor(config)) {
            var spark=SparkSession.active();spark.sparkContext().setLogLevel("ERROR");
            spark.read().format("delta").load("/benchmark-data/events").createOrReplaceTempView("events");
            var results=new ArrayList<Map<String,Object>>();
            var nativeConfig=new LinkedHashMap<String,Object>();
            for(String key:List.of("spark.gluten.memory.isolation","spark.gluten.sql.columnar.backend.velox.spillStrategy",
                    "spark.gluten.sql.columnar.maxBatchSize","spark.gluten.numTaskSlotsPerExecutor",
                    "spark.gluten.memory.task.offHeap.size.in.bytes","spark.gluten.memory.conservative.task.offHeap.size.in.bytes"))
                nativeConfig.put(key,spark.conf().get(key,spark.sparkContext().getConf().get(key,"<unset>")));
            for(var query:json.readTree(Path.of(args[0]).toFile())) {
                String name=query.get("query").asText(),sql=query.get("sql").asText();
                var warm=spark.sql(sql);long warmStart=System.nanoTime();var warmRows=warm.collectAsList();
                double warmSeconds=(System.nanoTime()-warmStart)/1e9;
                var measured=spark.sql(sql);long start=System.nanoTime();var rows=measured.collectAsList();
                double seconds=(System.nanoTime()-start)/1e9;
                String hash=LocalSparkFixtures.digest(rows),warmHash=LocalSparkFixtures.digest(warmRows);
                if(!hash.equals(warmHash)||!hash.equals(query.get("sha256").asText()))throw new IllegalStateException("Result mismatch "+name);
                var plans=planMetrics(measured.queryExecution().executedPlan());
                var result=new LinkedHashMap<String,Object>();
                result.put("query",name);result.put("sql",sql);result.put("seconds",seconds);result.put("warmupSeconds",warmSeconds);
                result.put("rows",rows.size());result.put("sha256",hash);result.put("warmupSha256",warmHash);result.put("planMetrics",plans);
                results.add(result);
                json.writerWithDefaultPrettyPrinter().writeValue(Path.of(args[1]).toFile(),Map.of("offHeapBytes",offHeap,
                        "heapBytes",Runtime.getRuntime().maxMemory(),"containerMemoryBytes",12884901888L,"sparkCores",6,
                        "ansiEnabled",false,"nativeConfig",nativeConfig,"results",results));
                System.out.println("MEMORY_DIAGNOSTIC offHeap="+offHeap+" query="+name+" seconds="+seconds+" sha256="+hash);
            }
        }
    }


    private static List<Map<String,Object>> planMetrics(org.apache.spark.sql.execution.SparkPlan root) {
        var seen=Collections.newSetFromMap(new IdentityHashMap<org.apache.spark.sql.execution.SparkPlan,Boolean>());
        var pending=new ArrayDeque<org.apache.spark.sql.execution.SparkPlan>();pending.add(root);
        var result=new ArrayList<Map<String,Object>>();
        while(!pending.isEmpty()) {
            var node=pending.removeFirst();if(!seen.add(node))continue;
            var metrics=new LinkedHashMap<String,Object>();
            for(var metric:scala.jdk.javaapi.CollectionConverters.asJava(node.metrics()).entrySet()){
                var value=metric.getValue();metrics.put(metric.getKey(),Map.of("name",value.name().isDefined()?value.name().get():"",
                        "value",value.value(),"type",value.metricType()));
            }
            result.add(Map.of("operator",node.getClass().getName(),"metrics",metrics));
            if(node instanceof org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec adaptive)pending.add(adaptive.executedPlan());
            if(node instanceof org.apache.spark.sql.execution.adaptive.QueryStageExec stage)pending.add(stage.plan());
            pending.addAll(scala.jdk.javaapi.CollectionConverters.asJava(node.children()));
        }
        return result;
    }
}
