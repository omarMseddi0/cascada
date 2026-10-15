# Cascada

A lakehouse query engine with a smart bucket cache



## Modules

| Module | Contents |
|---|---|
| `lib_identity` | Framework-free value objects: `SchemaVersion`, `LineageHash`, `QueryHash`, `PolicyVersion` |
| `lib_cache` | The cache domain + runnable engine: canonical query object, bucket math + pyramid, logic hashing, safety rules, columnar merge (`domain/merge/columnar`), AVG reconstruction, cube subsumption + consistency verifier, DataSketches HLL/KLL buckets, warming queue + popularity tracker, coverage bitmaps, `ResultFrame` + Arrow/zstd serialization, in-memory + Lettuce/Valkey backends, `CacheExecutionEngine` |
| `lib_sql` | Calcite SQL compiler: canonical extraction (AVGâ†’SUM/COUNT, group-by/filter/time detection, HAVING/JOIN/DISTINCT logic signature, per-alias aggregate map), MySQLâ†’Spark dialect translation, gap-query builder, cache-correctness simulation gate |
| `lib_spark_config` | Pure three-knob (RAM/CPU/placement) Spark config derivation, golden-tested key-by-key |
| `lib_spark` | SparkSession assembly and the Spark/Delta execution adapter |
| `lib_fabric` | Cluster deployment: env-driven K8s YAML templates applied via Fabric8 (ServiceAccount/RBAC, ConfigMaps, executor pod template, driver Deployment), tested against the Fabric8 mock server |
| `app` | Wiring. `mvn -Plocal-spark` bundles Spark 4.1.1 + Delta 4.1.0 for a local `local[*]` run on JDK 21 |

## Java 21 and runtime compatibility

The reactor targets Java 21 and Spark 4.1.1 / Scala 2.13.17 with Delta 4.1.0. Spark 3.5.6 documents Java 8/11/17 support; Spark 4.1.1 documents Java 17/21 support. A Java 21 runtime therefore uses the Spark 4.1 line. The Maven compiler release is 21, and Java 17 cannot run the resulting application.

The JVM options below cover Agrona, Arrow and the Spark runtime. Maven tests add them automatically. For an application launch, set them in `JAVA_TOOL_OPTIONS` or the JVM startup arguments; for `spark-submit`, include them in `--driver-java-options` before the Spark driver starts.

```text
--enable-native-access=ALL-UNNAMED
-Dio.netty.tryReflectionSetAccessible=true
--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
--add-opens=java.base/java.util.zip=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED
```
## Build and test

```bash
cd cascada
mvn -N install   # parent BOM, first time only
mvn test
```




## What the tests pin down

- AVG reconstruction divides summed SUM/COUNT, never averages averages.
- Bucket boundary algebra partitions head/body/tail with no gap or overlap; epoch resampling
  floors to step buckets, never to zero.
- The logic hash ignores the time range and clause order, but changes with aggregates, group-by,
  filters, step, HAVING, JOIN ON conditions, and DISTINCT. The canonical semantics version also
  separates identities produced by different projection-order rules.
- DISTINCT, HAVING, and JOIN bypass the cache entirely (`NON_MERGEABLE_SQL_FEATURE`): their
  per-bucket partials cannot be recombined into the whole-window answer.
- Warming stops at the last **complete** bucket boundary; a mid-bucket `warmEnd` never stores a
  partial bucket under a full-bucket key.
- A query with no aggregate bypasses the cache (`NO_AGGREGATION`) instead of having a GROUP BY
  fabricated for it.
- `MAX(latency) AS peak_latency` merges as MAX across buckets, and the cube consistency verifier
  rejects the old SUMmed answer.
- The zero-config derivation reproduces the golden `spark.json` key-by-key and never
  oversubscribes RAM.
- ArchUnit fails the build if a framework import enters a `domain` package.

## Gluten + Velox

Gluten is config-layered onto the same SparkSession â€” no engine code change. Needs a Linux image
with a Gluten Velox bundle built for Spark 4.1 / Scala 2.13 and, critically, the **off-heap re-split**: Velox works
off-heap, so `spark.memory.offHeap.size` takes ~60% of executor RAM and the JVM heap *shrinks* to
~30% (leaving heap at its vanilla size can exceed the executor memory budget). Gluten configuration
is selected by the workload profile; the executor does not implement an automatic retry with Gluten disabled. Verify with `EXPLAIN`
(look for `VeloxNativeScan`); many `VeloxColumnarToRowExec` boundaries mean a mostly-fallback plan.






