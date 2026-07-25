# Cascada

A lakehouse query engine with a smart bucket cache


## Modules

| Module | Contents |
|---|---|
| `lib_identity` | Framework-free value objects: `TenantIdentifier`, `SchemaVersion`, `LineageHash`, `QueryHash`, `PolicyVersion` |
| `lib_cache` | The cache domain + runnable engine: canonical query object, bucket math + pyramid, logic hashing, safety rules, columnar merge (`domain/merge/columnar`), AVG reconstruction, cube subsumption + consistency verifier, DataSketches HLL/KLL buckets, warming queue + popularity tracker, coverage bitmaps, `ResultFrame` + Arrow/zstd serialization, in-memory + Lettuce/Valkey backends, `CacheExecutionEngine` |
| `lib_sql` | Calcite SQL compiler: canonical extraction (AVG→SUM/COUNT, group-by/filter/time detection, HAVING/JOIN/DISTINCT logic signature, per-alias aggregate map), MySQL→Spark dialect translation, gap-query builder, cache-correctness simulation gate |
| `lib_spark_config` | Pure three-knob (RAM/CPU/placement) Spark config derivation, golden-tested key-by-key |
| `lib_spark` | SparkSession assembly and the Spark/Delta execution adapter |
| `lib_fabric` | Cluster deployment: env-driven K8s YAML templates applied via Fabric8 (ServiceAccount/RBAC, ConfigMaps, executor pod template, driver Deployment), tested against the Fabric8 mock server |
| `app` | Wiring. `mvn -Plocal-spark` bundles real Spark 3.5 + Delta for a local `local[*]` run (JDK 17 toolchain, or add-opens on newer) |

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
  filters, step, HAVING, JOIN ON conditions, and DISTINCT — and stays byte-stable for queries
  that use none of the new signature (no cold-cache event on upgrade).
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

Gluten is config-layered onto the same SparkSession — no engine code change. Needs a Linux image
with `gluten-velox-bundle-spark3.5_*.jar` and, critically, the **off-heap re-split**: Velox works
off-heap, so `spark.memory.offHeap.size` takes ~60% of executor RAM and the JVM heap *shrinks* to
~30% (leaving heap at its vanilla size is the classic production OOM). `spark.gluten.enabled` is
the per-query kill switch; any Gluten failure retries once with it off. Verify with `EXPLAIN`
(look for `VeloxNativeScan`); many `VeloxColumnarToRowExec` boundaries mean a mostly-fallback plan.






