# Cascada

A lakehouse query engine with a smart bucket cache: queries hit Spark once, then answer from
merged cached time buckets. The more a workload repeats, the less Spark it needs.

Java 21, Maven multi-module, hexagonal architecture — the `domain` packages have zero framework
imports (no Spring/Spark/Lettuce/Calcite/Jackson), enforced at build time by ArchUnit.

## How it answers a query

1. **Compile** — Calcite parses the SQL, extracts a canonical object (aggregates, group-by,
   filters, time range, step, HAVING/JOIN/DISTINCT signature) and a deterministic logic hash.
2. **Guard** — safety rules decide if the query is cacheable at all (must aggregate, must have a
   time column, no high-cardinality group-by, step compatible with the bucket grid — and no
   DISTINCT/HAVING/JOIN, which are not mergeable across buckets: the distinct set of two days is
   not the union of each day's distinct set). Anything unsafe bypasses straight to Spark.
   Correctness beats hit rate, always.
3. **Cover** — the coverage bitmap answers "which buckets are cached" in one fetch instead of one
   EXISTS per bucket. Cube subsumption can answer a coarse query from a finer cached shape.
4. **Fetch + fill** — cached buckets (MGET) and the Spark gap query run in parallel on virtual
   threads.
5. **Merge** — the columnar merge engine combines partial buckets: dictionary-encoded group keys
   packed into primitive arrays, open-addressed hashing, tight `double[]` loops. AVG is never
   stored — SUM and COUNT are, and the divide happens once at the end.

## Modules

| Module | Contents |
|---|---|
| `lib_identity` | Framework-free value objects: `TenantIdentifier`, `SchemaVersion`, `LineageHash`, `QueryHash`, `PolicyVersion` |
| `lib_cache` | The cache domain + runnable engine: canonical query object, bucket math + pyramid, logic hashing, safety rules, columnar merge (`domain/merge/columnar`), AVG reconstruction, cube subsumption + consistency verifier, DataSketches HLL/KLL buckets, warming queue + popularity tracker + Markov predictor, coverage bitmaps, `ResultFrame` + Arrow/zstd serialization, in-memory + Lettuce/Valkey backends, `CacheExecutionEngine` |
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

JDK 21+ (developed on JDK 22 with `--release 21`), Maven 3.9+. 335 tests, all green.

## The merge engine is columnar on purpose

Merging cached buckets used to walk `Map<String,Object>` rows — every cell boxed, every group key
a rebuilt string. On the canonical shape (30 buckets × 288 steps × 20 groups ≈ 170K rows) that was
millions of short-lived objects per merge; GC, not arithmetic, set the tail latency. The rewrite
follows what Spark's `HashAggregateExec` and its sketch code do:

- dimensions are dictionary-encoded to `int` ids once, group keys pack into a single `long`
- measures live in `double[]` accumulators indexed by an open-addressed `long→slot` table
- SUM/COUNT/MIN/MAX combine in tight primitive loops the JIT can vectorize
- `GlobalAggregateMerger` and `TimeSeriesBucketResampler` are thin row-object adapters over the
  same `ColumnarHashAggregator`, so the row-level API and its tests are unchanged

The combine function for every measure comes from the **parsed aggregate** (an alias→function map
built at canonicalization), never from sniffing column names.

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

## Known limitations

The bucket width is configurable (`cache_bucket_hours`); "bucket" below means one configured span.

1. **Global-aggregate dedup can undercount.** Non-time-series partial rows carry no bucket
   identity, so byte-identical rows from two different buckets collapse before summing. The
   overlap this dedup protects against cannot occur (the gap plan excludes cached buckets), so
   the fix is to drop or bucket-tag the dedup on this path.
2. **A time column is mandatory.** Tables without one cannot be bucket-cached and must bypass to
   Spark.
3. **`SELECT AVG(x), SUM(x)` can drop the user's SUM.** AVG reconstruction rewrites AVG to
   SUM/COUNT and may swallow a SUM the user also selected in its own right.
4. **`TimeBucketPyramid.assemble` is latent.** It mishandles the head partial and the current
   bucket; it is not wired into the live path and must be fixed before it is.
5. **Sketch blob serialization is deferred** — `datasketches-memory 3.0.2` refuses JDK > 21. The
   cross-bucket sketch *merge* works and is tested on any JDK.

## No ML in the engine

Warming and prefetch are deterministic statistics: a popularity tracker and a Markov next-query
predictor, both built and tested. "Gets faster the more you use it" is measured frequency, not a
learned policy.

## Next

Per `ARCHITECTURE.md` §12 and `FULL_PROJECT_PLAN.md`: the auto-profiler, the service shells
(api_gateway, query_service, spark_orchestrator), the live Spark/Delta adapter wiring, the zone-
sketch sidecars (plan Appendix J.3), and the Appendix L track (CDF bucket patching, per-family
zstd dictionaries, hot-bucket fission).
