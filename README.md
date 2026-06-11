# Cascada — Java 21 implementation

> The lakehouse query engine that gets faster the more you use it.

This is the Java 21 / Maven multi-module implementation. It follows the **hexagonal / ports-and-adapters** doctrine:
the **domain core is framework-free** (zero Spring / Spark / Lettuce / Calcite / Tablesaw /
Jackson imports in `domain` packages), enforced at build time by **ArchUnit**.

The Python under `data_collector/` and `data_explory/` is **reference only** — its proven logic is
re-implemented here in Java (per the §B.5 reuse map), not run in production.

## Modules

| Module | What it contains | Ported from |
|---|---|---|
| `lib_identity` | Framework-free value objects: `TenantIdentifier`, `SchemaVersion`, `LineageHash`, `QueryHash`, `PolicyVersion` | plan §8.4 |
| `lib_cache` | The smart-cache **domain core + a runnable engine**: canonical query object, day-bucket math + **hierarchical pyramid** (`5min→hour→day`), fixed-step logic hashing, the safety guardrails + registry, the merge math (AVG reconstruction, exact-duplicate dedup, epoch-aligned resampling), **cube subsumption** (roll-up/filter-down), **DataSketches HLL/KLL** sketch buckets, **warming queue + popularity tracker + Markov predictor**, `ResultFrame` + ports, **Arrow-ready/zstd serialization**, in-memory + **Lettuce/Valkey** backends, and the `CacheExecutionEngine` (two-phase EXISTS→MGET, virtual-thread parallel cache+Spark fetch, three fast paths) | `data_collector/src/core/smart_cache/` |
| `lib_spark_config` | The pure `deriveSparkConfigurationFromThreeKnobs(...)` zero-config derivation, with a **golden test** that reproduces `data_collector/src/spark.json` | plan §6.6, Appendix G |
| `lib_fabric` | The **Fabric-style cluster deployment layer** (plan §6.7). Real `kubectl apply -f`-shaped YAML templates under `src/main/resources/fabric/` (ServiceAccount + expanded RBAC, HDFS ConfigMaps, executor pod template, `spark.json`/log4j ConfigMaps, driver Deployment + headless Service — modeled on `deployment-review.yml`); `ClusterValues` fills them from `CASCADA_*` **environment variables** with reference defaults; `ClusterManifestRenderer` substitutes the templates; and the `FabricClusterDeployer` adapter applies / stops / restarts / deletes the cluster through the **Fabric8 Kubernetes client** (no Spring — the client auto-resolves the API server from the in-cluster ServiceAccount token or local kubeconfig). Tested end-to-end against the Fabric8 mock API server. | plan §6.7 |
| `lib_sql` | **Apache Calcite** SQL compiler: canonical-object extraction (AVG→SUM/COUNT, group-by/filter/time-range/composite-alias/time-series detection), **MySQL→Spark dialect translation** (the 100+ function-mapping table via `MySqlToSparkFunctionTranslator`), the **gap-query builder**, and the **cache-correctness simulation gate** (cache path == direct compute). Calcite provides the full navigable/transformable `SqlNode` AST and a dialect-aware unparser targeting Spark SQL. | `cache_component_adapter.py`, `sql_rewriter.py`, `data_explory/all_plans.md`, `in_memory_simulation.py` |

### "Caching just works" — no hot-view / hot-batch knob

Per the user's instruction and plan §8.11, the reference engine's microservice-specific `uses_hot_batch` /
"hot view" concept is **removed**. The customer never selects a caching mode; the generalisation is a
`stalenessToleranceMillis` on each query that informs warming and the bucket pyramid but never gates caching.

## What the tests prove

74 tests, all green, including the correctness invariants from `CACHE_INCONSISTENCY_EXPLAINED.md`:

- **RC4 (AVG):** `AverageReconstructionService` divides summed `SUM`/`COUNT`, never averages averages
  (day1 `sum=20,count=2` + day2 `sum=400,count=4` → `70`, not `55`).
- **RC3 (duplicate inflation):** `GlobalAggregateMerger` collapses byte-identical rows arriving from
  both cache and Spark before aggregating.
- **RC2 / RC5 (epoch alignment / precision collapse):** `TimeSeriesBucketResampler` floors epoch-second
  longs to step buckets (1900s → `85500`, `87400`, never `0`).
- **Hash determinism:** the logic hash is independent of the time range and of clause ordering, but
  sensitive to intent (aggregates, group-by, filters, step, composite aliases, source signature).
- **Zero-config golden:** the derivation reproduces `spark.json` key-by-key for the reference knobs
  (`executor.memory=18g`, `limit.cores=19`, `parallelism=254`, …) and never oversubscribes RAM.
- **Hexagon intact:** ArchUnit fails the build if any framework import enters a `domain` package.

## Build

```bash
cd cascada
mvn -N install            # install the parent BOM
mvn test                  # build + run every module's unit tests
```

Requires JDK 21+ (built/tested on JDK 22 with `--release 21`) and Maven 3.9+.

## Integrating Apache Gluten + Velox (acceleration layer)

Gluten is **config-layered onto the same SparkSession** the engine already builds (`SparkSessionConfigurationAssembler`) — it is not a second engine and needs no engine-code change. Plan §5 has the strategy; this is the concrete how-to.

**1. Jars / image.** Use a Spark 3.5.x image with the Gluten bundle, or add to `spark.jars`:
`gluten-velox-bundle-spark3.5_2.12-<linux-arch>-1.3.0.jar` (Gluten requires Linux for Velox; on Windows/macOS dev boxes Gluten stays off and everything still runs vanilla).

**2. Required Spark configs** (these belong in the derived config from `lib_spark_config`, as a `GLUTEN` overlay on top of the three-knob derivation):

```properties
spark.plugins=org.apache.gluten.GlutenPlugin
spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager
# Velox runs OFF-HEAP — this is the critical memory re-split (plan §6.6):
spark.memory.offHeap.enabled=true
spark.memory.offHeap.size=<~60% of executor RAM>        # e.g. RAM=16g → 9g
spark.executor.memory=<~30% of executor RAM>             # e.g. 6g (shrink the JVM heap!)
spark.executor.memoryOverhead=<~10%>                     # e.g. 1g
spark.gluten.enabled=true                                 # the per-query kill switch
```

The off-heap/heap re-split is the part people get wrong (OOM in production): when Gluten is ON, the heap must *shrink* because Velox does the heavy lifting off-heap. `SparkMemorySplit` is where this derivation lives — add a `glutenEnabled` flag that flips the split from heap-major to offheap-major. Keep `spark.sql.adaptive.*` on (Gluten is AQE-compatible).

**3. Fallback policy.** Any Gluten failure → retry the query once with `spark.gluten.enabled=false` and record the failing plan shape in metadata (plan §5.2.4). Operator coverage varies per release — mixed plans still work, they just fall back per-stage (watch for `VeloxColumnarToRowExec` boundaries in the UI; many of them = mostly-fallback plan).

**4. Delta caveats.** Pin Gluten/Velox/Delta versions together; deletion-vector reads are the moving target — the golden-SQL corpus must pass with Gluten ON and OFF before an image bump ships.

**5. Verification.** `EXPLAIN` should show `VeloxNativeScan` / `^Velox*` operators; the micro-benchmark harness (plan §5.2.5) gates each image bump at ≤10% regression on any corpus query.

## Known limitations & cache-correctness caveats (read before trusting a cached number)

These are documented, not silently shipped. Several are **latent** (the code path is not yet wired);
all are listed so an operator knows the exact boundary of "the cache is always correct." The bucket
size below is **configurable** (`cache_bucket_hours`, e.g. 5h in the reference `deployment-review.yml`,
not necessarily 24h), so "day" here means "one bucket of the configured width."

1. **Warming a partial bucket as if it were full.** `WarmingOrchestrator` truncates the last bucket to
   `warmEnd` but stores it under the full-bucket key and marks it covered. With `warmEnd = now` mid-bucket,
   every later query that covers that bucket undercounts, and the `EXISTS`-skip prevents self-healing
   (the bucket looks present, so it is never recomputed). Warming must stop at the last **complete**
   bucket boundary, never `now`.
2. **Global-aggregate dedup can undercount.** Non-time-series rows carry no bucket identity, so a
   byte-identical partial row from two different buckets (e.g. `{app=X, COUNT=1}` on Monday and Tuesday)
   collapses to one row before the SUM. The overlap this dedup guards against cannot actually occur (the
   gap plan already excludes cached buckets), so the dedup is both unnecessary and lossy here.
3. **A time column is mandatory for bucketed caching.** The engine buckets by a `ts`/time column. A table
   with no time/`ts` column cannot be bucket-cached correctly — such queries must bypass to the Spark
   (Gluten/Velox) layer rather than be force-fit into the bucket pyramid.
4. **MIN/MAX merged as SUM when aliased.** The cross-bucket combine function is chosen by **sniffing the
   column name** (`max(...)`/`max_…`). `MAX(latency) AS peak_latency` loses the `max` signal and gets
   **SUMmed** across buckets. Worse, `CubeConsistencyVerifier` uses the *same* name heuristic, so the
   "independent" oracle confirms the wrong answer instead of catching it. The combine op must come from
   the parsed aggregate, never the output alias.
5. **`HAVING` is dropped from canonicalization and the hash.** A query with `HAVING` and one without
   share a cache entry, so a `HAVING`-filtered result can be served for an unfiltered query (and vice
   versa). `HAVING` must be part of the logic hash.
6. **`JOIN ... ON` conditions and `DISTINCT` are not hashed.** Same tables with different join keys (or
   `SELECT` vs `SELECT DISTINCT`) collide on the same cache key. Both must feed the hash.
7. **No safety rule requires an aggregation.** A plain row-fetch `SELECT` passes every bypass guardrail,
   and the merge then fabricates a `GROUP BY` + `SUM` the user never wrote. A "must be an aggregate to be
   cacheable" rule is missing.
8. **`SELECT AVG(x), SUM(x)` loses the user's SUM.** AVG reconstruction rewrites `AVG` to `SUM/COUNT`;
   when a real `SUM(x)` is also selected, the reconstruction step can drop the user's own SUM column.
9. **`TimeBucketPyramid.assemble` loses the head partial / overcounts the current bucket** (latent — the
   pyramid assembly is not yet wired into the live path). Until it is, do not rely on multi-resolution
   roll-up through `assemble`.

## Next phases (not yet implemented)

**No AI/ML in the engine.** Cache warming and prefetch are driven by deterministic statistics only —
the popularity tracker and the Markov next-query predictor (both already built and tested). There is no
RL policy server and no NL→SQL model; "the cache gets faster the more you use it" is a measured-frequency
effect, not a learned one.

Per the build order in `ARCHITECTURE.md` §12: the auto-profiler (`profiling_service`), the Ed25519
license server + signed usage journal, the Spring Boot service shells (`api_gateway`, `query_service`,
`spark_orchestrator`), the real Spark/Delta execution adapter, and the frontend. The framework-free core,
the runnable engine, and the **`lib_fabric` cluster-lifecycle templating engine** built here are the seam
those plug into.

**Environment note:** sketch *blob serialization* (HLL/KLL round-trip) is deferred because
`datasketches-memory 3.0.2` refuses JDK > 21 (this repo builds on JDK 22, release 21). The live
cross-bucket sketch *merge* — the actual value — works on any JDK and is tested.
