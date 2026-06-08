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

## Next phases (not yet implemented)

Per the build order in `ARCHITECTURE.md` §12: the auto-profiler (`profiling_service`), the RL policy
server (bandit → offline RL), the Ed25519 license server + signed usage journal, the Spring Boot
service shells (`api_gateway`, `query_service`, `spark_orchestrator`), the real Spark/Delta execution
adapter, and the frontend. The framework-free core and runnable engine built here are the seam those
plug into.

**Environment note:** sketch *blob serialization* (HLL/KLL round-trip) is deferred because
`datasketches-memory 3.0.2` refuses JDK > 21 (this repo builds on JDK 22, release 21). The live
cross-bucket sketch *merge* — the actual value — works on any JDK and is tested.
