# Cascada — Implementation Status

This file tracks **what is built and tested** in `cascada/` versus **what the plan
([`../FULL_PROJECT_PLAN.md`](../FULL_PROJECT_PLAN.md)) still describes as future work.**
It is the honest "done / not done" map — read it before assuming a capability exists.

> Cascada is the **general, open-source (Apache-2.0)** engine. The Python `data_collector/` is the
> *old, customized-per-project* reference: Cascada inherits its **logic** (merge math, resample,
> hashing, warming, gap analysis) but **never** ports project-specific customizations. See
> "General-engine principle" below.

Test status: **`mvn -pl lib_cache -am test` → 111 tests, 0 failures.**

---

## ✅ Implemented & tested (`lib_cache`)

| Capability | Class(es) | Tests | Plan ref |
|---|---|---|---|
| Canonical query object + logic hashing (fixed-step strategy, order-independent) | `QueryHashGenerator` | `QueryHashGeneratorTest` | §8.12 |
| Time-bucket math (head/body/tail, configurable bucket seconds) | `TimeBucketCalculator` | `TimeBucketCalculatorTest` | §8.14 |
| Bucket pyramid (5min→hour→day, complete-bucket rule) | `TimeBucketPyramid`, `BucketLevel` | `TimeBucketPyramidTest` | §8.14 |
| Merge math: global-agg map-reduce, exact-dup dedup (RC3), AVG reconstruction (RC4) | `GlobalAggregateMerger`, `AverageReconstructionService`, `FrameMergeService` | `MergeMathTest` | §8.18 |
| Time-series resampling (coarser-only, caller-driven step) | `TimeSeriesBucketResampler` | `MergeMathTest`, `TimeBucketPyramidTest` | §8.14 |
| Safety-rule bypass guardrails | `*Rule` + registry | `SafetyRulesTest` | §8.18 |
| **Cube subsumption (roll-up / filter-down / best-subsumer)** | `CubeSubsumptionPlanner`, `QueryShape`, `CachedShapeEntry` | `CubeSubsumptionPlanner*Test` | §8.12, App. I |
| **Cube data-inconsistency guard** | `CubeConsistencyVerifier` | `CubeConsistencyVerifierTest` (10) | §8.12 |
| Cache backends (in-memory + Valkey/Redis via Lettuce) | `InMemoryBlobCacheBackendAdapter`, `ValkeyCacheBackendAdapter` | `CacheAdministrationTest` | §8.16, §8.17 |
| Cache size + flush admin | `CacheAdministrationService`, `CacheSizeReport`, `CacheScope` | `CacheAdministrationTest` | §8.17 |
| **Cache value serialization — portable codec + zstd** | `PortableFrameSerializer` | `PortableFrameSerializerTest` | §8.16 |
| **Cache value serialization — Apache Arrow IPC + zstd** | `ArrowResultFrameSerializer` | `ArrowResultFrameSerializerTest` (4) | §8.16 |
| **Spark + Delta executor (THE production executor)** | `lib_spark`: `SparkDeltaQueryExecutor`, `SparkSessionConfigBuilder` | `SparkSessionConfigBuilderTest` (6) | §5, §6 |
| **Engine assembler + runnable entrypoint** | `app`: `CascadaEngine`, `CascadaApplication` | `CascadaEngineTest` (2) | §4.3 |
| Cache execution engine: gap analysis, EXISTS→MGET, **partial-hit (cache + Spark gap merge)** | `CacheExecutionEngine` | `CacheExecutionEngineTest`, `CacheCorrectnessSimulationTest` | §8.18 |
| Warming: Layer-1 queue + Layer-2 popularity + Markov speculative, bucket-by-bucket | `WarmingOrchestrator`, `WarmingQueue`, `QueryPopularityTracker` | `WarmingOrchestratorTest`, `WarmingConsistencySimulationTest` | §8.15 |
| Sketches (HLL distinct, KLL quantile) — in-process merge | `HyperLogLogDistinctCounter`, `KllQuantileEstimator` | `SketchMergeErrorBoundTest` | §8.13 |
| SQL canonicalization + gap rewriting (Calcite) | `lib_sql` | `lib_sql` suite | §8.18 |
| Spark config derivation | `lib_spark_config` | `lib_spark_config` suite | §6 |

### How the new pieces fit (this session's additions)

- **`ArrowResultFrameSerializer`** — the language-neutral production serializer the port contract
  always named. Same blob envelope (`[4-byte len][zstd lvl-9]`) and same `ColumnType` domain as
  `PortableFrameSerializer`, so it is a **true Liskov substitute** (proven in test): the in-memory
  backend, the Valkey backend, and size accounting behave identically with either one injected. Arrow
  is columnar + cross-language, so a bucket written here is readable by Python (pyarrow), Spark, and
  DuckDB without a bespoke codec.
- **`SparkDeltaQueryExecutor` (THE executor)** — the production `SparkQueryExecutorPort`: a real Spark
  3.5.x SparkSession with Delta extensions, running `spark.sql()` over Delta tables and mapping the
  `Dataset<Row>` to `ResultFrame`. **Local and cluster are the same code — only the
  `SparkSessionConfig` (master + spark.json) differs**, exactly like the reference `spark_manager.py`.
  Spark/Delta are `provided` (the cluster image supplies them); the pure `SparkSessionConfigBuilder`
  carries the unit-test coverage (Delta always on, env > spark.json > defaults, k8s-vs-local = master).
  Gluten/Velox acceleration is a Spark **config + image** concern layered into this same session — see
  the plan's §5; there is no separate Java for it.
- **`CascadaEngine` + `CascadaApplication`** — the composition root: wires lib_sql (translate +
  canonicalize) → lib_cache (safety + cache engine) → the injected executor. `main()` wires the **real**
  Kubernetes Spark+Delta executor + Valkey backend (run inside the Spark image / cluster).
- **`CubeConsistencyVerifier`** — before any cube roll-up is served, an independent direct-compute
  oracle re-derives the answer from the same candidate frame and compares cell-by-cell; any
  disagreement (tampered cell, fabricated/missing group, missing AVG ingredient, holistic aggregate,
  absent grouped column, non-numeric measure) → bypass to Spark instead of a silently wrong number.

### Correctly understood (was a prior misconception, now verified in code)

- **Sparse / partial cache hits are the norm, not the exception.** `CacheExecutionEngine.execute`
  has three paths: (1) nothing cacheable → Spark; (2) zero buckets present → Spark; (3) **partial hit
  → fetch the cached buckets via MGET *and* the Spark gap in parallel, then merge.** The merge input
  is `cache frames + a Spark gap frame`. Never "always from cache."
- **The bucket step is configurable, not 5 minutes.** `fixed_step_seconds` / `bucketSeconds` /
  warming interval are all config-driven. 300s is only the pyramid *base*; warming can run on a 6h /
  20h cadence, and the per-query time grouping comes from the user's SQL.

---

## 🔜 Not yet implemented (plan describes; code does not exist)

| Capability | Plan ref | Note |
|---|---|---|
| **Live Spark+Delta integration test** (real cluster read) | §5, §6 | `SparkDeltaQueryExecutor` is implemented and compiles; it is exercised against a real Delta table in the Kubernetes cluster (the build env here has no cluster), not in the unit build. |
| **Gluten/Velox acceleration** | §5 | A Spark **config + container-image** concern (plan §5), not separate Java; the `SparkSessionConfigBuilder` is where the gluten keys are layered in by the operator. |
| **CDF incremental cache maintenance + Delta cold tier** | §8.16 | A Delta-backed cold-tier `CacheBackendPort` and Change-Data-Feed invalidation are pending. |
| **Cube planner wired into the engine** | §8.12 | `CubeSubsumptionPlanner` + `CubeConsistencyVerifier` exist and are tested, but `CacheExecutionEngine` does not yet consult them before the Spark gap path. |
| **REST + gRPC/Arrow API surface** (JSON small, Arrow/Parquet bulk, gRPC+protobuf streaming for Angular) | §10 | Not built — Angular frontend will call these; 1M-row results must stream as Arrow/gRPC, never plain JSON. |
| **Multi-tier cost-aware eviction & cold spill** (RAM→NVMe→object store) | §8.16 | Backends exist; the tier router + eviction policy do not. |
| **Sketch blob persistence on JDK > 21** | §8.18 | Merge works on any JDK; serialization round-trip disabled on JDK 22 (datasketches-memory 3.0.2). |
| **Fabric-style cluster lifecycle** (create/stop/restart/resize via templates) | §11 | Not started — the admin console + cluster controller. |
| **Cluster dashboard / Spark metrics → frontend** | §11.10 | Not started. |
| **Materialization Studio, NL→SQL, semantic catalog** | §11.8, §9 | Not started. |
| **REST API surface** | §10 | Not started in `cascada/` (the Python `data_collector` has its own FastAPI). |

---

## General-engine principle (do not violate)

1. **No per-project customization in the engine.** Inherit logic from the Python reference; never port
   project-specific behaviour (e.g. a hard-coded sampling step or a per-deployment "write each point").
2. **Time-series step is caller-driven.** Store at one fixed internal step; resample **coarser only**
   on read; the coarser step comes from the query / front-end, never an engine-side assumption. Never
   invent a finer resolution than what is stored (that would be data fabrication).
3. **Correctness over speed at the boundary.** When unsure a result is correct (cube roll-up, holistic
   aggregate), bypass to Spark. Speed comes from the cache (serving buckets instead of recomputing) and
   from Gluten/Velox accelerating the Spark gap — never from relaxing a correctness check.
4. **Spark + Delta on Kubernetes is THE engine.** Everything runs through `SparkDeltaQueryExecutor`;
   local and cluster differ only by `SparkSessionConfig`. There is no second execution engine.
