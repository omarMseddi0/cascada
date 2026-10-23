> Cleanup note (18 October 2026): standalone review probes and generated `codex-*.log` / benchmark JSON artifacts referenced below were deleted after the untracked-file audit. References describe historical evidence, not files still present. Findings and recorded measurements remain historical; current verification is recorded in `UNCOMMITTED_COMMIT_PLAN.md`.

# Cache core fixes and second parity review

Reviewed and repaired on 2026-10-16 (Europe/Paris). This report supersedes the open-finding status in `cache-python-parity-review-codex.md` and `freshness-review-codex.md`. Those documents describe the earlier snapshot; their reproduced bugs must not be read as current failures.

## Scope and result

The Python `src/core/smart_cache` module (all 20 source/documentation files listed in the baseline review), `cashing_system_technical_details.md`, and `SMART_CACHE_README.md` were personally read by the primary agent. Java cache behavior was compared with that source and the two root documents. `review_claude.md` was excluded. The working tree already contained extensive unrelated changes; no reset, commit, deployment, or replacement of that work was performed.

Cascada retains the Python cache's core: time-independent query identity, complete bucket storage, exact head/tail queries, parallel cached/gap retrieval, typed aggregation, SUM/COUNT AVG ingredients, deferred ordering/limit, request-side read-only payload access, background-only bucket writes, and deduplicated recent/popular warming. The fixes below restore missing parity and repair freshness/schema errors. This is a cache-module review, not certification of every subsystem of Cascada.

## Core comparison after repairs

| Contract | Final behavior | Relationship to Python |
|---|---|---|
| Identity and storage step | A configured fixed storage step; user display steps reconstruct from the same ingredients. Hash semantics version 4 isolates previous meanings. | Restores cross-granularity reuse. |
| Cold requests | Zero bucket hits execute the original physical query, including AVG. Requests do not write bucket payloads. | Preserved original fast path. |
| Partial/full hits | Complete cached buckets plus exact Spark gaps; missing/expired reads are recovered. Final public columns are projected after reconstruction. | Preserved core; stricter schema checks. |
| AVG | Store SUM/COUNT/type ingredients; compute final AVG after combining. Empty grouped results expose the requested alias and type. | SUM/COUNT is intentional; internal-column leakage is repaired. |
| Source policy | HOT_BATCH sources can cache; HOT_VIEW-only queries execute through Spark. Source matching uses registered physical paths. | Restored policy. |
| Warming votes | Eligible queries vote on hits and misses; the positive minimum-range exception can warm complete buckets. | Preserved; shared eligibility logic avoids a second policy implementation. |
| Historical validation | Compare the stored window, propose the next sliding window, acknowledge only after successful refresh. | Actual Python is sliding, despite the root README's frozen-epoch description. Failure handling is improved. |
| Warming failures | Failed patterns are requeued; baseline is not advanced; invalidation prevents old in-flight writes from restoring a flushed generation. | Repairs a shared failure class. |
| Cube accelerator | Enabled for supported global aggregates, with verified rollup, source/projection checks, fixed TTL, revision validation, and supporting-bucket expiry limits. | Java extension integrated into the bucket lifecycle. |

## Concrete repairs

1. **Cube reuse and freshness.** The production factory enables the catalog. Fine-grained grouped answers can roll up to a requested subset with the correct output order. Reads validate the backend revision; registration rejects concurrent revisions. Fixed expiry does not slide on reads and cannot exceed supporting buckets' remaining TTL. Flush/backfill invalidation clears catalog and bucket state. AVG/composite/time-series shapes continue through the ordinary bucket path.
2. **Refresh integrity.** Canary acknowledgement follows successful warming, rather than preceding it. Forced refresh evicts superseded payloads and clears scoped Cascada bucket keys/coverage. In-memory and Valkey generation-checked writes reject computations started before invalidation. Cancellation propagates.
3. **Correct final schemas.** Empty AVG is reconstructed; output projection removes internal AVG/composite ingredients. Cold AVG executes native Spark SQL. Decimal AVG preserves its category. Arbitrary decimal composite arithmetic stays with Spark because precision/scale cannot be inferred from the cache's coarse column type.
4. **Fixed-step parity.** Hash normalization and warmer storage normalization align 300-second/hourly display variants. Storage rewriting touches only recognized projected/grouped time buckets; business predicates and time expressions inside measures remain intact.
5. **SQL boundaries and alias safety.** Arithmetic composites have private collision-resistant ingredient names. `lib_sql` compiles its existing Calcite tree into a SQL-independent typed arithmetic plan; `lib_cache` binds/evaluates that plan. The custom `CompositeExpression.java` text parser was removed. No second SQL grammar or reverse module dependency remains. Missing ingredients reject cache reconstruction and allow the authoritative fallback.
6. **Scoped SQL and safe wrapper reuse.** Registered table/column mappings respect joins, CTEs, derived scopes, wildcard-derived outputs, and explicit column alias lists. Proven distributive wrappers can flatten; other SQL executes through Spark. New features do not justify speculative bucket arithmetic.
7. **Restart migration.** Persisted popularity SQL is recanonicalized and rehashed using current semantics and the configured fixed step. Current-process registered keys are retained. Resolved current identities are remembered for retries and deduplicated. Production warming rechecks current source/safety policy, including cache-disabled configuration.
8. **Bounded overhead.** Coverage storage has family/byte limits and refuses pathological bitmap ordinals before allocation. Cube admission has a conservative 64 MiB budget. Popularity writes accumulate in a bounded local map and flush in batches. Typed incremental merging avoids retaining every decoded bucket frame. Expensive cube computation runs outside the catalog metadata lock.
9. **Request latency isolation.** Query observation uses a short canonical-registry lock; background Spark warming holds a separate cycle lock. A concurrency regression holds the warming executor blocked and proves a request can record its observation before that work is released.

## SOLID and duplication

SQL parsing, dialect semantics, aggregate extraction, and formula compilation belong to `lib_sql`. Cache storage, typed combining, and evaluation belong to `lib_cache`; the shared formula records are domain contracts, independent of Calcite. Existing constructors remain compatible. Updating formula text invalidates a previously compiled plan instead of silently evaluating stale operations. Warming eligibility is shared with the read service through the safety registry. This change removes the duplicate parser without moving Spark/Calcite dependencies into the cache module or inventing an unnecessary generic expression framework.

## Validation

The final reactor run finished at **2026-10-16 01:02:21 +02:00**, with **BUILD SUCCESS** for all seven modules. Maven reported **691 tests, zero failures, zero errors, 26 skipped** (665 executed). Property-test engine totals in the console include invocations that are not separately represented by XML test-case totals; the numbers here are the reactor console totals.

| Module | Reported tests | Skipped |
|---|---:|---:|
| Identity | 13 | 0 |
| Cache | 324 | 0 |
| Spark configuration | 23 | 0 |
| Fabric | 35 | 0 |
| SQL | 193 | 0 |
| Spark executor | 30 | 1 |
| Application | 73 | 25 |

The three real Valkey tests executed successfully. `CacheSqlSparkIntegrationTest` executed successfully (17.86 seconds). `WarmingFreshnessTest` passed all seven checks, including persisted-hash migration, current source policy, and observation during blocked warming; `WarmingRetentionTest` passed its concurrent snapshot checks. `git diff --check` passed (only an existing LF/CRLF conversion notice).

Skipped checks include the separate Spark serialization, 14-case local consistency suite, large-result, shared-session, and nine process-level Valkey/Spark tests. The active local Spark parity test is distinct from those skipped suites. Native Gluten/Delta and production-scale validation are not claimed.

The final synthetic merge run used ten 100,000-row input frames, 1,000 groups, three warmups and seven measured trials, with value/count correctness checks: **27.230 ms median**, **36,724,606 input rows/s**, **128 MiB heap limit**, **27.00 MiB sum of heap-pool peaks**, **0 ms measured GC during trials**. These are local microbenchmark measurements, not an end-to-end speed guarantee. The disposable Valkey test container was stopped after validation.

The reproducible reactor command is:

```powershell
mvn '-Pvalidation,local-spark' test '-DforkCount=0' `
  '-Dcascada.cache.spark.integration=true' `
  '-Dcascada.valkey.url=redis://127.0.0.1:16379'
```

Java 21 and Arrow/Spark module opens were supplied through `MAVEN_OPTS`; builds use isolated `target/validation/target` directories. Evidence: `codex-final-validation.log` and `codex-merge-benchmark.json`.

The Spark test compares direct Spark answers with cold, mixed, and fully warmed paths, checking column names/types and values. It covers decimal AVG, composites, display-step reuse, safe wrappers, empty AVG, CTEs/joins, mapped wildcard-derived outputs, and explicit column alias lists. The real Valkey tests cover independent adapters, revisions, TTL expiry, batched popularity, and stale warming-generation rejection. Unit/architecture tests cover failure acknowledgement, signatures, memory bounds, source policy, aliases, rollup, hash identity, and SQL/cache dependency direction.

## Practical limits

These tests use local Spark 4.1.1 and a disposable Valkey instance. The Spark parity fixture substitutes temporary views for physical Delta paths; it does not validate native Gluten operators, multi-host execution, or real Delta transactional scans. The separate large-result/native/process suites remain opt-in, and skips must be reported. No production deployment was performed.

Freshness is bounded by configured bucket TTL and scheduled canary checks. It is not a claim of transactional snapshot isolation across arbitrary concurrent Delta mutations. The canary only detects changes visible to its configured validation query/window; aggregate canaries can miss offsetting changes. The sliding model is deliberate parity with actual Python.

The local merge benchmark measures typed in-process aggregation only, with retained synthetic inputs, 1,000 groups, and a 128 MiB JVM heap. Pool peaks are not process RSS; output cardinality and strings/decimals change memory cost. It proves neither a cluster throughput SLA nor an end-to-end speed multiplier. Spark still materializes a complete `ResultFrame`; an unbounded output contract cannot have constant driver RAM.

The separate `spark-retrieval-review-codex.md` explains the official Spark/Delta sources: direct `InternalRow` collection already avoids public external `Row` conversion; public Java has no PySpark-style Arrow collection switch. Iterator/Arrow/columnar alternatives require representative benchmarks. No unmeasured “10x” retrieval claim or collector replacement was introduced. Numeric reconstruction is exercised under the runtime's legacy arithmetic profile (`spark.sql.ansi.enabled=false`); ANSI overrides require separate parity qualification.
