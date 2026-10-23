> Cleanup note (18 October 2026): standalone review probes and generated `codex-*.log` / benchmark JSON artifacts referenced below were deleted after the untracked-file audit. References describe historical evidence, not files still present. Findings and recorded measurements remain historical; current verification is recorded in `UNCOMMITTED_COMMIT_PLAN.md`.

> Historical review snapshot. Fix status and final validation are superseded by [the 2026-10-16 fix review](CACHE_CORE_FIX_REVIEW_2026-10-16.md).

# Cascada cache: Python parity and correctness review

Reviewed personally by the primary agent on 2026-10-15 (Europe/Paris). This is a review, not a code repair. No production source was changed. The working tree contains extensive existing edits and was shared with another coding session. `review_claude.md` is excluded from all conclusions. Active subagents were stopped when the user requested that the primary agent personally read the Python module.

## Verdict

The Java cache retains the main Python architecture: canonical query intent, time-independent identity, bucket keys, full-bucket reuse, exact partial edges, parallel cache/Spark gap fetches, SUM/COUNT AVG ingredients, typed combine operations, deferred ordering/limit, warmer-only bucket writes, deduplicated warming votes, and persistent popularity-based warming. It is not an unrelated implementation with only similar class names.

It is also not a complete behavioral port. Fixed-step cross-granularity reuse, HOT_BATCH-only eligibility and the range of SQL shapes cached have changed. The cube accelerator adds a separate answer cache without the bucket layer's expiry. Several freshness issues remain, but some were inherited from Python or contradict documentation in BOTH implementations. These distinctions matter: a passing test can accurately test a behavior that is different from the supplied README.

## What was personally read

Both requested root documents were read fully: `data_collector/src/cashing_system_technical_details.md` and `data_collector/src/SMART_CACHE_README.md`.

Every file in `data_collector/src/core/smart_cache` was then read fully, including comments and helper code, in sequential chunks that avoided output truncation:

| File | Lines |
|---|---:|
| __init__.py | 22 |
| auto_warmer.py | 754 |
| cache_component_adapter.py | 120 |
| cache_execution_engine.py | 406 |
| cache_hashing.py | 145 |
| cache_manager.py | 207 |
| cache_safety_checker.py | 14 |
| cache_serialization.py | 64 |
| data_cache_adapter.py | 488 |
| domain.py | 155 |
| in_memory_simulation.py | 440 |
| interfaces.py | 145 |
| merging.py | 561 |
| query_tracker.py | 335 |
| safety_rules.py | 356 |
| sql_rewriter.py | 639 |
| time_utils.py | 124 |
| warming_queue.py | 92 |
| CACHE_INCONSISTENCY_EXPLAINED.md | 647 |
| QUERY_TRACKER_README.md | 190 |

These 20 files are the complete source-directory inventory returned by `rg --files`; no claim is made to have read every file in the much larger data_collector project. Related integration code outside this module remains a separate scope.

The primary agent also personally traced the corresponding current Java entry services, factory wiring, canonicalizer, classifier, translator, safety registry/rules, hashing, gap rewriting, execution engine, bucket calculator, warmer, data-change detector, merge/reconstruction, cube catalog, popularity adapter, Spark collector, mapper and Arrow serializer. Spark runtime performance was researched separately and its report was read and checked against the current collector and official Spark/Delta sources.

## Confirmed remaining issues and parity departures

### 1. P1: Cube answers bypass the bucket freshness lifecycle (new Java accelerator)

`lib_cache/.../application/service/CacheExecutionEngine.java:129-133` consults the cube before planning/checking bucket data. `application/service/cube/CubeShapeCatalog.java:84-103` stores whole-window results with no TTL, source generation or age. LRU is a capacity bound, not a freshness bound. Completed-window registration is a good existing fix, but historical Delta data can still change. The optional canary can clear the catalog on a detected change; that is not guaranteed when validation is absent, fails, or does not cover the changed history.

Direct reproduction in `docs/codex-review-probes/CubeFreshnessProbe.java`: authoritative value 100 is returned once and catalogued; change the authoritative value to 200; repeat the historical query with an empty bucket backend. It still returns 100 and total Spark calls remain 1. This accelerator can retain an answer after the Redis bucket TTL has expired, defeating the baseline's eventual refresh mechanism.

Recommendation: a common freshness generation/expiry across answer tiers, replacement of stale registrations, and explicit invalidation on relevant refresh. Merely verifying the arithmetic of an old cube cannot establish that its source data is current.

### 2. P1: Failed forced refresh loses its refresh obligation (shared failure class)

Java's `warming/DataChangeDetector.java:80-91` persists its new baseline before `WarmingOrchestrator.java:159` invokes bucket warming. A failed pattern is requeued at lines 197-202, but that queue entry does not retain `forceOverwrite=true`. The next comparison sees the acknowledged signature unchanged, so the warmer skips an existing old bucket at lines 280-291. A transient failed overwrite can therefore leave known-stale data until TTL or another source change.

The Python implementation defers `_save_validation_baseline` until after its warming loops, which is better ordering, but `_warm_single_pattern_sequential` and `_write_single_bucket` catch failures and do not report unsuccessful refreshes to the caller. It can therefore acknowledge a new baseline after failed writes as well. This is an unresolved failure class in both, not proof that Java corrupted formerly reliable Python behavior. Java requeues failed patterns, an improvement, but the missing forced-refresh state means the retry can still skip stale keys.

Recommendation: track the dirty generation until affected buckets are successfully refreshed, or invalidate them before they can pass EXISTS-skip. The current finding is control-flow evidence, not a real Spark/Valkey failure-injection test.

### 3. P2: Fixed-step identity does not produce cross-granularity reuse (confirmed Java parity gap)

Python `cache_hashing._normalize_projection_signature` replaces FLOOR bucket literals with `<STEP>`. The `/data` adapter removes the time bucket expression from the non-time group-by dimensions, while `generate_query_hash` supplies the configured fixed step. The Python warmer calls the engine without a user-step canonical argument, so its step rewriting targets fixed storage granularity. User gap fetches can use a coarser requested step as an optimization; these paths intentionally differ because only the warmer writes buckets.

Java sets the separate hash `step` field to 300, but `CalciteCanonicalObjectFactory.extractGroupBy` and `extractProjectionSignature` retain the full `FLOOR(ts/3600)*3600` expressions. `QueryHashGenerator` hashes those unnormalized fields. Java `GapQueryBuilder` does not rewrite the bucket step, and WarmingOrchestrator uses it directly. Consequently, the bucket payload and family remain tied to the original requested step.

`StepParityProbe.java` exercised current compiled classes:

```
step=300  hash=6f958c6ff8a85b80a0927d6778c84c16
step=3600 hash=3c33a10de879604a09ca0fc2f4288f0b
```

The generated daily SQL retained 300 and 3600 respectively. This avoids accidentally reading coarse data as fine data, so it is a reuse/cost departure rather than a demonstrated wrong-value bug. It contradicts the technical document's fixed-storage-step promise and current Python `/data` design.

Recommendation: normalize identity and write fixed-grain ingredients together, retaining the requested step only for final reconstruction. Do not change just the hash; that would let coarse payloads poison fine-grained requests.

### 4. P2: HOT_BATCH-only eligibility is deliberately absent (explicit policy departure)

Python's `HotViewOnlyRule.check` bypasses when metadata `uses_hot_batch` is false, and SafetyChecker registers it. Both requested READMEs call this a critical policy. Java `QueryMetadata.java:14-19` explicitly removes this concept; `SafetyRuleRegistry.java:15-18,32-40` intentionally omits the rule. Registered tables do not carry equivalent source eligibility here.

If Cascada intentionally generalizes the product to arbitrary Delta sources, this can be a reasonable product change. It is still not the same eligibility contract. Restoring it requires a source classification/eligibility policy, not a check for a Python directory name. It is not evidence of incorrect SUM/AVG mathematics.

### 5. P2: Empty grouped AVG leaks ingredient schema (reproduced; affects Java zero-hit path)

For `SELECT app, AVG(x) AS avg_x ... GROUP BY app` returning zero groups, Java rewrites AVG to private SUM/COUNT/type ingredients even on the direct AVG miss path. `IncrementalFrameMerge.java:45-49` returns an empty ingredient frame before reconstruction. `FrameMergeService.java:55-60` has an analogous all-empty early return.

`EmptyAverageProbe.java` output:

```
expected=[app, avg_x]
actual=[app, avg_x__cascada_avg_sum, avg_x__cascada_avg_count, avg_x__cascada_avg_type]
```

Python's `_reconstruct_avg_columns` also returns empty frames unchanged, so empty warmed ingredient schemas are not perfectly handled there either. However Python's zero-hit path runs the original physical SQL directly; Java's direct AVG miss path rewrites/merges ingredients, making that path exposed too. Preserve empty-result projection/type metadata rather than treating zero rows as a reason to skip public schema reconstruction.

### 6. P2: SQL cache coverage is narrower than the Python module (safe bypass, not wrong-result proof)

Python `data_cache_adapter` walks wrapped SELECT/UNION trees and extracts composite aggregate formulas; `sql_rewriter` injects gaps into innermost data-reading SELECTs; `merging` reconstructs arithmetic composites.

Java's current `hasMergeableProjection` only admits direct aggregate calls or supported projected group expressions. The tested `SUM(a)+SUM(b) AS total` query receives `UNSUPPORTED_MERGE_SHAPE`, and NonMergeableSqlFeatureRule bypasses it. A tested derived-table query is rejected with `derived-table time scopes cannot be safely rewritten for caching`. The factory accepts simple SELECT roots; UNION/CTE roots do not enter ordinary bucket caching. Logical translation additionally rejects nested SELECT scopes and multi-table FROMs.

Conservative bypass is preferable to incorrect decomposition, and some complex SQL is not mathematically mergeable. But supported Python-style composite and wrapped endpoint patterns are not fully ported. Also, translation errors occur before ExecuteLogicalQueryService's canonicalization fallback block: a logical query that cannot be translated is not automatically a successful direct Spark fallback. Do not advertise blanket support based on dormant gap-builder branches or merge helper classes.

Recommendation: enumerate supported shapes explicitly and port only those for which whole-window/direct and cached execution can be proved equivalent. Add mixed-hit tests through the actual logical entry point, not only direct merger fixtures.

## Important findings that are NOT Java regressions

- **Sliding validation:** Python `auto_warmer.py` explicitly performs the stored-window check, then re-baselines to the current window every successful cycle. Java does the same. Both differ from SMART_CACHE_README §4.4's frozen epoch/no-write-on-match requirements. `EpochProbe.java` proves two saves and three validation queries for an initial cycle plus an unchanged shifted cycle. Ordinary window movement does NOT itself trigger a false mismatch: both compare the stored window against its own prior signature first.
- **Warming on hits:** Python manager votes every non-bypassed query, and warming_queue documents hits as useful signals. Java's pre-execution vote is source parity, despite an older README path table saying full hits do not enqueue.
- **High-cardinality matching:** both implementations use exact membership for configured group-by column names. Java has not introduced that rule's case-sensitivity. Qualified names can still interact differently because the adapters represent identifiers differently. Treat normalization as hardening, not an established newly introduced regression.
- **Only bucket writes are warmer-owned:** the Python tracker writes popularity metadata in a background batch; Java queues asynchronous popularity commands per request. Neither implies that the request handler writes bucket payloads. Java's per-query network command volume is higher than the Python batched tracker and should be measured if popularity recording is significant.
- **Python documents are not an execution oracle:** CACHE_INCONSISTENCY_EXPLAINED.md says epoch-origin resampling, global deduplication and precision-independent datetime conversion were fixed. The current merging.py still uses `pd.Grouper` without explicit epoch origin in that branch, only deduplicates the preserve-raw branch, and retains `astype('int64') // 10**9`. Its text and code differ. The simulation intentionally bypasses some production guards and contains a 1900-second scenario incompatible with fixed step 300. A simulation result cannot substitute for the safety+engine entry path.

## Preserved or improved mechanisms

| Contract | Current Java assessment |
|---|---|
| Exclude request time/LIMIT/ORDER from base logical identity | Present; source/projection and semantics version protect identity further |
| Full-body miss runs Spark rather than issuing one query per missing day | Present in current engine |
| Full buckets plus exact partial edges | Present; completed-bucket clock guard is stronger than Python's bare bucket math |
| Parallel cached payload and Spark gap work | Present; vanished EXISTS-to-read buckets trigger a supplemental gap query |
| User path does not store bucket data | Preserved; cube registration is a separate in-memory answer cache |
| Sequential complete-bucket warmer, TTL, empty results | Present; Java adds row cap and rejects incomplete trailing buckets |
| AVG as SUM plus COUNT of non-null expression | Present, with Java decimal/type handling; empty-schema exception noted above |
| SUM/COUNT/MIN/MAX combine by parsed operation | Present; Java avoids relying only on alias prefixes |
| Merge before ordering and limiting | Present for admitted shapes; unresolved ordering throws rather than silently dropping it |
| Persistent top-N and per-cycle deduplication | Present; Java persists physical SQL and reconstructs canonical queries after restart |
| Binary compressed payload | Arrow IPC + Zstd replaces pickle + Zstd; preserves the design objective, not Python blob interoperability |

## Verification and limits

The targeted current Java tests reported 32 passed, zero failures/errors and Maven BUILD SUCCESS: DataChangeDetectorTest, WarmingFreshnessTest, FramePostProcessingTest and TypedMergeRegressionTest. Java 21 with in-process Surefire and the necessary module opens was used after fork setup failures. The tool wrapper still reported exit 1 for a Maven-success invocation, so the report records both facts.

The primary agent compiled and ran the standalone CubeFreshnessProbe, EpochProbe, EmptyAverageProbe and StepParityProbe against existing compiled classes; their sources are retained under `docs/codex-review-probes`. The first three use fake ports or direct merger input; the step probe uses the current Calcite factory and gap builder. These are focused reproductions, not a real Spark cluster benchmark.

The Python module was fully read but was not executed: the bundled Python environment lacks sqlglot/Redis/Zstandard dependencies, and no project environment was found at the usual local venv paths. No claim of a passing Python differential suite is made. Full production Spark/Delta/Gluten/Valkey validation was not run. The two foundational specifications contain conflicting historical examples, so the report distinguishes actual source parity, normative document differences and provable correctness issues rather than requiring every old sentence to hold simultaneously.

## Spark outcome

The reviewed POM uses Spark **4.1.1** and Delta **4.1.0**, not Delta 4.0. Delta's official matrix supports that pairing. Gluten's native Delta integration has a separate compatibility and executed-plan validation requirement. See `spark-retrieval-review-codex.md` for the full official-source comparison.

Current Java collection already uses `executedPlan().executeCollect()` and maps `InternalRow[]` directly. It avoids the public external Row conversion of `collectAsList`. Its main remaining constraint is full result materialization and coexistence of collected rows with ResultFrame arrays. `toLocalIterator`/internal `executeToIterator` trade temporary memory for partition/job overhead; PySpark's Arrow setting does not accelerate this Java collector; JSON would introduce encoding/parsing work for a typed-frame consumer. There is no demonstrated universal 10x path. Measure collect, map, encoding and peak memory separately, then compare identical workloads before changing the collector.
