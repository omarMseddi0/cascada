> Cleanup note (18 October 2026): standalone review probes and generated `codex-*.log` / benchmark JSON artifacts referenced below were deleted after the untracked-file audit. References describe historical evidence, not files still present. Findings and recorded measurements remain historical; current verification is recorded in `UNCOMMITTED_COMMIT_PLAN.md`.

> Historical review snapshot. Fix status and final validation are superseded by [the 2026-10-16 fix review](CACHE_CORE_FIX_REVIEW_2026-10-16.md).

# Cascada freshness review against the supplied baseline

> Source-parity correction after personally reading all 20 files in the original Python smart_cache directory: the sliding validation baseline below is already implemented in Python auto_warmer.py, and the failed-refresh acknowledgement problem can occur there too because per-bucket failures are swallowed before the baseline is saved. These are NOT evidence that Java alone introduced those behaviors. The frozen-epoch finding is a README-versus-both-implementations discrepancy. See `cache-python-parity-review-codex.md` for the final combined classification. This report's code observations and reproduced outputs remain valid.

Reviewed 2026-10-15, Europe/Paris. Source review only; no production files changed. The working tree is being edited concurrently, so findings describe the files read, not a clean commit. `review_claude.md` is excluded from evidence as requested.

Both baseline documents were read completely. Explicit normative sections take precedence over conflicting older examples: SMART_CACHE_README §8 says only the warmer writes bucket payloads, despite the older lazy-write diagram in §4; §4.4 specifies frozen validation epochs, despite the older always-overwrite table in §8. The technical details specify fixed 300-second storage steps and display resampling, despite older examples describing separate cache families for display steps. Pickle, Python classes and exact MD5 bytes are implementation details unless cross-language byte compatibility is required.

## Confirmed findings

### P1: The cube cache outlives bucket freshness

`CacheExecutionEngine.java:129-133` answers from `CubeShapeCatalog` before bucket lookup. `CubeShapeCatalog.java:84-88,95-103` stores frames and exact answers without timestamps, TTL or a source version. Registering an existing shape is a no-op. The completed-window guard prevents caching live windows, but does not invalidate completed history after backfills, TTL expiry or a disabled validation canary. Factory wiring always creates the catalog; validation SQL is optional.

Reproduction against compiled current classes: `docs/codex-review-probes/CubeFreshnessProbe.java`. An empty bucket backend, a completed historical SUM query, and a fake authoritative source initially returning 100 produce `first=[{SUM(bytes)=100}]`. Change source to 200 and repeat: `after_source_change_with_no_bucket_hits=[{SUM(bytes)=100}]; spark_calls=1`. No bucket was ever stored. Continued access can retain this answer indefinitely, independently of Redis expiry, until catalog eviction, clearing, process restart or successful change-triggered invalidation. The existing cube verifier checks the arithmetic of the cached input, not whether it matches current Delta data.

Required change: give every answer tier a common freshness/generation contract. Bound cube entries by TTL/source generation, invalidate on relevant bucket expiry/refresh, and replace stale registrations. This is necessary even with correct roll-up mathematics.

### P1: Failed forced refreshes are acknowledged before they succeed

`DataChangeDetector.java:80-87` saves the changed baseline before `WarmingOrchestrator.java:159` starts rewriting buckets. If one pattern fails at Spark/Redis, `WarmingOrchestrator.java:197-202` merely votes its hash back into the queue. On the next successful canary comparison the source matches the already-saved baseline, so `forceOverwrite=false`. `WarmingOrchestrator.java:280-291` skips the existing stale bucket instead of retrying the failed overwrite. The same problem occurs on a fresh epoch if overwrite fails. A row-cap rejection also leaves an old existing value in place.

Trigger: warm value 100; change historical source to 200; canary detects change; fail that pattern's refresh once; run another cycle with unchanged 200 source. The requeued pattern now sees the old key as present and skips it. Stale data survives until expiry or another detected change. This is a control-flow finding; no end-to-end Spark failure-injection test was run.

Required change: retain a dirty generation/forced-refresh obligation until each affected pattern/bucket completes, or invalidate the affected keys before allowing EXISTS-skip again. Baseline advancement alone cannot represent refresh completion.

### P2: Frozen validation epoch has been replaced by a sliding, perpetually renewed baseline

Baseline: SMART_CACHE_README lines 318-347 explicitly requires one query over the stored frozen window, no write on match, and natural TTL expiry as the only epoch reset. Current `DataChangeDetector.java:80-91` queries the old window and, if it differs from the current window, queries the new one too; then saves the new window even when no change occurred. Every successful match resets TTL.

`docs/codex-review-probes/EpochProbe.java` output:

```
first_changed=true
unchanged_changed=false
baseline_writes=2; validation_queries=3; final_window=TimeRange[startTimestampSeconds=86400, endTimestampSeconds=345599]
```

The baseline contract expects one initial save, two total validation queries, and the original frozen window after those two cycles. Existing `DataChangeDetectorTest.unchangedHistoryKeepsTheCacheAndSlidesTheBaselineForward` explicitly asserts the changed behavior, so green tests do not establish README compliance. The consequence is extra Spark validation work and loss of the baseline's natural periodic full-refresh mechanism; bucket TTL still works separately.

Required change: match on the stored window without saving; on mismatch update the signature while retaining that window; open a new current window only after absence/expiry. Preserve pending refresh work as described above.

## Additional coverage and caveats

- A separately reproduced P2 schema bug exists for all-empty grouped AVG ingredients. `IncrementalFrameMerge.java:45-49` returns the empty ingredient frame before reconstruction. `docs/codex-review-probes/EmptyAverageProbe.java` produces `expected=[app, avg_x]; actual=[app, avg_x__cascada_avg_sum, avg_x__cascada_avg_count, avg_x__cascada_avg_type]`. Zero rows are valid cached hits, but their public schema still needs the requested AVG alias. The direct AVG miss path also invokes this incremental merge. Fix empty-schema reconstruction with sufficient input type metadata; invoking numeric reconstruction blindly may not infer the empty AVG decimal type correctly.

- The warmer now excludes live buckets, writes with TTL, preserves empty frames, and enforces a row cap; those mechanisms exist in the reviewed source.
- The read engine now uses a Clock and completed-bucket planning. Claims that it has no clock or always caches today are not true of this snapshot.
- `warmLookbackWindow` excludes the bucket immediately preceding the live bucket from the validation window, and for short lookbacks can skip validation entirely (lines 155-158). This differs from the supplied README's described current lookback window. It may be a late-arrival policy, but the user-visible freshness contract needs to state it; the excluded bucket can have been warmed already.
- Validation signatures stringify cells and join unescaped separators. SQL NULL and literal string `null` have the same representation, and embedded separator characters can collide. This matters if the advertised arbitrary-row/column canary result is used. Prefer typed length-prefixed canonical encoding.
- No attempt was made to repair the code during this review.

## Verification

Targeted Maven run, Java 21, in-process Surefire with required module opens: DataChangeDetectorTest (6), WarmingFreshnessTest (5), FramePostProcessingTest (5), TypedMergeRegressionTest (16): **32 tests, zero failures/errors, Maven BUILD SUCCESS**. The command wrapper reported exit 1 even for that successful Maven output; this is recorded rather than presented as an entirely clean process exit. Earlier forked runs failed in Surefire setup; the first in-process attempt lacked Agrona's `jdk.internal.misc` open and was corrected for the successful run.

Probe sources compile with the cache and identity target classes and use fake ports; they are evidence for local control flow, not a real Spark/Valkey integration benchmark.

## Snapshot identity

Git HEAD was `f9f0aac231507c0d533a705b10061cf90189a083` with extensive pre-existing uncommitted modifications. SHA-256 hashes after review:

| File | SHA-256 |
|---|---|
| CacheExecutionEngine.java | `9F25A755BE261B0DF48D9160C1638F6FDDF31B29460D83532BF8684A05763076` |
| WarmingOrchestrator.java | `DC42F4FB445A4F4F5F6A7CCC0D37BA437A9912F89FF7794F553E26E585F4E6D9` |
| DataChangeDetector.java | `5E61FB9F6872FDAE2E4FDB40F5C8C4B7327DD03E5E8507950328F19E027CFCC3` |
| SparkDeltaQueryExecutor.java | `7C781232175AF25A93A47BF96F8A8D4625B7EF24C2F9C8B1E2AA26B3B2329DE6` |
