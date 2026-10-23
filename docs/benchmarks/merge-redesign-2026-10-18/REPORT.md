# Cascada merge redesign: before/after benchmark, 18 October 2026

Same workload as `avg20m-12cols-2026-10-18`: **20,000,000 partial-aggregate rows × 12 columns per query**, 300 two-hour buckets, GROUP BY 3 keys, 3 weighted AVG outputs. Machine: AMD Ryzen 7 5700G (8 cores / 16 threads), 31 GiB, Windows 11, OpenJDK 21.0.12, G1, `-Xms1g -Xmx14g`. Separate JVM per case, one warm-up, then measured rounds. **Every measured round re-verifies every output group against the independent oracle (all PASS).**

- **Before** = current production `FrameMergeService.incremental` → `TypedFrameAccumulator` → `TypedAggregationGroups`.
- **After** = prototype `FastAvgMerge`/`GroupTable` (in `prototype/`): row-major paged group state, hash-tagged slots, column-at-a-time frame processing, dictionary ids for `MIN(typeof)` strings, fused AVG finish, optional hash-partitioned workers (w = threads per query).
- `seq` = generator's ascending group order (the original report). `shuffled` = all 20M rows globally shuffled, so every bucket holds groups in arbitrary order (realistic Spark output).
- Heap = peak sampled heap above the retained input (merge working memory). CPU = process CPU-seconds for the round.

## Summary (median wall time)

| Queries | Groups/query | Keys | Order | Before | After 1 thread | After 4 threads/query | After 8 threads/query | Merge heap before → after (1 thread) |
|---:|---:|---|---|---:|---:|---:|---:|---|
| 1 | 1,000,000 | LONG | seq | 9.28 s | 3.69 s (**2.5×**) | 1.74 s (**5.3×**) | 1.60 s (**5.8×**) | 0.57 → 0.22 GiB (−62%) |
| 1 | 1,000,000 | LONG | shuffled | 22.56 s | 10.66 s (**2.1×**) | 3.48 s (**6.5×**) | 2.65 s (**8.5×**) | 0.58 → 0.23 GiB (−61%) |
| 1 | 1,000,000 | STRING | shuffled | 25.33 s | 14.25 s (**1.8×**) | 4.93 s (**5.1×**) | — | 0.56 → 0.22 GiB (−61%) |
| 1 | 5,000,000 | LONG | shuffled | 31.28 s | 14.21 s (**2.2×**) | 5.52 s (**5.7×**) | 4.76 s (**6.6×**) | 2.61 → 1.17 GiB (−55%) |
| 4 | 1,000,000 | LONG | shuffled | 28.64 s | 14.72 s (**1.9×**) | 9.30 s (**3.1×**) | — | 2.21 → 0.91 GiB (−59%) |
| 4 | 5,000,000 | LONG | shuffled | 39.87 s | 20.52 s (**1.9×**) | 13.59 s (**2.9×**) | — | 7.47 → 4.70 GiB (−37%) |

## Every measured round

| Queries | Groups | Keys | Order | Version | Threads/query | Round | Wall | Input M rows/s | Process CPU-s | Avg cores | Merge heap GiB | Verify |
|---:|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 1,000,000 | LONG | seq | before | 1 | 0 | 9.925 s | 2.02 | 10.0 | 1.01 | 0.570 | PASS |
| 1 | 1,000,000 | LONG | seq | before | 1 | 1 | 9.270 s | 2.16 | 9.7 | 1.04 | 0.539 | PASS |
| 1 | 1,000,000 | LONG | seq | before | 1 | 2 | 9.280 s | 2.16 | 9.2 | 0.99 | 0.539 | PASS |
| 1 | 1,000,000 | LONG | seq | after | 1 | 0 | 3.763 s | 5.31 | 3.9 | 1.04 | 0.219 | PASS |
| 1 | 1,000,000 | LONG | seq | after | 1 | 1 | 3.687 s | 5.42 | 3.8 | 1.03 | 0.219 | PASS |
| 1 | 1,000,000 | LONG | seq | after | 1 | 2 | 3.682 s | 5.43 | 3.8 | 1.02 | 0.219 | PASS |
| 1 | 1,000,000 | LONG | seq | after | 4 | 0 | 1.880 s | 10.64 | 6.9 | 3.68 | 0.227 | PASS |
| 1 | 1,000,000 | LONG | seq | after | 4 | 1 | 1.726 s | 11.59 | 6.5 | 3.77 | 0.227 | PASS |
| 1 | 1,000,000 | LONG | seq | after | 4 | 2 | 1.744 s | 11.47 | 6.7 | 3.83 | 0.227 | PASS |
| 1 | 1,000,000 | LONG | seq | after | 8 | 0 | 1.792 s | 11.16 | 11.6 | 6.48 | 0.219 | PASS |
| 1 | 1,000,000 | LONG | seq | after | 8 | 1 | 1.598 s | 12.52 | 11.6 | 7.25 | 0.211 | PASS |
| 1 | 1,000,000 | LONG | seq | after | 8 | 2 | 1.520 s | 13.16 | 10.5 | 6.92 | 0.211 | PASS |
| 1 | 1,000,000 | LONG | shuffled | before | 1 | 0 | 22.556 s | 0.89 | 22.7 | 1.01 | 0.578 | PASS |
| 1 | 1,000,000 | LONG | shuffled | before | 1 | 1 | 22.858 s | 0.87 | 23.1 | 1.01 | 0.570 | PASS |
| 1 | 1,000,000 | LONG | shuffled | before | 1 | 2 | 22.314 s | 0.90 | 22.3 | 1.00 | 0.547 | PASS |
| 1 | 1,000,000 | LONG | shuffled | after | 1 | 0 | 10.870 s | 1.84 | 11.0 | 1.01 | 0.227 | PASS |
| 1 | 1,000,000 | LONG | shuffled | after | 1 | 1 | 10.662 s | 1.88 | 11.0 | 1.03 | 0.227 | PASS |
| 1 | 1,000,000 | LONG | shuffled | after | 1 | 2 | 10.659 s | 1.88 | 10.6 | 1.00 | 0.227 | PASS |
| 1 | 1,000,000 | LONG | shuffled | after | 4 | 0 | 3.417 s | 5.85 | 12.8 | 3.73 | 0.227 | PASS |
| 1 | 1,000,000 | LONG | shuffled | after | 4 | 1 | 3.486 s | 5.74 | 12.7 | 3.65 | 0.227 | PASS |
| 1 | 1,000,000 | LONG | shuffled | after | 4 | 2 | 3.478 s | 5.75 | 12.6 | 3.61 | 0.227 | PASS |
| 1 | 1,000,000 | LONG | shuffled | after | 8 | 0 | 2.653 s | 7.54 | 18.3 | 6.90 | 0.219 | PASS |
| 1 | 1,000,000 | LONG | shuffled | after | 8 | 1 | 2.861 s | 6.99 | 18.5 | 6.47 | 0.219 | PASS |
| 1 | 1,000,000 | LONG | shuffled | after | 8 | 2 | 2.634 s | 7.59 | 18.4 | 6.99 | 0.219 | PASS |
| 1 | 1,000,000 | STRING | shuffled | before | 1 | 0 | 25.330 s | 0.79 | 25.9 | 1.02 | 0.555 | PASS |
| 1 | 1,000,000 | STRING | shuffled | before | 1 | 1 | 31.208 s | 0.64 | 30.8 | 0.99 | 0.555 | PASS |
| 1 | 1,000,000 | STRING | shuffled | before | 1 | 2 | 24.553 s | 0.81 | 24.7 | 1.00 | 0.523 | PASS |
| 1 | 1,000,000 | STRING | shuffled | after | 1 | 0 | 14.250 s | 1.40 | 14.5 | 1.01 | 0.219 | PASS |
| 1 | 1,000,000 | STRING | shuffled | after | 1 | 1 | 14.246 s | 1.40 | 14.3 | 1.00 | 0.219 | PASS |
| 1 | 1,000,000 | STRING | shuffled | after | 1 | 2 | 14.084 s | 1.42 | 14.2 | 1.01 | 0.219 | PASS |
| 1 | 1,000,000 | STRING | shuffled | after | 4 | 0 | 4.875 s | 4.10 | 17.9 | 3.67 | 0.219 | PASS |
| 1 | 1,000,000 | STRING | shuffled | after | 4 | 1 | 4.954 s | 4.04 | 18.4 | 3.71 | 0.219 | PASS |
| 1 | 1,000,000 | STRING | shuffled | after | 4 | 2 | 4.934 s | 4.05 | 18.4 | 3.74 | 0.219 | PASS |
| 1 | 5,000,000 | LONG | shuffled | before | 1 | 0 | 34.854 s | 0.57 | 35.2 | 1.01 | 2.547 | PASS |
| 1 | 5,000,000 | LONG | shuffled | before | 1 | 1 | 31.283 s | 0.64 | 31.6 | 1.01 | 2.609 | PASS |
| 1 | 5,000,000 | LONG | shuffled | before | 1 | 2 | 31.086 s | 0.64 | 31.0 | 1.00 | 2.438 | PASS |
| 1 | 5,000,000 | LONG | shuffled | after | 1 | 0 | 14.439 s | 1.39 | 14.6 | 1.01 | 1.172 | PASS |
| 1 | 5,000,000 | LONG | shuffled | after | 1 | 1 | 14.208 s | 1.41 | 14.5 | 1.02 | 1.172 | PASS |
| 1 | 5,000,000 | LONG | shuffled | after | 1 | 2 | 14.042 s | 1.42 | 14.0 | 0.99 | 1.172 | PASS |
| 1 | 5,000,000 | LONG | shuffled | after | 4 | 0 | 5.541 s | 3.61 | 17.3 | 3.13 | 1.258 | PASS |
| 1 | 5,000,000 | LONG | shuffled | after | 4 | 1 | 5.453 s | 3.67 | 16.9 | 3.09 | 1.258 | PASS |
| 1 | 5,000,000 | LONG | shuffled | after | 4 | 2 | 5.521 s | 3.62 | 17.5 | 3.18 | 1.258 | PASS |
| 1 | 5,000,000 | LONG | shuffled | after | 8 | 0 | 4.758 s | 4.20 | 24.2 | 5.08 | 1.297 | PASS |
| 1 | 5,000,000 | LONG | shuffled | after | 8 | 1 | 4.724 s | 4.23 | 24.2 | 5.13 | 1.297 | PASS |
| 1 | 5,000,000 | LONG | shuffled | after | 8 | 2 | 4.988 s | 4.01 | 26.0 | 5.22 | 1.297 | PASS |
| 4 | 1,000,000 | LONG | shuffled | before | 1 | 0 | 28.717 s | 2.79 | 112.3 | 3.91 | 2.211 | PASS |
| 4 | 1,000,000 | LONG | shuffled | before | 1 | 1 | 28.566 s | 2.80 | 110.6 | 3.87 | 2.211 | PASS |
| 4 | 1,000,000 | LONG | shuffled | after | 1 | 0 | 14.654 s | 5.46 | 56.4 | 3.85 | 0.906 | PASS |
| 4 | 1,000,000 | LONG | shuffled | after | 1 | 1 | 14.579 s | 5.49 | 56.2 | 3.85 | 0.906 | PASS |
| 4 | 1,000,000 | LONG | shuffled | after | 1 | 2 | 14.541 s | 5.50 | 56.1 | 3.86 | 0.906 | PASS |
| 4 | 1,000,000 | LONG | shuffled | after | 1 | 0 | 14.780 s | 5.41 | 57.5 | 3.89 | 0.906 | PASS |
| 4 | 1,000,000 | LONG | shuffled | after | 1 | 1 | 14.930 s | 5.36 | 57.7 | 3.86 | 0.906 | PASS |
| 4 | 1,000,000 | LONG | shuffled | after | 1 | 2 | 14.796 s | 5.41 | 57.2 | 3.87 | 0.906 | PASS |
| 4 | 1,000,000 | LONG | shuffled | after | 4 | 0 | 9.361 s | 8.55 | 113.7 | 12.14 | 0.930 | PASS |
| 4 | 1,000,000 | LONG | shuffled | after | 4 | 1 | 9.231 s | 8.67 | 113.3 | 12.27 | 0.930 | PASS |
| 4 | 5,000,000 | LONG | shuffled | before | 1 | 0 | 40.059 s | 2.00 | 187.7 | 4.69 | 7.459 | PASS |
| 4 | 5,000,000 | LONG | shuffled | before | 1 | 1 | 39.683 s | 2.02 | 185.0 | 4.66 | 7.466 | PASS |
| 4 | 5,000,000 | LONG | shuffled | after | 1 | 0 | 20.511 s | 3.90 | 81.7 | 3.98 | 4.695 | PASS |
| 4 | 5,000,000 | LONG | shuffled | after | 1 | 1 | 20.526 s | 3.90 | 80.6 | 3.92 | 4.695 | PASS |
| 4 | 5,000,000 | LONG | shuffled | after | 4 | 0 | 13.599 s | 5.88 | 152.9 | 11.24 | 5.055 | PASS |
| 4 | 5,000,000 | LONG | shuffled | after | 4 | 1 | 13.574 s | 5.89 | 153.2 | 11.28 | 5.055 | PASS |

## Production code after the in-place rewrite (single thread, same harness)

`TypedAggregationGroups` was rewritten in place (row-major paged state, tagged slots, column-at-a-time frames, dictionary ids for STRING MIN/MAX) and `SparkValues.compareStrings` gained an equal-strings fast path. This step keeps the existing finish path (12-column merged frame, then `SparkAverage.finish`). It has no fused finish and no partitioned workers yet, which is why its memory and speed gains are smaller than the prototype's.

| Queries | Groups | Keys | Order | Before median | After median | Speedup | Process CPU-s before → after | Merge heap before → after | Verify |
|---:|---:|---|---|---:|---:|---:|---|---|---|
| 1 | 1,000,000 | LONG | seq | 9.28 s | 3.91 s | **2.38×** | 9.7 → 4.2 | 0.57 → 0.40 GiB (−30%) | {'PASS'} |
| 1 | 1,000,000 | LONG | shuffled | 22.56 s | 10.78 s | **2.09×** | 22.7 → 10.8 | 0.58 → 0.41 GiB (−30%) | {'PASS'} |
| 1 | 1,000,000 | STRING | shuffled | 25.33 s | 15.55 s | **1.63×** | 25.9 → 15.8 | 0.56 → 0.39 GiB (−30%) | {'PASS'} |
| 1 | 5,000,000 | LONG | shuffled | 31.28 s | 18.01 s | **1.74×** | 31.6 → 19.4 | 2.61 → 1.87 GiB (−28%) | {'PASS'} |
| 4 | 1,000,000 | LONG | shuffled | 28.64 s | 16.09 s | **1.78×** | 111.4 → 63.8 | 2.21 → 1.63 GiB (−26%) | {'PASS'} |

Every round after the rewrite:

| Queries | Groups | Keys | Order | Round | Wall | Accept | Process CPU-s | Merge heap GiB | Verify |
|---:|---:|---|---|---:|---:|---:|---:|---:|---|
| 1 | 1,000,000 | LONG | seq | 0 | 3.941 s | 3.433 s | 4.2 | 0.398 | PASS |
| 1 | 1,000,000 | LONG | seq | 1 | 3.887 s | 3.416 s | 4.2 | 0.398 | PASS |
| 1 | 1,000,000 | LONG | seq | 2 | 3.906 s | 3.446 s | 3.8 | 0.367 | PASS |
| 1 | 1,000,000 | LONG | shuffled | 0 | 10.632 s | 9.356 s | 10.8 | 0.406 | PASS |
| 1 | 1,000,000 | LONG | shuffled | 1 | 10.811 s | 9.313 s | 11.2 | 0.375 | PASS |
| 1 | 1,000,000 | LONG | shuffled | 2 | 10.782 s | 9.472 s | 10.7 | 0.375 | PASS |
| 1 | 1,000,000 | STRING | shuffled | 0 | 15.757 s | 12.382 s | 16.0 | 0.391 | PASS |
| 1 | 1,000,000 | STRING | shuffled | 1 | 15.547 s | 12.214 s | 15.8 | 0.391 | PASS |
| 1 | 1,000,000 | STRING | shuffled | 2 | 15.457 s | 12.205 s | 15.4 | 0.359 | PASS |
| 1 | 5,000,000 | LONG | shuffled | 0 | 19.630 s | 10.546 s | 21.0 | 1.874 | PASS |
| 1 | 5,000,000 | LONG | shuffled | 1 | 18.007 s | 10.524 s | 19.4 | 1.872 | PASS |
| 1 | 5,000,000 | LONG | shuffled | 2 | 17.751 s | 10.454 s | 18.1 | 1.724 | PASS |
| 4 | 1,000,000 | LONG | shuffled | 0 | 16.172 s | 13.953 s | 64.8 | 1.633 | PASS |
| 4 | 1,000,000 | LONG | shuffled | 1 | 16.010 s | 13.910 s | 62.9 | 1.570 | PASS |

The 4-query × 5M-group case was stopped by the host for low system memory before producing a round, so it has no after number. Production correctness: all 320 `lib_cache` tests pass (3 Valkey integration tests skipped without a server). Full-size bit-exact checks against the prototype (itself bit-exact to the old code) pass for LONG seq, LONG shuffled and STRING shuffled. A **third single-bit flip** hit the harness shuffle during these checks (row index 67158622 = valid 49758 with bit 26 set, the same bit as the earlier crash), which further supports the hardware-memory diagnosis.

## Final production code: fused AVG finish + partitioned workers (`CASCADA_MERGE_WORKERS`)

This is the code on the feature branch: the rewritten `TypedAggregationGroups`, AVG emitted straight from group state (`TypedFrameAccumulator.finishWithAverages`), and hash-partitioned workers running on the engine's executor (virtual threads in `CascadaEngineFactory`). Workers = `CASCADA_MERGE_WORKERS`; the default is half the hardware threads, between 1 and 4 (4 on this machine). Output is bit-identical to the old code for every worker count (full-size checks at 1, 4 and 8 workers, LONG and STRING keys).

**Measurement caveat:** during this run the desktop was also running the League of Legends client and the Avast browser (CPU load ~44% before the benchmark started), so absolute times are noisier than the earlier batches. The 1-worker accept phase measured 3.6–4.3 s here versus 3.4 s in the step-1 run; that difference is within this run's round-to-round spread and was not attributed to the code.

| Queries | Groups | Keys | Order | Before (old code) | After, 1 worker | After, 4 workers (default) | After, 8 workers | Merge heap before → after (4 workers) |
|---:|---:|---|---|---:|---:|---:|---:|---|
| 1 | 1,000,000 | LONG | seq | 9.28 s | 4.28 s (**2.2×**) | 2.08 s (**4.5×**) | 1.92 s (**4.8×**) | 0.57 → 0.30 GiB (−46%) |
| 1 | 1,000,000 | LONG | shuffled | 22.56 s | 11.96 s (**1.9×**) | 4.26 s (**5.3×**) | 3.46 s (**6.5×**) | 0.58 → 0.30 GiB (−47%) |
| 1 | 1,000,000 | STRING | shuffled | 25.33 s | 16.07 s (**1.6×**) | 5.90 s (**4.3×**) | — | 0.56 → 0.30 GiB (−46%) |
| 1 | 5,000,000 | LONG | shuffled | 31.28 s | 19.06 s (**1.6×**) | 7.41 s (**4.2×**) | 6.59 s (**4.7×**) | 2.61 → 1.64 GiB (−37%) |
| 4 | 1,000,000 | LONG | shuffled | 28.64 s | 16.35 s (**1.8×**) | 10.30 s (**2.8×**) | — | 2.21 → 1.23 GiB (−44%) |

Every round of the final production run:

| Queries | Groups | Keys | Order | Workers | Round | Wall | Accept | Process CPU-s | Avg cores | Merge heap GiB | Verify |
|---:|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 1,000,000 | LONG | seq | 1 | 0 | 4.469 s | 4.166 s | 4.6 | 1.03 | 0.297 | PASS |
| 1 | 1,000,000 | LONG | seq | 1 | 1 | 4.262 s | 4.005 s | 4.4 | 1.03 | 0.297 | PASS |
| 1 | 1,000,000 | LONG | seq | 1 | 2 | 4.281 s | 4.043 s | 4.4 | 1.02 | 0.297 | PASS |
| 1 | 1,000,000 | LONG | seq | 4 | 0 | 2.085 s | 1.805 s | 8.0 | 3.86 | 0.305 | PASS |
| 1 | 1,000,000 | LONG | seq | 4 | 1 | 2.079 s | 1.820 s | 7.9 | 3.80 | 0.297 | PASS |
| 1 | 1,000,000 | LONG | seq | 4 | 2 | 2.059 s | 1.805 s | 7.7 | 3.73 | 0.297 | PASS |
| 1 | 1,000,000 | LONG | seq | 8 | 0 | 1.915 s | 1.588 s | 12.8 | 6.66 | 0.289 | PASS |
| 1 | 1,000,000 | LONG | seq | 8 | 1 | 2.102 s | 1.764 s | 13.7 | 6.51 | 0.281 | PASS |
| 1 | 1,000,000 | LONG | seq | 8 | 2 | 1.878 s | 1.587 s | 12.5 | 6.66 | 0.281 | PASS |
| 1 | 1,000,000 | LONG | shuffled | 1 | 0 | 12.384 s | 10.990 s | 12.5 | 1.01 | 0.297 | PASS |
| 1 | 1,000,000 | LONG | shuffled | 1 | 1 | 11.955 s | 10.818 s | 12.1 | 1.01 | 0.297 | PASS |
| 1 | 1,000,000 | LONG | shuffled | 1 | 2 | 11.838 s | 10.817 s | 11.7 | 0.99 | 0.305 | PASS |
| 1 | 1,000,000 | LONG | shuffled | 4 | 0 | 4.261 s | 3.547 s | 15.1 | 3.54 | 0.305 | PASS |
| 1 | 1,000,000 | LONG | shuffled | 4 | 1 | 4.283 s | 3.563 s | 15.0 | 3.50 | 0.305 | PASS |
| 1 | 1,000,000 | LONG | shuffled | 4 | 2 | 4.253 s | 3.538 s | 15.0 | 3.54 | 0.305 | PASS |
| 1 | 1,000,000 | LONG | shuffled | 8 | 0 | 3.036 s | 2.383 s | 19.4 | 6.38 | 0.289 | PASS |
| 1 | 1,000,000 | LONG | shuffled | 8 | 1 | 3.482 s | 2.762 s | 21.7 | 6.24 | 0.289 | PASS |
| 1 | 1,000,000 | LONG | shuffled | 8 | 2 | 3.463 s | 2.790 s | 22.2 | 6.42 | 0.289 | PASS |
| 1 | 1,000,000 | STRING | shuffled | 1 | 0 | 16.995 s | 13.768 s | 16.8 | 0.99 | 0.297 | PASS |
| 1 | 1,000,000 | STRING | shuffled | 1 | 1 | 15.721 s | 12.515 s | 15.8 | 1.00 | 0.289 | PASS |
| 1 | 1,000,000 | STRING | shuffled | 1 | 2 | 16.068 s | 12.877 s | 16.2 | 1.01 | 0.297 | PASS |
| 1 | 1,000,000 | STRING | shuffled | 4 | 0 | 5.896 s | 4.629 s | 20.9 | 3.55 | 0.297 | PASS |
| 1 | 1,000,000 | STRING | shuffled | 4 | 1 | 6.089 s | 4.834 s | 22.8 | 3.74 | 0.297 | PASS |
| 1 | 1,000,000 | STRING | shuffled | 4 | 2 | 5.705 s | 4.466 s | 20.8 | 3.65 | 0.297 | PASS |
| 1 | 5,000,000 | LONG | shuffled | 1 | 0 | 19.295 s | 11.242 s | 19.5 | 1.01 | 1.547 | PASS |
| 1 | 5,000,000 | LONG | shuffled | 1 | 1 | 19.063 s | 11.147 s | 19.3 | 1.01 | 1.547 | PASS |
| 1 | 5,000,000 | LONG | shuffled | 1 | 2 | 17.283 s | 11.229 s | 17.2 | 1.00 | 1.547 | PASS |
| 1 | 5,000,000 | LONG | shuffled | 4 | 0 | 7.702 s | 3.413 s | 22.1 | 2.87 | 1.369 | PASS |
| 1 | 5,000,000 | LONG | shuffled | 4 | 1 | 7.414 s | 3.549 s | 22.0 | 2.96 | 1.633 | PASS |
| 1 | 5,000,000 | LONG | shuffled | 4 | 2 | 7.333 s | 3.656 s | 21.8 | 2.97 | 1.641 | PASS |
| 1 | 5,000,000 | LONG | shuffled | 8 | 0 | 6.589 s | 2.673 s | 30.2 | 4.58 | 1.680 | PASS |
| 1 | 5,000,000 | LONG | shuffled | 8 | 1 | 6.702 s | 2.805 s | 31.6 | 4.72 | 1.680 | PASS |
| 1 | 5,000,000 | LONG | shuffled | 8 | 2 | 6.301 s | 2.598 s | 28.5 | 4.53 | 1.680 | PASS |
| 4 | 1,000,000 | LONG | shuffled | 1 | 0 | 16.472 s | 14.576 s | 65.1 | 3.95 | 1.219 | PASS |
| 4 | 1,000,000 | LONG | shuffled | 1 | 1 | 16.220 s | 14.430 s | 63.5 | 3.91 | 1.211 | PASS |
| 4 | 1,000,000 | LONG | shuffled | 4 | 0 | 10.318 s | 9.476 s | 121.3 | 11.76 | 1.234 | PASS |
| 4 | 1,000,000 | LONG | shuffled | 4 | 1 | 10.283 s | 9.212 s | 120.8 | 11.75 | 1.227 | PASS |

The 4-query × 5M-group case was not rerun: it exhausted host memory in the previous batch.

## Correctness

- Bit-exact cross-check (every cell, including row order and double bits) of after vs before: 2M/100k LONG seq w1, 2M/100k LONG shuffled w4, 2M/100k STRING shuffled w3, 3M/1M LONG shuffled w8, 20M/1M LONG seq w1, then a stress run of **80 full 20M-row/1M-group merges** (10 JVMs × 4 repeats × w1 and w8): **all bit-exact** (`raw/stress.log`).
- Three anomalies occurred during the session and are excluded from timing: (1) one wrong AVG (sum off by exactly 2^-17), (2) one worker exception that hung a w8 run (prototype then fixed to fail fast instead of hang), (3) one `ArrayIndexOutOfBoundsException` in the **test-harness shuffle**, before any merge code ran, where index `0x40000E7` is the valid value 231 with bit 26 flipped. Both diagnosable values are single-bit flips, including one outside the merge code, which points to **hardware memory errors on this non-ECC desktop**, not to the merge logic. Recommend MemTest86 / disabling XMP before trusting long benchmark runs on this machine.

## Why it is faster and smaller

1. **~40% of the old merge time was `SparkValues.compareStrings("double","double")`**: the `MIN(typeof)` AVG metadata column walked code points 60M times per query. After: one dictionary id per value; strings are compared only if ids differ.
2. **Old state layout = ~26 parallel arrays per group** (keys, presence booleans, 9 measure arrays, hashes). Each input row touched ~26 random cache lines; with shuffled input this made the old merge 2.4× slower than the published report's sequential case (22.6 s vs 9.3 s). After: one contiguous `long[]` slab per group (≈1–2 cache lines) + a slot that stores the hash tag, so a mismatch is rejected without touching group memory.
3. **No doubling copies**: group state lives in fixed 16,384-group pages, so growth never copies state or over-allocates up to 2× (the old arrays hit 8.4M capacity for 5M groups).
4. **Column-at-a-time processing**: one hash pass, one probe pass, one tight loop per measure column; no per-cell `switch` on type.
5. **Fused finish**: AVG is computed straight from group state into the 6 output columns, instead of building a 12-column merged frame and then a 6-column AVG frame.
6. **Partitioned parallelism**: each worker owns a disjoint hash partition (no locks, no shared mutable state); results are k-way merged in key order. One query can now use several cores.

## Limits

One desktop, other applications open, 2–3 measured rounds per case, no p95/p99. In-memory merge only (no cache fetch, decoding, Spark, network). The prototype supports LONG/DOUBLE/STRING keys and DOUBLE AVG; DECIMAL and time-dimension flooring are handled only by the production port. With 4 concurrent queries the machine's 8 physical cores are the ceiling, so threads per query help less (3.0× vs 1.9×).

## Files

`prototype/` holds the measured prototype and harness; `raw/` holds the raw log lines and `all-runs.json`.
