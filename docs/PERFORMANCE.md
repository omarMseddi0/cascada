# Cascada performance analysis and measured changes

Measured on 2026-10-03. Baseline source: `bfc57f1b1921d9c2bddcfe0b1e117e235c3de119`.

## Measured result

The live typed merge path improved by **7.9–14.9x** on the million-row workloads below. Cache eligibility, bucket boundaries, time-step arithmetic, Spark query shapes and warming rules match the baseline source. The current implementation consumes ordered bucket frames incrementally to avoid retaining every decoded input.

Host: Windows 11, Ryzen 7 5700G (8 cores / 16 logical processors), 32 GB RAM. Merge and serializer probes used Java 25; the real Spark probe and final test suite used the already-installed Java 21 runtime. All project sources compile for Java 17. The production Spark 3.5.6/JDK 17 environment still needs its own representative integration measurements.

### Typed merge: one million input rows

Each input has one LONG grouping dimension, a SUM measure and a DOUBLE MAX measure. SUM is either DOUBLE or exact LONG. Data construction is outside the timed region. Old and new aggregation implementations run in the same JVM over the same immutable frames, with four alternating warmups and seven alternating measurements; the table contains medians. A JFR profile runs across both implementations. Full output rows are compared before measurement.

| Workload | Groups | Before | After | Speedup | Allocated before / after per merge |
|---|---:|---:|---:|---:|---:|
| DOUBLE SUM | 1,000 | 567.14 ms | 38.08 ms | 14.89x | 1,477.55 / 0.112 MiB |
| LONG SUM | 1,000 | 393.97 ms | 40.57 ms | 9.71x | 1,454.60 / 0.112 MiB |
| DOUBLE SUM | 100,000 | 730.44 ms | 92.32 ms | 7.91x | 1,516.58 / 12.843 MiB |

JFR identified repeated aggregate-function normalization, regex compilation, boxing, row-map reads and per-row group-key lists in the old path. The replacement resolves schema/aggregate functions once, uses open-addressed typed grouping, and stores primitive LONG/DOUBLE accumulator arrays. Storage grows with distinct groups rather than allocating a temporary key and measure objects for every input row. Decimal and STRING MIN/MAX paths preserve their own types; these workloads do not establish the same speedup for those types.

### Real Spark execution smoke benchmark

Spark 3.5.6 ran in `local[8]`, with 16 input partitions, 16 shuffle partitions, AQE enabled and a 4 GiB JVM heap. A generated range supplied fourteen million events with twenty app IDs and integer measures. A full-window SQL aggregate produced twenty output rows through the actual `SparkDeltaQueryExecutor` and mapper in 2.42 seconds including first-action overhead. This is a generated-data smoke benchmark, not a steady-state or production Delta/Redis throughput claim.

That first query exposed a pre-existing runtime failure: Spark's Jackson Scala module 2.15.2 rejected the platform's Jackson databind 2.17.1. Importing the Jackson BOM at the platform's existing 2.17.1 version aligns those dependencies. The real Spark query completed after that fix.
### Spark external row conversion

A separate microbenchmark maps one million repeated external `GenericRow` values with thirteen columns: byte, short, int, long, float, double, decimal, string, boolean, date, timestamp, timestamp without timezone, and null. It measures conversion and frame construction rather than allocating a million source rows or executing Spark SQL.

The old mapper median was **370.68 ms**; the updated mapper median was **321.84 ms** (**1.15x**). Schema-specific typed getters are selected once; the hot loop uses a compact getter switch and typed appends. Date/time external string representations remain unchanged. This synthetic repeated-row result is not a guarantee for arbitrary schemas or Spark source conversion costs.

### Arrow serialization and compression

Million-row Arrow probes with string grouping dimensions compared Zstandard levels using the unchanged length-prefixed Arrow/zstd blob format. Each IPC payload was approximately 39.50 MiB.

| String cardinality | Level 9 serialize | Level 3 serialize | Level 9 blob | Level 3 blob |
|---|---:|---:|---:|---:|
| 1,000 | 208.5 ms | 167.3 ms | 3.302 MiB | 3.383 MiB |
| 100,000 | 196.0 ms | 156.7 ms | 3.372 MiB | 3.458 MiB |

Level 3 reduced serialization time by approximately 20–25% in these runs, at approximately 2.5% more stored/transferred bytes. Compression alone measured roughly 82–88 ms at level 9 versus 15–19 ms at level 3. Level 1 produced 70–83% larger blobs and was inconsistent on speed, so it was not selected. The default is now level 3 in both serializers; constructors allow another level. Decoders remain compatible with blobs produced at level 9. Results depend on value entropy and network bandwidth.

Vectors and result builders are preallocated when row counts are known, per-column type/vector metadata is cached, and decompression reads directly from the compressed region of the blob. Precisely sized frame builds transfer arrays safely using copy-on-write when the builder is subsequently reused, avoiding another full-frame copy.

## Changes throughout the execution path

- **Merging:** typed grouping preserves exact LONG arithmetic and overflow rejection, decimal precision checks, null groups/cells, NaN and signed-zero key semantics, first representative values, deterministic ordering and original floating-point accumulation order.
- **AVG and composites:** indexed cell copies replace rebuilding a row map for every output row. AVG ingredient columns are resolved once; duplicate bookkeeping no longer grows with the number of rows.
- **ORDER BY / LIMIT:** one typed multi-column comparator replaces repeated stable full-result sorts. A primitive-index bounded heap selects only the best K rows for ORDER BY with LIMIT. Stable ties retain the previous merge order; tests compare bounded selection with the full sorted result for null ordering, multiple keys and boundary limits.
- **Cube roll-up and verification:** measure indices, types and aggregate functions are hoisted out of both independent row scans. Verification retains a separate combine implementation. Filter expression parsing and object-based cube group keys remain potential bottlenecks on heavily filtered/high-cardinality cube workloads; their speed has not been established by the typed-merge numbers above.
- **Redis/Valkey retrieval:** MGET uses chunks of 128 keys and at most two queued chunks, preserving key order and missing entries while overlapping the next reply with decoding. Empty requests issue no command. Outstanding client futures are canceled on interruption, timeout or failure. Production async waits respect the connection timeout.
- **Redis administration:** MEMORY USAGE commands are pipelined per SCAN page; unsupported/denied requests use batched STRLEN fallbacks. This removes the old sequential per-key wait. The shared connection's automatic flushing remains enabled.
- **Cache and bucket execution:** eligibility and query planning retain the original rules. Cache frames are now consumed in key order into an incremental accumulator; cold buckets still execute the original per-bucket queries. No multi-day query batching, daily result splitting, new time-alignment eligibility rule, or batch-specific row-limit retry is introduced.
- **Warming:** `WarmingOrchestrator` matches the baseline source byte-for-byte, including original existence checks, advisory coverage handling and per-bucket query execution.

MGET chunking limits the number of retained compressed replies, not the maximum bytes of an individual bucket. Decoded frames are consumed by the incremental merge and can be released after delivery; the legacy bulk-returning API remains available to other callers. Tiny values over high-latency networks can favor larger batches, so real network measurements are needed before claiming a Redis latency gain. No Redis or Valkey service was running on this PC; adapter command ordering, bounded prefetch and failure behavior were validated with controlled interface tests, not a live server throughput benchmark.

## Monitoring without default hot-path logging

The Spark executor, merge service and Valkey fetch path emit `FINE` stage timings when enabled. No SQL text, cache key values or row data is included. Timing and message construction are skipped when those loggers are disabled.

Use `docs/performance-logging.properties` through the JVM option:

```text
-Djava.util.logging.config.file=docs/performance-logging.properties
```

The logged stages are:

- Spark query setup, combined execution/output-mapping time, and output shape.
- Cache aggregate time, reconstruction/sort time, and input/output row counts.
- Valkey fetch elapsed time, non-overlapped reply wait, and combined decoding/consumer time.

Reply wait is only time spent awaiting futures; network work can overlap decoding. Spark execution and mapping also overlap while its iterator streams results, so that combined duration cannot be read as pure executor CPU time. Use a sampled JFR profile and Spark SQL UI task metrics to distinguish those costs in a representative deployment.

## Remaining workload-dependent limits

The SQL and Spark adapters keep event scans and GROUP BY inside Spark. Only grouped results enter the driver; the existing one-million-result-row ceiling remains. Increasing that ceiling is not a performance optimization and would increase driver memory pressure.

The deployed Fabric template defaults to two or three executors with three cores each, and 254 shuffle partitions. The standalone benchmark's local eight-worker setup is different. The launcher uses its loaded flat Spark configuration; changing an unused configuration derivation class does not tune the running cluster. AQE is enabled, but real Delta partition pruning, scan bytes, aggregate cardinality, shuffle/spill and task skew must be measured on actual tables before changing resource or shuffle settings. No arbitrary cluster tuning was applied.

A whole-query 10x improvement requires the optimized stages to dominate the original latency. For example, if merging was 80% of total latency, a 14.9x merge gain yields about 4x overall. Cold Spark execution, Redis bandwidth, serialization and source I/O can become the next dominant stage. The measured gains here support significant improvements, not a universal 10x claim.

## Validation and cleanup

The final full Maven reactor reported BUILD SUCCESS. Surefire XML reports contain **539 tests, zero failures, zero errors and zero skipped tests**. The final verification used the Maven Java launcher directly on Java 21 with the existing required module opens and in-process tests, avoiding this host's Maven batch-wrapper/fork issues.

Regression coverage includes exact numbers, null/NaN/signed-zero grouping, negative timestamps, decimal scales/overflow, schema compatibility, non-associative floating-point order, stable bounded sorting, codec compatibility/corruption, Redis ordering/failure cancellation and the original cache/warming business-rule tests. The time-flooring calculation already existed in the original aggregator; the optimization preserves it rather than introducing a new time-step rule. The primary agent reviewed subagent changes, found and corrected a reversed decompression API call during integration, and reran the combined suite afterward.

Temporary Java probes, generated source copies, dependency classpaths, benchmark logs, JFR recordings and Spark scratch directories remain under `.tmp-perf/`; Maven also created `lib_identity/.tmp-perf/`. Automatic approval review rejected both recursive deletion and a narrower file-by-file cleanup attempt with the message "blocked by policy". No further deletion was attempted after those rejections. No production dataset, existing project file, Maven dependency cache or pre-existing review checkout was deleted.

## Further memory improvements, implemented and benchmarked by the primary agent

This phase used no subagents. Its baseline is the already-optimized `35c66f7` source, rather than the older row-map aggregation implementation. Improvements from the two phases must not be multiplied into an end-to-end claim.

### Implementation

- **Packed, lazy validity:** non-null columns allocate no per-row validity array. Nullable columns use one bit per cell, initialized valid; only nulls write bits. Existing typed getters, row views and builder snapshot immutability remain intact.
- **Resolved read-only column readers:** aggregation and Arrow encoding resolve typed array access once, then scan without repeated frame-level type checks. Backing arrays remain private.
- **Bounded UTF-8 reuse:** encoding reuses UTF-8 byte arrays for repeated values. Decoding checks bytes before constructing strings and reuses immutable values on a hit. Dictionaries have limits of 16,384 entries and 1 MiB of key bytes, remain local to one codec operation, and stop caching predominantly distinct columns. Smaller batches use a shorter sample. Empty/all-null columns do not allocate dictionary tables.
- **Direct-buffer compression:** frames with at least 4,096 rows write Arrow IPC buffers directly into a native Zstandard streaming context through bounded scratch buffers. A counting pass advances buffer positions without copying payloads, and pledging that exact byte count preserves the original known-length zstd envelope. Small frames keep the existing one-shot path. There is no new native aggregation library or Agrona dependency.
- **Ordered incremental aggregation:** production adapters deliver decoded frames in key order. The accumulator owns typed group keys and measures, rather than keeping representative input frames. AVG/composite reconstruction and ORDER BY/LIMIT occur once after all inputs. Schema failures and arithmetic failures are deferred to the original final fallback stage. Delivery indices/counts are checked, and missing buckets retain the original recovery query. Cold queries still run and store one complete bucket at a time.

A streaming-decompression experiment was discarded: it lowered temporary heap usage but slowed decoding on this host. The current decoder retains the existing one-shot zstd decompression, followed by the optimized typed-frame construction.

### Million-row comparison

Both implementations were loaded into isolated class loaders in the same Java 25 JVM, sharing the same Arrow/zstd dependencies. Each fixture has one million rows: STRING host, LONG timestamp, nullable DOUBLE sum, LONG count; one sum cell in 97 is null. Six alternating warmups precede nine alternating measurements. Values are compared row-for-row; merged outputs are compared; both codec versions read the other version's blobs. Medians from the final run:

| Cardinality | Operation | Before | After | Speedup | Java allocation before / after |
|---|---|---:|---:|---:|---:|
| 1,000 | Serialize | 138.83 ms | 100.39 ms | 1.38x | 313.04 / 11.15 MiB |
| 1,000 | Deserialize | 165.82 ms | 145.31 ms | 1.14x | 152.13 / 64.64 MiB |
| 1,000 | Merge | 53.74 ms | 39.79 ms | 1.35x | 0.135 / 0.128 MiB |
| 100,000 | Serialize | 133.62 ms | 100.43 ms | 1.33x | 313.11 / 41.81 MiB |
| 100,000 | Deserialize | 132.41 ms | 122.91 ms | 1.08x | 152.13 / 148.62 MiB |
| 100,000 | Merge | 92.50 ms | 73.18 ms | 1.26x | 15.57 / 14.44 MiB |

In the repeated-string case, a decoded host column retained **1,000 String objects instead of 1,000,000**. Validity array payloads across the four columns dropped from **4,000,000 bytes to 125,000 bytes**. Those are counts/payload sizes, not an estimate of total JVM/native memory. Compressed blob size changed by less than 0.1% in these fixtures.

A 1,000-row distinct-host probe remained approximately at parity for decoding (0.617 vs 0.618 ms); its serialization and merge medians were slightly slower in one short run (1.433 vs 1.529 ms and 0.750 vs 0.774 ms), with lower allocation. Small-operation timings were noisier, and there is no claim of a speedup for every tiny frame.

### Many-bucket memory test

The actual cache execution engine read 50 serialized Arrow/zstd buckets from the in-memory backend, 50,000 rows each: **2.5 million input rows**, 1,000 output groups. No Spark query was allowed in this complete-hit fixture. The benchmark checked total counts and sums against the generated inputs. One warmup preceded five timed executions, with explicit GC before each; both versions used a 512 MiB heap.

| Measurement | Before | After |
|---|---:|---:|
| Median decode + merge query time | 444.80 ms | 336.58 ms |
| Sum of heap-pool peak usage | 323.38 MiB | 119.09 MiB |
| Aggregate checksum | 128113000 | 128113000 |

That is approximately **1.32x faster** and **63% lower summed heap-pool peaks**. Pool peaks are measured separately and may occur at different times; their sum is a diagnostic comparison, not an exact simultaneous process RSS or native-memory peak.

With the heap capped at **128 MiB**, the baseline decoder raised `OutOfMemoryError: Java heap space` and the engine attempted its authoritative fallback. The enhanced complete-hit path finished with the correct answer: 325.75 ms median, 25.02 MiB summed heap-pool peaks.

An expanded **200-bucket / 10-million-row** complete-hit case also passed at **128 MiB heap**, with 1,000 output groups, a 1,210.25 ms median and checksum `512452000`. Summed heap-pool peaks were 63.37 MiB; JVM garbage-collection sizing and the retained compressed test store affect that number.

These tests exercise binary decoding and the real engine/merge code, not a live Redis server or network. Memory now scales with distinct aggregation groups, the current input frame and bounded compressed replies, rather than all decoded buckets simultaneously. A very large final group count/output can still consume substantial memory. Cube catalog retention and its eviction policy were not changed.

### Preserved business rules and verification

Warming, safety/bypass rules, time-bucket calculations, query hashes and cache keys remain unchanged. No new cacheability decision or time-flooring rule was added. The original time flooring and floating-point accumulation order are preserved. Tests cover two-minute steps/negative timestamps, schema-only empty frames, nullable word boundaries, immutable readers, exact numbers, reconstruction after all inputs, malformed streaming delivery and the existing gap/cold-cache/fallback behavior. Large direct-compression output is compared against the one-shot IPC bytes, and legacy level-9 blobs remain readable.

The final full reactor validation is recorded above. Benchmark sources, baseline class snapshots and raw measurements are in the existing `.tmp-perf/` scratch directory; deletion of those directories remains blocked by automatic approval review.