# Readability, streams, and logging review

Reviewed 17 October 2026. Priority: query speed and low memory use before shorter syntax.

## Coverage

Four parallel reviewers covered SQL, cache, platform/configuration, and logging. The production
inventory and logging scan covered 239 Java files across all seven modules, eight Maven POMs,
and deployment resources. SQL review covered all 30 production files. Cache review enumerated
139 production files, examined every stream/toList/toArray site, and inspected merge, warming,
time-bucket, presence, and cube paths more closely. Platform review covered app, fabric, identity,
Spark, and Spark configuration. Generated classes, downloaded native sources, and test/probe
console output were excluded from production findings.

This was a static review plus regression testing, not a throughput or allocation benchmark.
The checkout already contained extensive changes; those were preserved.

## Changes made

* `CascadaLauncher`: replace the one production println with the existing JUL logger and lazy
  startup message; reuse a static logger for shutdown failures. The startup message now follows
  logger configuration (the default ConsoleHandler writes to stderr).
* `WarmingOrchestrator`: guard two FINE messages before formatting, avoiding concatenation and
  capturing-supplier construction when FINE is disabled. Correct the skipped-bucket diagnostic:
  decimal composite aggregates can cause a skip even when the row cap was not exceeded.
* `GlutenRuntimeValidator`: take one local `sparkProperties()` snapshot. That accessor returns
  a defensive map copy; native startup previously requested up to five copies. Encapsulation and
  validation rules are unchanged.
* `SparkConfigurationFileReader`: recurse directly over nested maps instead of copying each
  section into a temporary LinkedHashMap. Preserve traversal and last-value-wins behavior.
  Add regression coverage for nested groups, duplicate settings, and invalid nested values.
* `performance-logging.properties`: point merge diagnostics at `IncrementalFrameMerge`, the
  class that actually emits them.

No query scanner, canonicalization, aggregation, serialization, or transport data loop was changed.
No dependencies, async logging queues, threads, or caches were added.

## SQL: keep stateful loops

| Source | Why the loop remains appropriate |
| --- | --- |
| `SqlTextScanner`, `CalciteSql.quoteReservedTimestampIdentifiers` | Quote/comment masks, cursor advancement, escaping, and token boundaries are stateful. |
| `MySqlToSparkFunctionTranslator` | Parenthesis depth, argument boundaries, and Matcher append state need explicit sequencing. |
| `SqlTrees` recursive walkers | Depth-first visits, alias exclusion, and early exits are clearer without a pipeline per AST node. |
| `GapQueryBuilder` | Range coalescing carries the previous interval; other traversals mutate AST nodes or deduplicate projections. |
| `CalciteCanonicalObjectFactory`, `AggregateNormalizer` | Visitors collect multiple related outputs and preserve identity/alias behavior. |

`RegisteredTable` and `TimeDimensionMap` already use concise normalization streams. Existing
map/filter pipelines that return required lists or sets can remain. There was no compelling
loop-to-stream conversion that improved readability enough to justify changing the query path.
Do not introduce parallel streams into query processing without workload-specific evidence.

The same reasoning applies to primitive frame accumulation, Spark result mapping, direct JSON
and Parquet output, and bounded gRPC chunk copying. Keep reverse-order resource cleanup and
Fabric's parse-all-manifests-before-apply ordering.

## Logging decision

After the launcher change, production `src/main` contains no System.out, System.err, or
printStackTrace calls. Cascada uses java.util.logging in 11 classes. Existing Spark, Valkey,
and merge timing messages already gate timing and formatting with FINE checks. Warning/error
messages remain on failure paths. HTTP writes and gRPC sends are response payloads.

The parent POM manages an slf4j-api version; dependencyManagement alone does not install a
provider. Spark's deployment Log4j2 configuration does not configure Cascada's JUL loggers.
Test runs report missing SLF4J providers; the deployed Spark runtime's provider must be checked
in that runtime before drawing a production conclusion.

Keep JUL for this change: the source shows sparse diagnostics, not per-row logging, and there
is no measured logging bottleneck. Log4j2 async logging may help a logging-heavy workload,
but it adds a ring buffer/background processing and a queue-full policy. It is not a free
memory or latency improvement. A migration should compare request latency, allocation, queue
memory, shutdown flushing, and saturated-output behavior on the deployed runtime.

References: [Log4j async logging](https://logging.apache.org/log4j/2.x/manual/async.html),
[Java 21 streams](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/stream/Stream.html).

## Follow-up candidates left unchanged

These require separate behavior decisions or measured changes; they are not claimed speedups.

* `app/.../concurrent/RequestExecutor`: virtual-thread-per-task execution has no application
  concurrency cap, and ServerSettings has no admission-limit setting. Simultaneous queries can
  multiply retained result memory even though chunks are bounded. Decide the overload policy
  and deployment-wide memory budget before adding admission control.
* `InMemoryBlobCacheBackendAdapter.sizeReport`: collects all live bucket byte-array references
  before summing. Running byte/count totals could remove an O(bucket-count) temporary reference
  list. This is the local/test backend, not the production Valkey sizing path.
* `DataChangeDetector.signature`: creates a SHA-256 digest per row and repeatedly resolves
  column indexes/null checks. Investigate digest reuse and prepared indexes while preserving
  exact signature bytes, ordering, nulls, and baseline compatibility.
* `CubeFilterEvaluator` and cube roll-up/verification: prepare predicates and column indexes
  once rather than reparsing filters per row. Preserve numeric, decimal, NULL, and IN semantics;
  measure whether lightweight row-view wrappers escape before replacing them.
* `ShapeLatticeIndex`: candidate sorting followed by projection creates another reference list.
  Review its ordering and admission semantics before consolidating collections.
* `TimeBucketCalculator.getCompletedBuckets`: an ordered prefix can be identified without two
  streams. Also review `nowSeconds - secondsPerBucket` at Long.MIN_VALUE for underflow; cover
  extreme timestamps before changing the comparison.

For any later hot-path refactor, compare equivalent workloads using the existing benchmark
tools, including allocation/GC and peak memory as well as latency. Correctness tests alone do
not establish performance equivalence.

## Validation

* `scripts/Test-LocalSparkConsistency.ps1 -UnitTests -HeapMb 1536`: reactor BUILD SUCCESS across
  all seven modules. SQL reported 193 tests with no failures/errors. Environment-gated Spark,
  Valkey, and large-result integration tests were skipped; native/Linux behavior was not exercised.
* After the configuration changes, reran the script with
  `-TestClass 'SparkConfigurationFileReaderTest,GlutenRuntimeValidatorTest' -HeapMb 1536`:
  BUILD SUCCESS, six tests passed, none skipped.
* Production console-print scan found no matches. Whitespace diff checks passed for the tracked
  files changed here. No speedup or peak-memory benchmark claim is made.
