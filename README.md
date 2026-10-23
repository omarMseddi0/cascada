# Cascada

A lakehouse query engine with a smart bucket cache



## Modules

| Module | Contents |
|---|---|
| `lib_identity` | Framework-free query identity: `QueryHash` |
| `lib_cache` | The cache domain + runnable engine: canonical query object, bucket math + pyramid, logic hashing, safety rules, columnar merge (`application/service/merge/columnar`), AVG reconstruction, cube subsumption + consistency verifier, warming queue + popularity tracker, coverage bitmaps, `ResultFrame` + Arrow/zstd serialization, in-memory + Lettuce/Valkey backends, `CacheExecutionEngine` |
| `lib_sql` | Calcite SQL compiler: canonical extraction (AVGâ†’SUM/COUNT, group-by/filter/time detection, HAVING/JOIN/DISTINCT logic signature, per-alias aggregate map), MySQLâ†’Spark dialect translation, gap-query builder, cache-correctness simulation gate |
| `lib_spark_config` | Pure three-knob (RAM/CPU/placement) Spark config derivation, golden-tested key-by-key |
| `lib_spark` | SparkSession assembly and the Spark/Delta execution adapter |
| `lib_fabric` | Cluster deployment: one self-contained YAML source applied via Fabric8 (ServiceAccount/RBAC, ConfigMaps, executor pod template, driver Deployment), tested against the Fabric8 mock server |
| `app` | Wiring. `mvn -Plocal-spark` bundles Spark 4.1.1 + Delta 4.1.0; Velox is the primary Linux execution backend (Java 21) |

## Model package convention

Production Java records live in a dedicated `model` package within their owning layer:
`domain/model/<subdomain>`, `application/model/<concern>`, or
`adapter/<direction>/<technology>/model`. Application settings live in `app/model/config`.
Services and ports reference these models; model placement does not change dependency direction.
A Java record represents data and can be a value object or DTO; being a record does not make it an entity.
Adapter models may use adapter technologies, while domain and application models remain framework-free.
The sealed `CompositeFormula` contract stays with its record implementations.
Test-local fixtures stay in tests. `AppArchitectureTest` checks record placement across the platform.

This is Cascada's package convention, based on the boundary separation described in
[Cockburn's original hexagonal architecture article](https://alistair.cockburn.us/hexagonal-architecture).

## Java and Agrona runtime

The project targets Java 21 and uses Agrona **2.6.1** (latest stable release verified on 4 October 2026).
Java does not need an upgrade for this Agrona release. Agrona 2.x requires these options on the JVM
running Cascada, including the Spark driver JVM:

```text
--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
--add-opens=java.base/java.util.zip=ALL-UNNAMED
```

Keep the existing Arrow/Spark module options as well. Maven tests and the benchmark/resource runners
include the required options automatically. For an application launch, add them to `JAVA_TOOL_OPTIONS`
or the JVM startup arguments; for `spark-submit`, include them in `--driver-java-options` before the
Spark driver starts. Configuring driver JVM options after an in-process SparkSession starts is too late.

## Build and test

```bash
cd cascada
mvn -N install   # parent BOM, first time only
mvn test
```

For constrained local Spark comparisons against full-window SQL, run
`./scripts/Test-LocalSparkConsistency.ps1 -Rows 5000000` in PowerShell. The runner uses
two Spark task slots and a 1,536 MiB JVM heap. See the
[E2E SQL consistency report](E2E_README.md#cache) for findings, coverage and current
logical SQL/cache/cube support boundaries.




## What the tests pin down

- AVG reconstruction divides summed SUM/COUNT, never averages averages.
- Bucket boundary algebra partitions head/body/tail with no gap or overlap; epoch resampling
  floors to step buckets, never to zero.
- The logic hash ignores the time range and clause order, but changes with aggregates, group-by,
  filters, step, HAVING, JOIN ON conditions, and DISTINCT. The canonical semantics version also
  separates identities produced by different projection-order rules.
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

## API process and existing library integration

`CascadaLauncher` starts persistent HTTP (8000) and gRPC (9090) APIs configured through environment variables and settings files.
The runtime wires the existing SQL translator/canonicalizer/gap rewriter, cache execution and
administration services, warming orchestrator, Spark executor, and Fabric cluster lifecycle ports.
Spark is initialized once and shared; each request and asynchronous cache gap runs on a separate
Java 21 virtual thread. Per-request Spark job groups isolate cancellation, including future jobs in
the cancelled group. A cancelled request does not stop the shared Spark session.

HTTP endpoints:

| Method | Endpoint | Existing operation |
|---|---|---|
| POST | `/query` | Logical SQL; `{"sql":"SELECT ...", "format":"json"}` or `"parquet"` |
| GET | `/cache/size` | Bytes, MB and bucket count |
| POST | `/cache/flush` | `{}` flushes Cascada buckets; `{"prefix":"QC:V4:..."}` selects a prefix |
| POST | `/cache/warm` | `{"start":0,"end":86399,"forceOverwrite":false}` runs the existing warmer |
| POST | `/cluster/deploy`, `/cluster/stop`, `/cluster/start`, `/cluster/restart`, `/cluster/delete` | Existing Fabric lifecycle operations; body is a map of deployment setting names to strings |
| GET | `/health` | API process health; does not start Spark or contact Kubernetes |

The gRPC contract is `app/src/main/proto/query.proto`. `QueryService.Query` streams typed protobuf
rows by default, or JSON/Parquet payload chunks. `CacheService` exposes measure/flush/warm;
`ClusterService.ChangeCluster` drives the existing lifecycle service. The first query message
contains column metadata, row count, content type, and cache-path status. Concatenate encoded payload
chunks to recover the JSON document or Parquet file. Protobuf null cells have no value set; decimals
use exact decimal strings. Invalid request shape returns HTTP 400; unsupported logical translation
returns 422. Server configuration/execution failures return safe 5xx errors.

There are **no application row/byte result ceilings or request concurrency gates**. Spark's
`spark.driver.maxResultSize` and the actual driver JVM/container resources are operator-controlled.
Fifty-five million rows are not rejected merely for their count. `ResultFrame` still materializes
primitive columns on the driver: results must fit the heap you provision. Response encoding streams
without constructing a second whole JSON document or row-map table. Network serialization remains
necessary. gRPC backpressure waits for the receiver or its cancellation/deadline.

## Grouped application configuration

Set `CASCADA_CONFIG_PATH` to the JSON application configuration. `server`, `warming` and `tables`
are bound once to immutable typed settings. `CASCADA_HTTP_PORT`/`CASCADA_GRPC_PORT` override file
ports; legacy engine environment settings continue to work.

```json
{
  "server": {"httpPort":8000,"grpcPort":9090,"shutdownTimeout":"PT30S"},
  "warming": {"enabled":true,"interval":"PT5M","lookback":"P7D","maximumPatterns":10000},
  "tables": [{
    "logicalName":"traffic", "deltaPath":"hdfs://lake/traffic",
    "columnMappings":{"ts":"ts","country":"C5","bytes":"C6"},
    "timeColumns":["ts"]
  }]
}
```

Configured physical time columns must match `CASCADA_TIME_COLUMN`, the existing cache merge clock.
Use SQL aliases for the desired output column labels. Table configuration is loaded at startup;
this is not a new live table-registration feature. Warming runs only complete buckets and has
bounded, configurable observation retention. Disabling scheduled warming also disables automatic
query observation recording. Cache size/flush/warm remain available as existing admin operations.

`SPARK_CONFIG_PATH` accepts flat Spark JSON or grouped `common`/`delta`/`native` sections. It is
validated before binding API sockets, while actual Spark initialization remains lazy. Explicit
Spark environment overrides take priority, including `SPARK_DRIVER_MAX_RESULT_SIZE`.

## Single-file Fabric deployment

`lib_fabric/src/main/resources/fabric/deployment.yaml` is the only deployment resource source.
It embeds RBAC, Spark JSON, application JSON, Hadoop XML, logging, the executor pod template,
FAIR allocation XML, driver Deployment and Service. The default scheduler pool is explicitly FAIR
so concurrent jobs share the Spark task slots. One shared driver uses a headless service and
`Recreate` updates; multiple driver replicas behind that hostname are rejected.

Manifest substitution accepts simple scalar values and rejects quotes,
line breaks, and YAML/XML structural characters; percent-encode special characters in URLs.

`ClusterManifestRenderer.renderCombined(values)` produces one multi-document YAML stream;
Fabric8 applies the same documents. The source must be rendered before applying. The namespace,
image pull secret, storage and Valkey endpoint are supplied by the operator. Kubernetes access is
lazy; query-only startup does not open a control-plane client. No deployment was made during this work.

`CASCADA_DRIVER_MEMORY` controls the actual `-Xmx` and corresponding pod memory in the rendered
Deployment. `CASCADA_DRIVER_MAX_RESULT_SIZE` controls the Spark property (the supplied reference
value is 6g). Set these for your workload; the application does not substitute result-size limits.

## Java 21, Spark and Gluten/Velox

The reactor targets **Java 21**, **Spark 4.1.1 / Scala 2.13.17**, and the matching
**Delta 4.1.0 `delta-spark_4.1_2.13` artifact**. Shared Jackson, Arrow, Parquet, Hadoop, Protobuf and Netty versions
are aligned with that Spark runtime. A Java 17 image cannot run the release-21 application.
See [Spark 4.1.1](https://spark.apache.org/docs/4.1.1/) and
[Delta 4.1's Spark compatibility](https://github.com/delta-io/delta/releases/tag/v4.1.0).

Fabric configures `org.apache.gluten.GlutenPlugin`, native columnar shuffle, and an explicit
executor heap/off-heap/overhead split. `CASCADA_EXECUTOR_RAM_GB` and `CASCADA_GLUTEN_ENABLED`
control the existing memory derivation; the same compatible runtime image must be used by driver
and executors. AQE stays enabled. Driver and executor Java native/module-access options are explicit.

The Linux image must include a **Gluten Velox bundle built with `-Pdelta` for Spark 4.1 / Scala 2.13**
as `/opt/spark/jars/gluten.jar`, plus the matching Spark and Delta jars. The bundle is first
on the driver classpath so Gluten's Spark overrides take precedence. A Spark-3.5 bundle cannot be reused.
The generic Gluten 1.7.0 Spark-4.1 prebuilt bundle used in the historical benchmark lacks
the optional native Delta adapter. Build the coherent backend/package reactor with explicit
`-Pdelta`; adding only the ordinary Delta SQL extension does not supply that adapter.
See [the pinned build configuration](https://github.com/apache/gluten/blob/v1.7.0/pom.xml) and
[Delta support](https://github.com/apache/gluten/blob/v1.7.0/docs/get-started/VeloxDelta.md).
Java-17-built Gluten classes run on the Java 21 driver;
Cascada itself requires Java 21. Configuration validates the bundle's Spark minor version, Scala binary version
and Velox backend before session startup, and requires the Delta scan classes and backend component marker.
Native execution must run in Linux containers or WSL;
native Windows is rejected explicitly.
Keep the Spark image's executor entrypoint; the app Dockerfile retains it.

Velox is enabled by default by `SparkSessionConfigBuilder`. `CASCADA_GLUTEN_ENABLED=false`
selects JVM-only Spark for native Windows development. `CASCADA_GLUTEN_OFFHEAP_BYTES`
sets local native memory (default 536870912 bytes); `CASCADA_GLUTEN_JAR` adds a matching
bundle to `spark.jars` for executor distribution. The driver must also include that JAR
in its JVM classpath before it starts. Setting `spark.driver.extraClassPath` after an
in-process JVM has started cannot make plugin classes available.

The native profile defaults `spark.sql.ansi.enabled=false`: Gluten 1.7 otherwise falls back
for Spark 4's ANSI mode. This uses Spark's legacy invalid-cast/arithmetic behavior. Set the
property to `true` explicitly when ANSI errors are required, accepting native fallback.
Native sessions now default to `spark.gluten.memory.isolation=true` for concurrent-query
services and tracked automatic spilling. Gluten derives task-slot memory quotas from Spark's
resources. Operator overrides remain supported; executor classpaths also prepend the bundle.
Enabling Gluten without an explicit off-heap environment override preserves an existing
configuration-file budget. The [six-core local profile](config/spark-gluten-local.json) uses
6 GiB off-heap, ANSI=false, UTC and 48 shuffle partitions. Set `SPARK_MASTER=local[6]`
and `SPARK_CONFIG_PATH` to that file before starting the Linux JVM with a 3 GiB heap.

Build and validate locally through Docker:

```powershell
./scripts/Build-GlutenDeltaBundle.ps1 -Jobs 2
# Use the candidate directory printed by the successful build:
./scripts/Complete-GlutenDeltaValidation.ps1 -CandidateDirectory '<candidate-directory>' -RemoveBuildImage
```

The build records its image digest and profiles, builds native libraries first, then uses
Gluten's Maven wrapper. Validation checks actual native Delta scans before running real
Valkey warming/concurrency and the randomized 10 GB two-round benchmark. The
[central E2E README](E2E_README.md) contains the latest native Delta results,
dataset/schema, exact SQL, CPU/RAM settings and all measurement/warm-up times.

```sh
mvn -Pdistribution -pl app -am package -DskipTests
docker build --build-arg SPARK_RUNTIME_IMAGE=<matching-spark-delta-gluten-java21-image> -t cascada:0.1.0 .
```

## Verification

Unit/architecture and network tests cover the existing library graph, alias normalization for cube
reuse, error classification, warming, cache administration, cluster port calls, cancellation, and
52 simultaneous HTTP plus 52 simultaneous gRPC requests on virtual threads. Verified exact cube
hits return the original immutable frame; JSON reads primitive columns; gRPC chunks transfer owned
buffers without reusing published arrays. The columnar merge algorithm remains the existing engine.

Real local Spark tests exercise 52 independent job groups on one session, a million-row scalar codec
round trip, and an opt-in 55-million-row result/JSON encoding test. Enable native Spark tests with
`-Plocal-spark -Dcascada.spark.integration=true`; the large-result test additionally uses
`-Dcascada.large.result.integration=true`. Linux Velox offload, production HDFS/Valkey and live
Kubernetes remain environment-specific checks. Linux Docker validation subsequently passed
the native Delta gate, both 88-test cache/process suites and the full randomized 10 GB matrix.
See the [E2E README](E2E_README.md) for resource limits and unresolved stability findings.
# Local Spark, Velox and Valkey validation

Read [E2E_README.md](E2E_README.md) for the latest native Delta validation:
data types and distributions, row/byte counts, exact SQL, cache/cube boundaries,
Valkey warming/concurrency, every measured run and warm-up, native Delta configuration,
bundle location and reproduction commands. Raw and historical evidence is retained
in one verified local archive linked there. Runners write ignored temporary evidence and
refresh that single README; they do not create separate report Markdown files.
