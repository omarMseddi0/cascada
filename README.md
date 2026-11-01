# Cascada

Cascada is a Java lakehouse query engine that uses a time-bucketed cache to reuse results of analytical queries. It accepts SQL, identifies reusable cached buckets, executes missing ranges against Spark and Delta Lake, and merges partial results into the requested output. Queries that cannot safely use the cache take the direct execution path.

The cache stores columnar result frames and supports in-memory and Valkey backends. Its merge pipeline combines aggregates across buckets, including reconstructing averages from sums and counts. Cache warming and query popularity tracking can prepare frequently used results in the background. The application exposes HTTP and gRPC APIs for queries, cache administration, and cluster lifecycle operations.

## How it fits together

| Module | Responsibility |
| --- | --- |
| `lib_identity` | Query identity value objects. |
| `lib_sql` | SQL parsing, canonicalization, cacheability checks, and rewriting missing ranges. |
| `lib_cache` | Bucket coverage, cache backends, result frames, merging, and warming. |
| `lib_spark` | Spark session and Spark/Delta query execution adapters. |
| `lib_spark_config` | Spark configuration derivation. |
| `lib_fabric` | Kubernetes cluster configuration and lifecycle operations. |
| `app` | Application wiring, configuration, HTTP and gRPC servers. |

## Build and test

Use JDK 21 and Maven. From the repository root:

```sh
mvn test
mvn -Pdistribution -pl app -am package -DskipTests
```

The reactor targets Java 21, Spark 4.1.1, Scala 2.13, and Delta Lake 4.1. The distribution expects its Spark runtime dependencies to be supplied by the deployment environment. The [Dockerfile](Dockerfile) describes the required runtime image, including the compatible Gluten/Velox Delta bundle. The `local-spark` Maven profile supplies Spark and Delta dependencies for local development; native execution still requires a compatible runtime.

## Configuration and API

The server loads deployment settings from the JSON file specified by `CASCADA_CONFIG_PATH`, with supported environment variables overriding settings. These include `CASCADA_RUN_MODE`, `CASCADA_MAIN_TABLE_NAME`, `CASCADA_MAIN_TABLE_PATH`, `CASCADA_CACHE_BACKEND`, `CASCADA_HTTP_PORT`, and `CASCADA_GRPC_PORT`. Spark properties can also be supplied through `SPARK_CONFIG_PATH`; see the configuration readers in `app` and the [example Spark configuration](config/spark-gluten-local.json) for details. A suitable Delta table and Spark runtime are needed to execute queries.

HTTP provides `GET /health` and `POST /query`, as well as cache and cluster administration routes. For example, once a server and its data source are configured:

```sh
curl -X POST http://localhost:8000/query \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT COUNT(*) AS total FROM my_table","format":"json"}'
```

Query responses can be JSON or Parquet over HTTP. The [gRPC contract](app/src/main/proto/query.proto) defines streaming query results and cache and cluster services. Exact supported cache shapes and deployment settings are determined by the implementation and tests; a successful direct query does not necessarily imply it was served from cache.
