# Model and record package refactor — isolated publication

## Exact scope and baseline

This branch is `refactor/model-record-packages`, based on remote `origin/main` at `6c0be454ca63a1069885508fe64e68239c24beea`. It contains one record/model refactor commit. The other 24 local commits remain on local `main` and are not ancestors of this branch or included in its push. The main checkout and its complete API-only work are preserved.

Move **54 existing production records** into dedicated model packages within their existing owning layers. Preserve record components, validation and helper logic; update consumer/test imports and fully qualified references. Widen formerly package-private record types/constructors/helpers where the new package boundary requires access. Test-local records remain fixtures in their tests. Existing architecture assertions target the relocated packages. No enum reorganization, entity redesign, feature implementation, dependency change, CLI conversion, sketch removal or configuration-algorithm change is included.

The remote baseline still contains some library types later removed on local main; their relocation here does not publish their later removal. Likewise, this branch does not publish the API-only launcher conversion: it contains only the requested record moves and required reference/verification updates relative to the remote baseline.

## Verification verdict

- PASS: normalized before/after source audit preserves components, validation and helper bodies for all 54 records, allowing package/import/access changes only.
- PASS: other authored Java changes are reference/formatting updates; the only policy adjustment is the existing app architecture test retargeting moved model packages.
- PASS: Java 21 `mvn -B -pl lib_identity test '-DforkCount=0'` reports BUILD SUCCESS, 13 executions, zero failures/errors.
- INHERITED FAILURE: full `mvn -B -DskipTests test-compile` fails before and after this refactor on the baseline's missing helper/contracts (for example CompositeFormula, CompositeExpressionNode, IncrementalFrameMerge, IndexedShape and freshness port methods). Both captured compiler outputs report the same diagnostic category counts: cannot-find-symbol and missing-override errors. Model-package diagnostic locations change with the refactor. Compiler diagnostics are capped, so this is not proof of a successful full build or exhaustive error equivalence. No unrelated helper implementation is added to disguise the baseline failure.
- PASS: staged whitespace and record/reference-only scope checks. The commit is intended for a separate remote review branch, not a verified main release; one-commit ancestry and the remote ref are checked during publication. No native/integration/deployment result is claimed.

## Record relocation verdicts

Every row is ACCEPT — move data type/package and required access; preserve implementation and record components.

| Old path | Replacement path | Verdict |
|---|---|---|
| `app/src/main/java/com/cascada/app/adapter/in/api/ClusterChangeResult.java` | `app/src/main/java/com/cascada/app/adapter/in/api/model/ClusterChangeResult.java` | ACCEPT — record relocation |
| `app/src/main/java/com/cascada/app/adapter/in/api/ParquetResult.java` | `app/src/main/java/com/cascada/app/adapter/in/api/model/ParquetResult.java` | ACCEPT — record relocation |
| `app/src/main/java/com/cascada/app/config/ApplicationSettings.java` | `app/src/main/java/com/cascada/app/model/config/ApplicationSettings.java` | ACCEPT — record relocation |
| `app/src/main/java/com/cascada/app/config/CacheSafetySettings.java` | `app/src/main/java/com/cascada/app/model/config/CacheSafetySettings.java` | ACCEPT — record relocation |
| `app/src/main/java/com/cascada/app/config/EngineSettings.java` | `app/src/main/java/com/cascada/app/model/config/EngineSettings.java` | ACCEPT — record relocation |
| `app/src/main/java/com/cascada/app/config/TableSettings.java` | `app/src/main/java/com/cascada/app/model/config/TableSettings.java` | ACCEPT — record relocation |
| `app/src/main/java/com/cascada/app/config/WarmingSettings.java` | `app/src/main/java/com/cascada/app/model/config/WarmingSettings.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/adapter/out/cache/PendingMget.java` | `lib_cache/src/main/java/com/cascada/cache/adapter/out/cache/model/PendingMget.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/adapter/out/cache/StoredBaseline.java` | `lib_cache/src/main/java/com/cascada/cache/adapter/out/cache/model/StoredBaseline.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/adapter/out/cache/StoredBlob.java` | `lib_cache/src/main/java/com/cascada/cache/adapter/out/cache/model/StoredBlob.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/adapter/out/tracking/RankedQuery.java` | `lib_cache/src/main/java/com/cascada/cache/adapter/out/tracking/model/RankedQuery.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/application/config/CacheExecutionConfiguration.java` | `lib_cache/src/main/java/com/cascada/cache/application/model/config/CacheExecutionConfiguration.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/application/config/WarmingConfiguration.java` | `lib_cache/src/main/java/com/cascada/cache/application/model/config/WarmingConfiguration.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/application/execution/CancellationRegistration.java` | `lib_cache/src/main/java/com/cascada/cache/application/model/CancellationRegistration.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/application/result/PatternWarmingResult.java` | `lib_cache/src/main/java/com/cascada/cache/application/model/PatternWarmingResult.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/application/result/QueryResult.java` | `lib_cache/src/main/java/com/cascada/cache/application/model/QueryResult.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/application/result/WarmingReport.java` | `lib_cache/src/main/java/com/cascada/cache/application/model/WarmingReport.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/application/service/CompositeTerm.java` | `lib_cache/src/main/java/com/cascada/cache/application/model/CompositeTerm.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/domain/admin/CacheSizeReport.java` | `lib_cache/src/main/java/com/cascada/cache/domain/model/admin/CacheSizeReport.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/domain/cube/CachedShapeEntry.java` | `lib_cache/src/main/java/com/cascada/cache/domain/model/cube/CachedShapeEntry.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/domain/cube/CubeAverageColumn.java` | `lib_cache/src/main/java/com/cascada/cache/domain/model/cube/CubeAverageColumn.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/domain/cube/CubeExpectedSchema.java` | `lib_cache/src/main/java/com/cascada/cache/domain/model/cube/CubeExpectedSchema.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/domain/cube/CubeFilterLiteral.java` | `lib_cache/src/main/java/com/cascada/cache/domain/model/cube/CubeFilterLiteral.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/domain/cube/CubePlannerGroup.java` | `lib_cache/src/main/java/com/cascada/cache/domain/model/cube/CubePlannerGroup.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/domain/cube/CubeVerifierGroup.java` | `lib_cache/src/main/java/com/cascada/cache/domain/model/cube/CubeVerifierGroup.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/domain/cube/QueryShape.java` | `lib_cache/src/main/java/com/cascada/cache/domain/model/cube/QueryShape.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/domain/hashing/HashComponents.java` | `lib_cache/src/main/java/com/cascada/cache/domain/model/hashing/HashComponents.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/domain/merge/AverageColumn.java` | `lib_cache/src/main/java/com/cascada/cache/domain/model/merge/AverageColumn.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/domain/query/CanonicalQueryObject.java` | `lib_cache/src/main/java/com/cascada/cache/domain/model/query/CanonicalQueryObject.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/domain/query/OrderByClause.java` | `lib_cache/src/main/java/com/cascada/cache/domain/model/query/OrderByClause.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/domain/query/PopularQuery.java` | `lib_cache/src/main/java/com/cascada/cache/domain/model/query/PopularQuery.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/domain/query/PostProcessing.java` | `lib_cache/src/main/java/com/cascada/cache/domain/model/query/PostProcessing.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/domain/query/QueryMetadata.java` | `lib_cache/src/main/java/com/cascada/cache/domain/model/query/QueryMetadata.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/domain/safety/CacheConfiguration.java` | `lib_cache/src/main/java/com/cascada/cache/domain/model/safety/CacheConfiguration.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/domain/time/DailyBuckets.java` | `lib_cache/src/main/java/com/cascada/cache/domain/model/time/DailyBuckets.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/domain/time/GapPlan.java` | `lib_cache/src/main/java/com/cascada/cache/domain/model/time/GapPlan.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/domain/time/TimeRange.java` | `lib_cache/src/main/java/com/cascada/cache/domain/model/time/TimeRange.java` | ACCEPT — record relocation |
| `lib_cache/src/main/java/com/cascada/cache/domain/validation/ValidationBaseline.java` | `lib_cache/src/main/java/com/cascada/cache/domain/model/validation/ValidationBaseline.java` | ACCEPT — record relocation |
| `lib_identity/src/main/java/com/cascada/identity/domain/LineageHash.java` | `lib_identity/src/main/java/com/cascada/identity/domain/model/LineageHash.java` | ACCEPT — record relocation |
| `lib_identity/src/main/java/com/cascada/identity/domain/PolicyVersion.java` | `lib_identity/src/main/java/com/cascada/identity/domain/model/PolicyVersion.java` | ACCEPT — record relocation |
| `lib_identity/src/main/java/com/cascada/identity/domain/QueryHash.java` | `lib_identity/src/main/java/com/cascada/identity/domain/model/QueryHash.java` | ACCEPT — record relocation |
| `lib_identity/src/main/java/com/cascada/identity/domain/SchemaVersion.java` | `lib_identity/src/main/java/com/cascada/identity/domain/model/SchemaVersion.java` | ACCEPT — record relocation |
| `lib_spark/src/main/java/com/cascada/spark/adapter/out/spark/SessionLease.java` | `lib_spark/src/main/java/com/cascada/spark/adapter/out/spark/model/SessionLease.java` | ACCEPT — record relocation |
| `lib_spark/src/main/java/com/cascada/spark/adapter/out/spark/SparkColumnPlan.java` | `lib_spark/src/main/java/com/cascada/spark/adapter/out/spark/model/SparkColumnPlan.java` | ACCEPT — record relocation |
| `lib_spark_config/src/main/java/com/cascada/sparkconfig/domain/SparkConfiguration.java` | `lib_spark_config/src/main/java/com/cascada/sparkconfig/domain/model/SparkConfiguration.java` | ACCEPT — record relocation |
| `lib_spark_config/src/main/java/com/cascada/sparkconfig/domain/SparkMemorySplit.java` | `lib_spark_config/src/main/java/com/cascada/sparkconfig/domain/model/SparkMemorySplit.java` | ACCEPT — record relocation |
| `lib_sql/src/main/java/com/cascada/sql/adapter/calcite/CanonicalTimeBound.java` | `lib_sql/src/main/java/com/cascada/sql/adapter/calcite/model/CanonicalTimeBound.java` | ACCEPT — record relocation |
| `lib_sql/src/main/java/com/cascada/sql/adapter/calcite/CanonicalTimeRangeExtraction.java` | `lib_sql/src/main/java/com/cascada/sql/adapter/calcite/model/CanonicalTimeRangeExtraction.java` | ACCEPT — record relocation |
| `lib_sql/src/main/java/com/cascada/sql/adapter/calcite/CanonicalTimeSeriesShape.java` | `lib_sql/src/main/java/com/cascada/sql/adapter/calcite/model/CanonicalTimeSeriesShape.java` | ACCEPT — record relocation |
| `lib_sql/src/main/java/com/cascada/sql/adapter/calcite/NormalizedAggregates.java` | `lib_sql/src/main/java/com/cascada/sql/adapter/calcite/model/NormalizedAggregates.java` | ACCEPT — record relocation |
| `lib_sql/src/main/java/com/cascada/sql/adapter/calcite/ParsedSqlQuery.java` | `lib_sql/src/main/java/com/cascada/sql/adapter/calcite/model/ParsedSqlQuery.java` | ACCEPT — record relocation |
| `lib_sql/src/main/java/com/cascada/sql/adapter/out/calcite/TimeBucket.java` | `lib_sql/src/main/java/com/cascada/sql/adapter/out/calcite/model/TimeBucket.java` | ACCEPT — record relocation |
| `lib_sql/src/main/java/com/cascada/sql/domain/RegisteredTable.java` | `lib_sql/src/main/java/com/cascada/sql/domain/model/RegisteredTable.java` | ACCEPT — record relocation |
| `lib_sql/src/main/java/com/cascada/sql/domain/TimeDimensionMap.java` | `lib_sql/src/main/java/com/cascada/sql/domain/model/TimeDimensionMap.java` | ACCEPT — record relocation |

## Consumer and test reference verdicts

Each path is ACCEPT — imports/fully qualified model references and necessary package assertions follow the moved records; no runtime algorithm is changed.

- `app/src/main/java/com/cascada/app/adapter/in/api/AdministrationRequests.java` — ACCEPT — update model references/formatting only.
- `app/src/main/java/com/cascada/app/adapter/in/api/QueryGrpcService.java` — ACCEPT — update model references/formatting only.
- `app/src/main/java/com/cascada/app/adapter/in/api/QueryHttpServer.java` — ACCEPT — update model references/formatting only.
- `app/src/main/java/com/cascada/app/adapter/in/cli/CascadaCli.java` — ACCEPT — update model references/formatting only.
- `app/src/main/java/com/cascada/app/adapter/in/scheduling/CacheWarmingScheduler.java` — ACCEPT — update model references/formatting only.
- `app/src/main/java/com/cascada/app/adapter/out/configuration/ApplicationSettingsReader.java` — ACCEPT — update model references/formatting only.
- `app/src/main/java/com/cascada/app/adapter/out/configuration/EnvironmentSettingsReader.java` — ACCEPT — update model references/formatting only.
- `app/src/main/java/com/cascada/app/adapter/out/execution/LazyQueryExecutor.java` — ACCEPT — update model references/formatting only.
- `app/src/main/java/com/cascada/app/bootstrap/CascadaLauncher.java` — ACCEPT — update model references/formatting only.
- `app/src/main/java/com/cascada/app/config/CascadaEngineFactory.java` — ACCEPT — update model references/formatting only.
- `app/src/test/java/com/cascada/app/adapter/in/scheduling/CacheWarmingSchedulerTest.java` — ACCEPT — update model references/formatting only.
- `app/src/test/java/com/cascada/app/adapter/out/configuration/EnvironmentSettingsReaderTest.java` — ACCEPT — update model references/formatting only.
- `app/src/test/java/com/cascada/app/adapter/out/execution/CloseTrackingQueryExecutor.java` — ACCEPT — update model references/formatting only.
- `app/src/test/java/com/cascada/app/adapter/out/execution/LazyQueryExecutorTest.java` — ACCEPT — update model references/formatting only.
- `app/src/test/java/com/cascada/app/architecture/AppArchitectureTest.java` — ACCEPT — retarget existing architecture assertions to the model packages.
- `app/src/test/java/com/cascada/app/bootstrap/CacheSqlSparkIntegrationTest.java` — ACCEPT — update model references/formatting only.
- `app/src/test/java/com/cascada/app/bootstrap/CascadaEngineFactoryTest.java` — ACCEPT — update model references/formatting only.
- `app/src/test/java/com/cascada/app/bootstrap/LocalSparkConsistencyIntegrationTest.java` — ACCEPT — update model references/formatting only.
- `app/src/test/java/com/cascada/app/bootstrap/ValkeySparkProcessIntegrationTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/adapter/out/cache/InMemoryBlobCacheBackendAdapter.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/adapter/out/cache/ValkeyCacheBackendAdapter.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/adapter/out/index/InMemoryCoverageIndexAdapter.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/adapter/out/sketch/HyperLogLogDistinctCounter.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/adapter/out/sketch/KllQuantileEstimator.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/adapter/out/tracking/QueryPopularityTracker.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/adapter/out/tracking/ValkeyQueryPopularityAdapter.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/execution/QueryCancellation.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/port/in/ExecuteCachedQueryUseCase.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/port/in/ExecuteLogicalQueryUseCase.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/port/in/MeasureCacheSizeUseCase.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/port/in/WarmCacheUseCase.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/port/out/CacheAdministrationPort.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/port/out/CacheValueSerializerPort.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/port/out/CoverageIndexPort.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/port/out/GapQueryRewriterPort.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/port/out/QueryPopularityPort.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/port/out/SqlCanonicalizerPort.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/port/out/ValidationBaselinePort.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/service/CacheAdministrationService.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/service/CacheExecutionEngine.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/service/CompositeFrameExpression.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/service/ExecuteCachedQueryService.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/service/ExecuteLogicalQueryService.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/service/FrameMergeService.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/service/WarmingOrchestrator.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/service/cube/CubeConsistencyVerifier.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/service/cube/CubeShapeAnalysis.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/service/cube/CubeShapeCatalog.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/service/cube/CubeSubsumptionPlanner.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/service/cube/CubeWindowShapes.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/service/cube/ShapeLatticeIndex.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/service/hashing/QueryHashGenerator.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/service/safety/HighCardinalityGroupByRule.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/service/safety/HotViewOnlyRule.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/service/safety/ImpossibleMathRule.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/service/safety/IncompatibleTimeStepRule.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/service/safety/LiquidClusteredFilterRule.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/service/safety/MinimumTimeRangeRule.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/service/safety/NonMergeableSqlFeatureRule.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/service/safety/PartialDayBucketRule.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/service/safety/RequiresAggregationRule.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/service/safety/SafetyRule.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/service/safety/SafetyRuleRegistry.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/service/time/TimeBucketCalculator.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/service/warming/DataChangeDetector.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/application/service/warming/WarmingQueue.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/domain/admin/CacheScope.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/domain/cube/CubeFilterEvaluator.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/main/java/com/cascada/cache/domain/key/CacheKeyFactory.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/adapter/out/cache/ValkeyCacheBackendAdapterTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/adapter/out/serialization/ArrowResultFrameSerializerTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/adapter/out/serialization/PortableFrameSerializerTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/adapter/out/sketch/SketchMergeErrorBoundTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/adapter/out/tracking/QueryPopularityTrackerTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/application/service/CacheAdministrationTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/application/service/CacheExecutionEngineCubeTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/application/service/CacheExecutionEngineTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/application/service/CoverageIndexedEngineTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/application/service/EngineTestFixtures.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/application/service/ExecuteCachedQueryServiceTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/application/service/FrameMergeServiceTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/application/service/FramePostProcessingTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/application/service/IncrementalFrameMergeTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/application/service/LiveBucketEngineTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/application/service/SmallRangeWarmingVoteTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/application/service/TypedMergeRegressionTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/application/service/WarmingFreshnessTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/application/service/WarmingOrchestratorTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/application/service/WarmingRetentionTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/application/service/cube/CubeConsistencyVerifierTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/application/service/cube/CubeSubsumptionPlannerDeepTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/application/service/cube/CubeTestFixtures.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/application/service/hashing/QueryHashGeneratorTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/application/service/safety/SafetyRulesTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/application/service/time/TimeBucketCalculatorTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/application/service/warming/DataChangeDetectorTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/application/service/warming/WarmingQueueTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/domain/cube/CubeShapeCatalogTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/domain/cube/CubeSubsumptionPlannerTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/domain/cube/ShapeLatticeIndexTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/domain/frame/ResultFrameTest.java` — ACCEPT — update model references/formatting only.
- `lib_cache/src/test/java/com/cascada/cache/domain/index/BucketCoverageBitmapTest.java` — ACCEPT — update model references/formatting only.
- `lib_fabric/src/main/java/com/cascada/fabric/adapter/out/kubernetes/FabricClusterDeployer.java` — ACCEPT — update model references/formatting only.
- `lib_fabric/src/main/java/com/cascada/fabric/application/port/in/ManageClusterLifecycleUseCase.java` — ACCEPT — update model references/formatting only.
- `lib_fabric/src/main/java/com/cascada/fabric/application/port/out/ClusterManifestRenderingPort.java` — ACCEPT — update model references/formatting only.
- `lib_fabric/src/main/java/com/cascada/fabric/application/service/ClusterLifecycleService.java` — ACCEPT — update model references/formatting only.
- `lib_fabric/src/main/java/com/cascada/fabric/application/service/ClusterSettingsService.java` — ACCEPT — update model references/formatting only.
- `lib_fabric/src/test/java/com/cascada/fabric/adapter/out/kubernetes/FabricClusterDeployerTest.java` — ACCEPT — update model references/formatting only.
- `lib_fabric/src/test/java/com/cascada/fabric/application/service/ClusterLifecycleServiceTest.java` — ACCEPT — update model references/formatting only.
- `lib_fabric/src/test/java/com/cascada/fabric/domain/ClusterValuesTest.java` — ACCEPT — update model references/formatting only.
- `lib_identity/src/test/java/com/cascada/identity/domain/IdentityValueObjectTest.java` — ACCEPT — update model references/formatting only.
- `lib_spark/src/main/java/com/cascada/spark/adapter/out/spark/SessionOwnershipCoordinator.java` — ACCEPT — update model references/formatting only.
- `lib_spark/src/main/java/com/cascada/spark/adapter/out/spark/SparkDeltaQueryExecutor.java` — ACCEPT — update model references/formatting only.
- `lib_spark/src/main/java/com/cascada/spark/adapter/out/spark/SparkResultFrameMapper.java` — ACCEPT — update model references/formatting only.
- `lib_spark/src/test/java/com/cascada/spark/adapter/out/spark/SessionOwnershipCoordinatorTest.java` — ACCEPT — update model references/formatting only.
- `lib_spark/src/test/java/com/cascada/spark/application/configuration/SparkSessionConfigBuilderTest.java` — ACCEPT — update model references/formatting only.
- `lib_spark_config/src/main/java/com/cascada/sparkconfig/application/service/SparkConfigurationDeriver.java` — ACCEPT — update model references/formatting only.
- `lib_spark_config/src/main/java/com/cascada/sparkconfig/domain/SparkSessionConfigurationAssembler.java` — ACCEPT — update model references/formatting only.
- `lib_spark_config/src/test/java/com/cascada/sparkconfig/domain/FullSparkConfigParityTest.java` — ACCEPT — update model references/formatting only.
- `lib_spark_config/src/test/java/com/cascada/sparkconfig/domain/GoldenSparkConfigurationTest.java` — ACCEPT — update model references/formatting only.
- `lib_spark_config/src/test/java/com/cascada/sparkconfig/domain/SparkConfigurationDeriverInvariantTest.java` — ACCEPT — update model references/formatting only.
- `lib_spark_config/src/test/java/com/cascada/sparkconfig/domain/SparkJsonGoldenFixture.java` — ACCEPT — update model references/formatting only.
- `lib_sql/src/main/java/com/cascada/sql/adapter/out/calcite/CalciteCanonicalObjectFactory.java` — ACCEPT — update model references/formatting only.
- `lib_sql/src/main/java/com/cascada/sql/adapter/out/calcite/CanonicalTimeSeriesAnalyzer.java` — ACCEPT — update model references/formatting only.
- `lib_sql/src/main/java/com/cascada/sql/adapter/out/calcite/GapQueryBuilder.java` — ACCEPT — update model references/formatting only.
- `lib_sql/src/main/java/com/cascada/sql/adapter/out/calcite/GapQueryRewriterAdapter.java` — ACCEPT — update model references/formatting only.
- `lib_sql/src/main/java/com/cascada/sql/adapter/out/calcite/LogicalToPhysicalSqlTranslator.java` — ACCEPT — update model references/formatting only.
- `lib_sql/src/main/java/com/cascada/sql/adapter/out/calcite/ScopedLogicalSqlTranslator.java` — ACCEPT — update model references/formatting only.
- `lib_sql/src/main/java/com/cascada/sql/adapter/out/calcite/SqlCacheabilityGuard.java` — ACCEPT — update model references/formatting only.
- `lib_sql/src/main/java/com/cascada/sql/adapter/out/calcite/SqlTrees.java` — ACCEPT — update model references/formatting only.
- `lib_sql/src/main/java/com/cascada/sql/adapter/out/calcite/TimeBoundClassifier.java` — ACCEPT — update model references/formatting only.
- `lib_sql/src/main/java/com/cascada/sql/adapter/out/calcite/TimeBuckets.java` — ACCEPT — update model references/formatting only.
- `lib_sql/src/main/java/com/cascada/sql/adapter/out/calcite/TimeStepNormalizer.java` — ACCEPT — update model references/formatting only.
- `lib_sql/src/main/java/com/cascada/sql/domain/TableCatalog.java` — ACCEPT — update model references/formatting only.
- `lib_sql/src/test/java/com/cascada/sql/adapter/calcite/CalciteCanonicalObjectFactoryDeepTest.java` — ACCEPT — update model references/formatting only.
- `lib_sql/src/test/java/com/cascada/sql/adapter/calcite/CanonicalLogicSignatureTest.java` — ACCEPT — update model references/formatting only.
- `lib_sql/src/test/java/com/cascada/sql/adapter/calcite/CanonicalObjectFactoryTest.java` — ACCEPT — update model references/formatting only.
- `lib_sql/src/test/java/com/cascada/sql/adapter/calcite/GapMergePropertyTest.java` — ACCEPT — update model references/formatting only.
- `lib_sql/src/test/java/com/cascada/sql/adapter/calcite/GapQueryBuilderDeepTest.java` — ACCEPT — update model references/formatting only.
- `lib_sql/src/test/java/com/cascada/sql/adapter/calcite/GapQueryBuilderTest.java` — ACCEPT — update model references/formatting only.
- `lib_sql/src/test/java/com/cascada/sql/adapter/calcite/HugeComplexSqlCanonicalTest.java` — ACCEPT — update model references/formatting only.
- `lib_sql/src/test/java/com/cascada/sql/adapter/calcite/LogicalToPhysicalSqlTranslatorDeepTest.java` — ACCEPT — update model references/formatting only.
- `lib_sql/src/test/java/com/cascada/sql/adapter/calcite/LogicalToPhysicalSqlTranslatorTest.java` — ACCEPT — update model references/formatting only.
- `lib_sql/src/test/java/com/cascada/sql/adapter/calcite/SqlCorrectnessRegressionTest.java` — ACCEPT — update model references/formatting only.
- `lib_sql/src/test/java/com/cascada/sql/simulation/CacheCorrectnessSimulationTest.java` — ACCEPT — update model references/formatting only.
- `lib_sql/src/test/java/com/cascada/sql/simulation/DirectComputeOracle.java` — ACCEPT — update model references/formatting only.
- `lib_sql/src/test/java/com/cascada/sql/simulation/WarmingConsistencySimulationTest.java` — ACCEPT — update model references/formatting only.

`docs/UNCOMMITTED_COMMIT_PLAN.md` — ACCEPT — describe this isolated publication rather than claiming the unrelated local feature/removal history is included.
