package com.cascada.app;

import com.cascada.cache.adapter.backend.ValkeyCacheBackendAdapter;
import com.cascada.cache.adapter.serialization.ArrowResultFrameSerializer;
import com.cascada.cache.application.ExecuteCachedQueryUseCase;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.port.CacheBackendPort;
import com.cascada.spark.config.SparkSessionConfig;
import com.cascada.spark.config.SparkSessionConfigBuilder;
import com.cascada.spark.execution.SparkDeltaQueryExecutor;
import com.cascada.sql.translate.RegisteredTable;
import com.cascada.sql.translate.TableCatalog;

import java.util.Map;

/**
 * The production composition root — the "main code that calls the library." It wires the <b>real</b>
 * execution and storage tiers and runs a logical SQL query end-to-end through {@link CascadaEngine}:
 *
 * <ul>
 *   <li><b>Executor:</b> {@link SparkDeltaQueryExecutor} on a Spark session built from
 *       {@link SparkSessionConfigBuilder#forKubernetes()} (master {@code k8s://...}, Delta extensions,
 *       and the cluster {@code spark.json} layered in via {@code withGroup(...)} + env overrides) —
 *       the same Spark + Gluten/Velox + Delta runtime the deployment ships in its image. Local vs
 *       cluster is purely this config; the engine code is identical.</li>
 *   <li><b>Cache hot tier:</b> {@link ValkeyCacheBackendAdapter} (Valkey/Redis) with the Arrow IPC
 *       serializer.</li>
 * </ul>
 *
 * <p>This is wired for the Kubernetes data plane and is meant to run inside the Spark image / cluster
 * (where the Spark + Delta runtime and the Valkey endpoint exist). It reads its endpoints from the
 * environment, exactly like {@code spark_manager.py} ({@code SPARK_MASTER}, {@code REDIS_URL}, etc.).
 */
public final class CascadaApplication {

    private CascadaApplication() {
    }

    public static void main(String[] args) {
        // The real Spark + Delta session config: Kubernetes master + Delta extensions + cluster spark.json
        // (layered in by the operator) + environment overrides (SPARK_MASTER, SPARK_EXECUTOR_MEMORY, ...).
        SparkSessionConfig sparkConfig = SparkSessionConfigBuilder.forKubernetes()
                .appName("CascadaDeltaQueryExecutor")
                .withEnvOverride("spark.executor.memory", "SPARK_EXECUTOR_MEMORY")
                .withEnvOverride("spark.executor.cores", "SPARK_EXECUTOR_CORES")
                .withEnvOverride("spark.kubernetes.namespace", "SPARK_K8S_NAMESPACE")
                .withEnvOverride("spark.kubernetes.container.image", "SPARK_K8S_CONTAINER_IMAGE")
                .build();

        String redisUrl = environmentOrDefault("REDIS_URL", "redis://localhost:6379");
        CacheBackendPort cacheBackend =
                new ValkeyCacheBackendAdapter(redisUrl, new ArrowResultFrameSerializer());

        // Logical→physical table registry — in production populated from the deployment's `tables` config.
        TableCatalog catalog = new TableCatalog().register(RegisteredTable.of(
                "main_data_table",
                environmentOrDefault("MAIN_TABLE_DELTA_PATH", "hdfs:///user/cascada/main_data_table"),
                Map.of("ts", "ts", "appName", "appName", "bytesFromClient", "bytesFromClient"), "ts"));

        try (SparkDeltaQueryExecutor executor = new SparkDeltaQueryExecutor(sparkConfig)) {
            CascadaEngine engine = CascadaEngine.builder()
                    .executor(executor)
                    .cacheBackend(cacheBackend)
                    .tableCatalog(catalog)
                    .build();

            String logicalSql = "SELECT appName, SUM(bytesFromClient) AS total_bytes FROM main_data_table "
                    + "WHERE ts >= 0 AND ts <= 9999999999 GROUP BY appName ORDER BY appName";

            ExecuteCachedQueryUseCase.Result result = engine.query(logicalSql);
            ResultFrame frame = result.frame();

            System.out.println("=== Cascada end-to-end query (Spark + Delta on Kubernetes) ===");
            System.out.println("served through cache: " + result.servedThroughCache());
            System.out.println("columns: " + frame.columnNames());
            for (Map<String, Object> row : frame.rows()) {
                System.out.println("  " + row);
            }
        }
    }

    private static String environmentOrDefault(String name, String fallback) {
        String value = System.getenv(name);
        return (value != null && !value.isEmpty()) ? value : fallback;
    }
}
