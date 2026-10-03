package com.cascada.app.bootstrap;

import com.cascada.app.config.EngineSettings;
import com.cascada.app.adapter.out.configuration.EnvironmentSettingsReader;
import com.cascada.app.adapter.out.configuration.SparkConfigurationFileReader;
import com.cascada.app.adapter.out.execution.LazyQueryExecutor;
import com.cascada.app.adapter.in.cli.CascadaCli;

import com.cascada.spark.adapter.out.environment.SystemEnvironmentAdapter;
import com.cascada.spark.adapter.out.spark.SparkDeltaQueryExecutor;
import com.cascada.spark.domain.SparkSessionConfig;
import com.cascada.spark.application.configuration.SparkSessionConfigBuilder;

/** Process entry point. Validates the command and owns cache and executor resources. */
public final class CascadaLauncher {

    private CascadaLauncher() {
    }

    public static void main(String[] args) throws Exception {
        if (!CascadaCli.validateCommand(args)) {
            return;
        }
        EngineSettings settings = new EnvironmentSettingsReader(SystemEnvironmentAdapter.INSTANCE::get).read();

        try (LazyQueryExecutor executor = new LazyQueryExecutor(() -> sparkExecutor(settings));
             CascadaEngineFactory factory = new CascadaEngineFactory(settings, executor)) {

            // TODO(cascada): replace this with a real driving adapter (REST server / JDBC listener) that
            // stays up. Handing the ports to the CLI is a placeholder so the wiring is exercised and the
            // engine is reachable, not a production front end.
            new CascadaCli(
                    factory.executeLogicalQueryUseCase(),
                    factory.measureCacheSizeUseCase(),
                    factory.flushCacheUseCase(),
                    factory.warmCacheUseCase()).run(args);
        }
    }

    /** Builds Spark with deployment properties followed by explicit environment overrides. */
    private static SparkDeltaQueryExecutor sparkExecutor(EngineSettings settings) {
        SparkSessionConfig config = (settings.useLocalSpark()
                ? SparkSessionConfigBuilder.forLocal()
                : SparkSessionConfigBuilder.forKubernetes())
                .appName("CascadaDeltaQueryExecutor")
                .withGroup(new SparkConfigurationFileReader().read(
                        SystemEnvironmentAdapter.INSTANCE.get("SPARK_CONFIG_PATH")))
                // Reading the OS is opt-in and happens only here, via the adapter.
                .withEnvironment(SystemEnvironmentAdapter.INSTANCE)
                .withEnvOverride("spark.executor.memory", "SPARK_EXECUTOR_MEMORY")
                .withEnvOverride("spark.executor.cores", "SPARK_EXECUTOR_CORES")
                .withEnvOverride("spark.kubernetes.namespace", "SPARK_K8S_NAMESPACE")
                .withEnvOverride("spark.kubernetes.container.image", "SPARK_K8S_CONTAINER_IMAGE")
                .build();
        return new SparkDeltaQueryExecutor(config);
    }

}
