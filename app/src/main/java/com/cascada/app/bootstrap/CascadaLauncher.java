package com.cascada.app.bootstrap;

import com.cascada.app.adapter.out.configuration.ApplicationSettingsReader;
import com.cascada.app.adapter.out.configuration.EnvironmentSettingsReader;
import com.cascada.app.adapter.out.configuration.SparkConfigurationFileReader;
import com.cascada.app.config.EngineSettings;
import com.cascada.spark.adapter.out.environment.SystemEnvironmentAdapter;
import com.cascada.spark.adapter.out.spark.SparkDeltaQueryExecutor;
import com.cascada.spark.application.configuration.SparkSessionConfigBuilder;
import com.cascada.spark.domain.SparkSessionConfig;

/** Starts the API process; CascadaRuntime owns transport and engine resources. */
public final class CascadaLauncher {

    private CascadaLauncher() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 0) throw new IllegalArgumentException("Cascada is an API server; configure it through environment variables");
        var environment = SystemEnvironmentAdapter.INSTANCE;
        EngineSettings settings = new EnvironmentSettingsReader(environment::get).read();
        var applicationSettings = new ApplicationSettingsReader().read(environment::get);
        SparkSessionConfig sparkConfiguration = sparkConfiguration(settings);
        try (CascadaRuntime runtime = new CascadaRuntime(settings,
                () -> new SparkDeltaQueryExecutor(sparkConfiguration), applicationSettings, null)) {
            Thread shutdown = new Thread(() -> {
                try { runtime.close(); }
                catch (Exception failed) { java.util.logging.Logger.getLogger(CascadaLauncher.class.getName())
                        .log(java.util.logging.Level.SEVERE, "API shutdown failed", failed); }
            }, "cascada-shutdown");
            Runtime.getRuntime().addShutdownHook(shutdown);
            try {
                runtime.start();
                System.out.println("Cascada API listening: HTTP=" + runtime.httpPort() + " gRPC=" + runtime.grpcPort());
                runtime.awaitTermination();
            } finally {
                try { Runtime.getRuntime().removeShutdownHook(shutdown); }
                catch (IllegalStateException shuttingDown) { /* hook owns resource cleanup */ }
            }
        }
    }


    /** Builds Spark with deployment properties followed by explicit environment overrides. */
    private static SparkSessionConfig sparkConfiguration(EngineSettings settings) {
        SparkSessionConfig config = (settings.useLocalSpark()
                ? SparkSessionConfigBuilder.forLocal()
                : SparkSessionConfigBuilder.forKubernetes())
                .appName("CascadaDeltaQueryExecutor")
                .withGroup(new SparkConfigurationFileReader().read(
                        SystemEnvironmentAdapter.INSTANCE.get("SPARK_CONFIG_PATH")))
                // Reading the OS is opt-in and happens only here, via the adapter.
                .withEnvironment(SystemEnvironmentAdapter.INSTANCE)
                .withEnvOverride("spark.driver.maxResultSize", "SPARK_DRIVER_MAX_RESULT_SIZE")
                .withEnvOverride("spark.driver.host", "SPARK_DRIVER_HOST")
                .withEnvOverride("spark.kubernetes.driver.pod.name", "POD_NAME")
                .withEnvOverride("spark.executor.memory", "SPARK_EXECUTOR_MEMORY")
                .withEnvOverride("spark.executor.cores", "SPARK_EXECUTOR_CORES")
                .withEnvOverride("spark.kubernetes.namespace", "SPARK_K8S_NAMESPACE")
                .withEnvOverride("spark.kubernetes.container.image", "SPARK_K8S_CONTAINER_IMAGE")
                .build();
        return config;
    }

}
