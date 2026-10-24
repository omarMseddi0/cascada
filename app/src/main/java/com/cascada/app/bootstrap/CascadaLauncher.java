package com.cascada.app.bootstrap;

import com.cascada.app.adapter.out.configuration.DeploymentSettingsReader;
import com.cascada.app.model.config.EngineSettings;
import com.cascada.spark.adapter.out.environment.SystemEnvironmentAdapter;
import com.cascada.spark.adapter.out.spark.SparkDeltaQueryExecutor;
import com.cascada.spark.application.configuration.SparkSessionConfigBuilder;
import com.cascada.spark.domain.SparkSessionConfig;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Starts the API process; CascadaRuntime owns transport and engine resources. */
public final class CascadaLauncher {

    private static final Logger LOGGER = Logger.getLogger(CascadaLauncher.class.getName());

    private CascadaLauncher() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 0) throw new IllegalArgumentException("Cascada is an API server; configure it through CASCADA_CONFIG_PATH");
        var environment = SystemEnvironmentAdapter.INSTANCE;
        var deployment = new DeploymentSettingsReader().read(environment::get);
        EngineSettings settings = deployment.engine();
        var applicationSettings = deployment.application();
        SparkSessionConfig sparkConfiguration = sparkConfiguration(settings, deployment.sparkProperties());
        try (CascadaRuntime runtime = new CascadaRuntime(settings,
                () -> new SparkDeltaQueryExecutor(sparkConfiguration), applicationSettings, null)) {
            Thread shutdown = new Thread(() -> {
                try { runtime.close(); }
                catch (Exception failed) { LOGGER.log(Level.SEVERE, "API shutdown failed", failed); }
            }, "cascada-shutdown");
            Runtime.getRuntime().addShutdownHook(shutdown);
            try {
                runtime.start();
                LOGGER.info(() -> "Cascada API listening: HTTP=" + runtime.httpPort() + " gRPC=" + runtime.grpcPort());
                runtime.awaitTermination();
            } finally {
                try { Runtime.getRuntime().removeShutdownHook(shutdown); }
                catch (IllegalStateException shuttingDown) { /* hook owns resource cleanup */ }
            }
        }
    }


    /** Builds Spark with deployment properties followed by explicit environment overrides. */
    private static SparkSessionConfig sparkConfiguration(EngineSettings settings, java.util.Map<String, String> properties) {
        SparkSessionConfig config = (settings.useLocalSpark()
                ? SparkSessionConfigBuilder.forLocal()
                : SparkSessionConfigBuilder.forKubernetes())
                .appName("CascadaDeltaQueryExecutor")
                .withGroup(properties)
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
