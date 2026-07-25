package com.cascada.app.bootstrap;

import com.cascada.spark.adapter.out.environment.SystemEnvironmentAdapter;
import com.cascada.spark.adapter.out.spark.SparkDeltaQueryExecutor;
import com.cascada.spark.domain.SparkSessionConfig;
import com.cascada.spark.domain.SparkSessionConfigBuilder;

/**
 * The process entry point: read the environment, build the execution tier, hand both to
 * {@link CascadaEngineFactory}, and start a driving adapter.
 *
 * <p>It does exactly three things and nothing else — no query text, no printing of results, no business
 * decisions. The previous entry point in this module hard-coded a demo SQL string and printed rows to
 * standard output, which meant the only way to run Cascada was to run that one query; there was no seam
 * for a real caller to attach to. Everything a caller might actually want now lives behind an inbound
 * port, and this class merely opens the door.
 *
 * <p>Local versus cluster is decided in one place here — {@code CASCADA_RUN_MODE} picks the Spark master —
 * and nothing downstream is aware of the difference.
 *
 * <h2>Not yet implemented</h2>
 * <ul>
 *   <li>TODO(cascada): serve the inbound ports over the network. A REST adapter under
 *       {@code adapter/in/rest} (one controller per use case, per the reference architecture) and/or a
 *       JDBC/Arrow Flight SQL endpoint so BI tools can connect. Until one exists, Cascada is a library
 *       with a CLI, not a service.</li>
 *   <li>TODO(cascada): a scheduler adapter under {@code adapter/in/scheduler} that periodically calls
 *       {@code WarmCacheUseCase.warmCycle(...)}. Warming is fully implemented and tested but nothing ever
 *       triggers it, so in practice the cache only ever fills on demand.</li>
 *   <li>TODO(cascada): graceful shutdown. The Spark session and the Valkey connection are closed on the
 *       happy path below, but there is no shutdown hook, so a SIGTERM (what Kubernetes sends) leaks both.</li>
 *   <li>TODO(cascada): authentication and tenant resolution at the inbound edge. Every cache key is
 *       already tenant-scoped by construction, so this is the missing half of multi-tenancy.</li>
 *   <li>TODO(cascada): structured logging and metrics. There is no logging framework wired at all, which
 *       is survivable for a library and not for a service.</li>
 * </ul>
 */
public final class CascadaLauncher {

    private CascadaLauncher() {
    }

    public static void main(String[] args) {
        EngineSettings settings = new EnvironmentSettingsReader(SystemEnvironmentAdapter.INSTANCE).read();

        try (SparkDeltaQueryExecutor executor = sparkExecutor(settings)) {
            CascadaEngineFactory factory = new CascadaEngineFactory(settings, executor);

            // TODO(cascada): replace this with a real driving adapter (REST server / JDBC listener) that
            // stays up. Handing the ports to the CLI is a placeholder so the wiring is exercised and the
            // engine is reachable, not a production front end.
            new com.cascada.app.adapter.in.cli.CascadaCli(
                    factory.executeLogicalQueryUseCase(),
                    factory.measureCacheSizeUseCase(),
                    factory.flushCacheUseCase(),
                    factory.warmCacheUseCase()).run(args);
        }
    }

    /**
     * Build the Spark/Delta execution adapter. The only difference between a laptop and the production
     * data plane is the master string chosen here; the Delta extensions, the executor code, and the cache
     * are identical.
     */
    private static SparkDeltaQueryExecutor sparkExecutor(EngineSettings settings) {
        SparkSessionConfig config = (settings.useLocalSpark()
                ? SparkSessionConfigBuilder.forLocal()
                : SparkSessionConfigBuilder.forKubernetes())
                .appName("CascadaDeltaQueryExecutor")
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
