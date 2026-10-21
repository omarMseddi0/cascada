package com.cascada.app.bootstrap;

import com.cascada.app.adapter.in.api.CacheGrpcService;
import com.cascada.app.adapter.in.api.CancellationInterceptor;
import com.cascada.app.adapter.in.api.ClusterGrpcService;
import com.cascada.app.adapter.in.api.QueryGrpcService;
import com.cascada.app.adapter.in.api.QueryHttpServer;
import com.cascada.app.adapter.in.scheduling.CacheWarmingScheduler;
import com.cascada.app.adapter.out.execution.LazyQueryExecutor;
import com.cascada.app.adapter.out.kubernetes.LazyClusterOrchestrator;
import com.cascada.app.model.config.ApplicationSettings;
import com.cascada.app.config.CascadaEngineFactory;
import com.cascada.app.model.config.EngineSettings;
import com.cascada.app.model.config.ServerSettings;
import com.cascada.app.model.config.WarmingSettings;
import com.cascada.cache.application.port.out.QueryExecutorPort;
import com.cascada.fabric.adapter.out.kubernetes.FabricClusterDeployer;
import com.cascada.fabric.adapter.out.manifest.ClusterManifestRenderer;
import com.cascada.fabric.application.port.in.ManageClusterLifecycleUseCase;
import com.cascada.fabric.application.port.in.PrepareClusterSettingsUseCase;
import com.cascada.fabric.application.service.ClusterLifecycleService;
import com.cascada.fabric.application.service.ClusterSettingsService;
import com.cascada.sparkconfig.application.port.in.DeriveSparkConfigurationUseCase;
import com.cascada.sparkconfig.application.service.SparkConfigurationDeriver;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** Owns server and engine lifetimes, including startup failures and concurrent shutdown. */
final class CascadaRuntime implements AutoCloseable {
    private final List<AutoCloseable> resources = new ArrayList<>();
    private final AtomicBoolean closing = new AtomicBoolean();
    private final CountDownLatch closed = new CountDownLatch(1);
    private final QueryHttpServer http;
    private final Server grpc;
    private final CacheWarmingScheduler warming;

    CascadaRuntime(EngineSettings settings, Supplier<? extends QueryExecutorPort> executorFactory,
                   int httpPort, int grpcPort) throws Exception {
        this(settings, executorFactory, new ApplicationSettings(
                new com.cascada.app.model.config.ServerSettings(httpPort, grpcPort, java.time.Duration.ofSeconds(30)),
                com.cascada.app.model.config.WarmingSettings.defaults(), java.util.List.of()), null);
    }

    CascadaRuntime(EngineSettings settings, Supplier<? extends QueryExecutorPort> executorFactory,
                   ApplicationSettings applicationSettings, ManageClusterLifecycleUseCase suppliedClusters) throws Exception {
        try {
            LazyQueryExecutor executor = new LazyQueryExecutor(executorFactory);
            resources.add(executor);
            CascadaEngineFactory factory = new CascadaEngineFactory(settings, executor, applicationSettings);
            resources.add(factory);
            QueryGrpcService service = new QueryGrpcService(factory.executeLogicalQueryUseCase(), applicationSettings.server());
            resources.add(service);
            ManageClusterLifecycleUseCase clusters = suppliedClusters;
            if (clusters == null) {
                LazyClusterOrchestrator orchestrator = new LazyClusterOrchestrator(FabricClusterDeployer::autoConfigured);
                resources.add(orchestrator);
                clusters = new ClusterLifecycleService(new ClusterManifestRenderer(), orchestrator);
            }
            http = new QueryHttpServer(applicationSettings.server(), factory.executeLogicalQueryUseCase());
            http.bindCacheAdministration(factory.measureCacheSizeUseCase(), factory.flushCacheUseCase(), factory.warmCacheUseCase());
            DeriveSparkConfigurationUseCase sparkConfiguration = new SparkConfigurationDeriver();
            PrepareClusterSettingsUseCase preparation = new ClusterSettingsService(sparkConfiguration::deriveMemorySplit);
            http.bindClusterLifecycle(clusters, preparation);
            resources.add(http);
            warming = new CacheWarmingScheduler(factory.warmCacheUseCase(), applicationSettings.warming(), java.time.Clock.systemUTC());
            resources.add(warming);
            CancellationInterceptor administration = new CancellationInterceptor(applicationSettings.server());
            resources.add(administration);
            grpc = ServerBuilder.forPort(applicationSettings.server().grpcPort()).addService(service)
                    .addService(ServerInterceptors.intercept(new CacheGrpcService(factory.measureCacheSizeUseCase(),
                            factory.flushCacheUseCase(), factory.warmCacheUseCase()), administration))
                    .addService(ServerInterceptors.intercept(new ClusterGrpcService(clusters, preparation), administration))
                    .build();
            resources.add(() -> {
                grpc.shutdownNow();
                grpc.awaitTermination(applicationSettings.server().shutdownTimeout().toMillis(), TimeUnit.MILLISECONDS);
            });
        } catch (Exception failure) {
            try { closeResources(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    void start() throws Exception { grpc.start(); http.start(); warming.start(); }
    int httpPort() { return http.port(); }
    int grpcPort() { return grpc.getPort(); }
    void awaitTermination() throws InterruptedException { grpc.awaitTermination(); }

    @Override public void close() throws Exception {
        if (!closing.compareAndSet(false, true)) { closed.await(); return; }
        try { closeResources(); } finally { closed.countDown(); }
    }
    private void closeResources() throws Exception {
        Exception failure = null;
        for (int i = resources.size() - 1; i >= 0; i--) {
            try { resources.get(i).close(); }
            catch (Exception exception) {
                if (failure == null) failure = exception; else failure.addSuppressed(exception);
            }
        }
        if (failure != null) throw failure;
    }
}
