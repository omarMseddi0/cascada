package com.cascada.app.adapter.out.kubernetes;

import com.cascada.fabric.application.port.out.ClusterOrchestratorPort;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/** Query-only startup does not open a Kubernetes control-plane client. */
public final class LazyClusterOrchestrator implements ClusterOrchestratorPort, AutoCloseable {
    private final Supplier<? extends ClusterOrchestratorPort> factory;
    private ClusterOrchestratorPort delegate;

    public LazyClusterOrchestrator(Supplier<? extends ClusterOrchestratorPort> factory) {
        this.factory = Objects.requireNonNull(factory, "factory");
    }
    private boolean closed;
    private synchronized ClusterOrchestratorPort client() {
        if (closed) throw new IllegalStateException("Cluster orchestrator is closed");
        if (delegate == null) delegate = Objects.requireNonNull(factory.get(), "cluster orchestrator");
        return delegate;
    }
    @Override public List<String> apply(List<String> manifests) { return client().apply(manifests); }
    @Override public void delete(List<String> manifests) { client().delete(manifests); }
    @Override public void scaleWorkload(String namespace, String name, int replicas) { client().scaleWorkload(namespace, name, replicas); }
    @Override public void restartWorkload(String namespace, String name) { client().restartWorkload(namespace, name); }
    @Override public synchronized void close() throws Exception {
        if (closed) return;
        closed = true;
        if (delegate instanceof AutoCloseable closeable) closeable.close();
    }
}
