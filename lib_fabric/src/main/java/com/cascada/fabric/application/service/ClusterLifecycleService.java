package com.cascada.fabric.application.service;

import com.cascada.fabric.application.port.in.ManageClusterLifecycleUseCase;
import com.cascada.fabric.application.port.out.ClusterOrchestratorPort;
import com.cascada.fabric.domain.ClusterManifestRenderer;
import com.cascada.fabric.domain.ClusterValues;

import java.util.List;
import java.util.Objects;

/**
 * The cluster lifecycle use case: turn {@link ClusterValues} into manifests with the domain renderer,
 * then hand those manifests to whatever can talk to the control plane.
 *
 * <p><b>Why this class exists at all.</b> Before it, {@code FabricClusterDeployer} did both jobs — it
 * rendered manifests <em>and</em> issued Kubernetes calls — so this module had no application layer,
 * and the only way to test "deploy applies the manifests in order" was to stand up a mock API server.
 * Splitting them means:
 * <ul>
 *   <li>the <b>orchestration decisions</b> (stop is a scale-to-zero, not a delete; restart is a rolling
 *       restart so ConfigMaps are re-read) live here, in framework-free code that a plain fake can
 *       verify;</li>
 *   <li>the <b>Kubernetes mechanics</b> live in the adapter behind {@link ClusterOrchestratorPort}.</li>
 * </ul>
 *
 * <p>Note what this class does not import: no Fabric8, no Kubernetes model types, no {@code System}
 * calls. That is the test for whether a class truly belongs in the application layer.
 */
public final class ClusterLifecycleService implements ManageClusterLifecycleUseCase {

    private final ClusterManifestRenderer renderer;
    private final ClusterOrchestratorPort orchestrator;

    public ClusterLifecycleService(ClusterOrchestratorPort orchestrator) {
        this(new ClusterManifestRenderer(), orchestrator);
    }

    public ClusterLifecycleService(ClusterManifestRenderer renderer, ClusterOrchestratorPort orchestrator) {
        this.renderer = Objects.requireNonNull(renderer, "renderer");
        this.orchestrator = Objects.requireNonNull(orchestrator, "orchestrator");
    }

    @Override
    public List<String> deploy(ClusterValues values) {
        return orchestrator.apply(renderer.render(values));
    }

    @Override
    public void stop(ClusterValues values) {
        orchestrator.scaleWorkload(values.namespace(), values.driverWorkloadName(), 0);
    }

    @Override
    public void start(ClusterValues values) {
        orchestrator.scaleWorkload(values.namespace(), values.driverWorkloadName(), 1);
    }

    @Override
    public void restart(ClusterValues values) {
        orchestrator.restartWorkload(values.namespace(), values.driverWorkloadName());
    }

    @Override
    public void delete(ClusterValues values) {
        orchestrator.delete(renderer.render(values));
    }
}
