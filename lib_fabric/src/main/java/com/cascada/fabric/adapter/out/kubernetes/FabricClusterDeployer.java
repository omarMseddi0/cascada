package com.cascada.fabric.adapter.out.kubernetes;

import com.cascada.fabric.application.port.out.ClusterOrchestratorPort;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * The Kubernetes {@link ClusterOrchestratorPort}: applies, scales, restarts and deletes resources through
 * the Fabric8 client — the programmatic equivalent of {@code kubectl apply -f} / {@code scale} /
 * {@code rollout restart} / {@code delete}.
 *
 * <p><b>This class is now only mechanics.</b> It receives already-rendered YAML documents plus resource
 * coordinates and performs I/O. It no longer renders manifests, and it no longer decides <em>what</em> a
 * stop or a restart means — that moved to {@code application.service.ClusterLifecycleService}. The split
 * is why the lifecycle rules ("stop is a scale-to-zero, not a delete") are now testable with a plain
 * fake, while only this class needs a mock API server.
 *
 * <p>No Spring and no hand-configured API-server URL: the injected {@link KubernetesClient} resolves the
 * cluster itself — in-cluster from the mounted ServiceAccount token and {@code KUBERNETES_SERVICE_*}
 * variables, or out-of-cluster from {@code ~/.kube/config}. The RBAC these manifests create is what
 * authorises that same ServiceAccount to make these calls.
 *
 * <p>Being an adapter is what earns it the right to import Fabric8 at all. It is constructor-injected
 * with a client so tests drive it against the Fabric8 mock API server instead of a real cluster.
 */
public final class FabricClusterDeployer implements ClusterOrchestratorPort, AutoCloseable {

    private final KubernetesClient client;

    public FabricClusterDeployer(KubernetesClient client) {
        this.client = client;
    }

    /** Production entry point: auto-discover the cluster (in-cluster SA token, or local kubeconfig). */
    public static FabricClusterDeployer autoConfigured() {
        return new FabricClusterDeployer(new KubernetesClientBuilder().build());
    }

    @Override
    public List<String> apply(List<String> manifestDocuments) {
        List<HasMetadata> items = parse(manifestDocuments);
        for (HasMetadata item : items) {
            client.resource(item).createOrReplace();
        }
        return items.stream().map(i -> i.getKind() + "/" + i.getMetadata().getName()).toList();
    }

    @Override
    public void delete(List<String> manifestDocuments) {
        client.resourceList(parse(manifestDocuments)).delete();
    }

    @Override
    public void scaleWorkload(String namespace, String workloadName, int replicas) {
        client.apps().deployments().inNamespace(namespace).withName(workloadName).scale(replicas);
    }

    @Override
    public void restartWorkload(String namespace, String workloadName) {
        client.apps().deployments().inNamespace(namespace).withName(workloadName).rolling().restart();
    }

    /**
     * Parse rendered documents into typed Kubernetes resources. Each document is loaded separately rather
     * than as one concatenated stream so that a single malformed manifest fails on its own, instead of
     * making the whole batch fail opaquely.
     */
    private List<HasMetadata> parse(List<String> manifestDocuments) {
        List<HasMetadata> items = new ArrayList<>();
        for (String document : manifestDocuments) {
            items.addAll(client.load(new ByteArrayInputStream(document.getBytes(StandardCharsets.UTF_8))).items());
        }
        return items;
    }

    @Override
    public void close() {
        client.close();
    }
}
