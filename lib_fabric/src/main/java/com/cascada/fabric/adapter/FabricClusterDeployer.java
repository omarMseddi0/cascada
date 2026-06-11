package com.cascada.fabric.adapter;

import com.cascada.fabric.ClusterManifestRenderer;
import com.cascada.fabric.ClusterValues;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * The "apply via Fabric" half (plan §6.7, Phase B): renders the manifest templates from
 * {@link ClusterValues} and applies / stops / restarts / deletes the cluster through the Fabric8
 * Kubernetes client — the programmatic equivalent of {@code kubectl apply -f} / {@code scale} /
 * {@code rollout restart} / {@code delete}.
 *
 * <p>No Spring and no hand-configured API-server URL: the injected {@link KubernetesClient} resolves the
 * cluster automatically — in-cluster from the mounted ServiceAccount token + {@code KUBERNETES_SERVICE_*}
 * env vars, or out-of-cluster from {@code ~/.kube/config}. The RBAC we render is what authorizes that
 * ServiceAccount to perform these calls.
 *
 * <p>This is the one place a framework is allowed (it is an adapter, not domain). It is constructor-
 * injected with a client so tests can drive it against the Fabric8 mock API server without a real cluster.
 */
public final class FabricClusterDeployer implements AutoCloseable {

    private final KubernetesClient client;
    private final ClusterManifestRenderer renderer = new ClusterManifestRenderer();

    public FabricClusterDeployer(KubernetesClient client) {
        this.client = client;
    }

    /** Production entry point: auto-discover the cluster (in-cluster SA or local kubeconfig). */
    public static FabricClusterDeployer autoConfigured() {
        return new FabricClusterDeployer(new KubernetesClientBuilder().build());
    }

    /** {@code apply -f}: create-or-replace every rendered manifest. Returns the applied {@code Kind/name}s. */
    public List<String> apply(ClusterValues values) {
        List<HasMetadata> items = parse(values);
        for (HasMetadata item : items) {
            client.resource(item).createOrReplace();
        }
        return items.stream().map(i -> i.getKind() + "/" + i.getMetadata().getName()).toList();
    }

    /** Stop: scale the driver Deployment to zero replicas (ConfigMaps and RBAC are kept). */
    public void stop(ClusterValues values) {
        client.apps().deployments()
                .inNamespace(values.namespace())
                .withName(values.driverWorkloadName())
                .scale(0);
    }

    /** Start (after a stop): scale the driver Deployment back to one replica. */
    public void start(ClusterValues values) {
        client.apps().deployments()
                .inNamespace(values.namespace())
                .withName(values.driverWorkloadName())
                .scale(1);
    }

    /** Restart: a rolling restart of the driver Deployment so it re-reads the spark.json ConfigMap. */
    public void restart(ClusterValues values) {
        client.apps().deployments()
                .inNamespace(values.namespace())
                .withName(values.driverWorkloadName())
                .rolling()
                .restart();
    }

    /** Delete: tear down every rendered manifest. */
    public void delete(ClusterValues values) {
        client.resourceList(parse(values)).delete();
    }

    private List<HasMetadata> parse(ClusterValues values) {
        byte[] yaml = renderer.renderCombined(values).getBytes(StandardCharsets.UTF_8);
        return client.load(new ByteArrayInputStream(yaml)).items();
    }

    @Override
    public void close() {
        client.close();
    }
}
