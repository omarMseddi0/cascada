package com.cascada.fabric.adapter.out.kubernetes;

import com.cascada.fabric.application.port.in.ManageClusterLifecycleUseCase;
import com.cascada.fabric.application.service.ClusterLifecycleService;
import com.cascada.fabric.domain.ClusterValues;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives the real lifecycle stack — {@link ClusterLifecycleService} over the
 * {@link FabricClusterDeployer} adapter — against the Fabric8 <em>mock</em> API server (CRUD mode), so
 * apply/stop/restart/delete is exercised end-to-end without a real cluster.
 *
 * <p>Note the shape: the test talks to the {@link ManageClusterLifecycleUseCase} port, exactly as a CLI
 * or an admin endpoint would, and only the adapter under test is concrete. That is the hexagon's
 * testability claim made literal — "the application should be equally controllable by users, other
 * applications, or automated tests" — and it is why this file needs no Kubernetes-specific setup beyond
 * the injected client.
 */
@EnableKubernetesMockClient(crud = true)
class FabricClusterDeployerTest {

    static KubernetesClient client;

    private ClusterValues values() {
        return ClusterValues.fromEnvironment(Map.of(
                "CASCADA_RELEASE_NAME", "rel",
                "CASCADA_COPY_SUFFIX", "c1",
                "CASCADA_NAMESPACE", "default")::get);
    }

    private ManageClusterLifecycleUseCase lifecycle() {
        return new ClusterLifecycleService(new FabricClusterDeployer(client));
    }

    @Test
    void deployCreatesEveryRenderedManifest() {
        List<String> applied = lifecycle().deploy(values());

        assertThat(applied).hasSize(11);
        assertThat(applied).contains("ServiceAccount/rel-spark-c1", "Deployment/rel-driver-c1");
        assertThat(client.serviceAccounts().inNamespace("default").withName("rel-spark-c1").get())
                .isNotNull();
        assertThat(client.apps().deployments().inNamespace("default").withName("rel-driver-c1").get())
                .isNotNull();
        assertThat(client.configMaps().inNamespace("default").list().getItems()).hasSize(5);
    }

    @Test
    void stopScalesTheDriverToZeroWithoutDeletingAnything() {
        ManageClusterLifecycleUseCase lifecycle = lifecycle();
        lifecycle.deploy(values());

        lifecycle.stop(values());

        Integer replicas = client.apps().deployments().inNamespace("default")
                .withName("rel-driver-c1").get().getSpec().getReplicas();
        assertThat(replicas).isZero();
        // A stop must be reversible: the ConfigMaps and the ServiceAccount survive it.
        assertThat(client.configMaps().inNamespace("default").list().getItems()).hasSize(5);
        assertThat(client.serviceAccounts().inNamespace("default").withName("rel-spark-c1").get())
                .isNotNull();
    }

    @Test
    void startScalesTheDriverBackToOne() {
        ManageClusterLifecycleUseCase lifecycle = lifecycle();
        lifecycle.deploy(values());
        lifecycle.stop(values());

        lifecycle.start(values());

        assertThat(client.apps().deployments().inNamespace("default")
                .withName("rel-driver-c1").get().getSpec().getReplicas()).isOne();
    }

    @Test
    void deleteRemovesTheManifests() {
        ManageClusterLifecycleUseCase lifecycle = lifecycle();
        lifecycle.deploy(values());

        lifecycle.delete(values());

        assertThat(client.apps().deployments().inNamespace("default").withName("rel-driver-c1").get())
                .isNull();
        assertThat(client.serviceAccounts().inNamespace("default").withName("rel-spark-c1").get())
                .isNull();
    }
}
