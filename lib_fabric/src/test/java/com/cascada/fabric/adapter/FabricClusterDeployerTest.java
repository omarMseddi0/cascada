package com.cascada.fabric.adapter;

import com.cascada.fabric.ClusterValues;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives {@link FabricClusterDeployer} against the Fabric8 <em>mock</em> API server (CRUD mode), so the
 * apply/stop/restart/delete path is exercised end-to-end without a real cluster — the same proof the
 * production client gives, minus the network.
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

    @Test
    void applyCreatesEveryRenderedManifest() {
        FabricClusterDeployer deployer = new FabricClusterDeployer(client);

        List<String> applied = deployer.apply(values());

        assertThat(applied).hasSize(11);
        assertThat(applied).contains("ServiceAccount/rel-spark-c1", "Deployment/rel-driver-c1");
        assertThat(client.serviceAccounts().inNamespace("default").withName("rel-spark-c1").get())
                .isNotNull();
        assertThat(client.apps().deployments().inNamespace("default").withName("rel-driver-c1").get())
                .isNotNull();
        assertThat(client.configMaps().inNamespace("default").list().getItems()).hasSize(5);
    }

    @Test
    void stopScalesTheDriverToZero() {
        FabricClusterDeployer deployer = new FabricClusterDeployer(client);
        deployer.apply(values());

        deployer.stop(values());

        Integer replicas = client.apps().deployments().inNamespace("default")
                .withName("rel-driver-c1").get().getSpec().getReplicas();
        assertThat(replicas).isZero();
    }

    @Test
    void deleteRemovesTheManifests() {
        FabricClusterDeployer deployer = new FabricClusterDeployer(client);
        deployer.apply(values());

        deployer.delete(values());

        assertThat(client.apps().deployments().inNamespace("default").withName("rel-driver-c1").get())
                .isNull();
        assertThat(client.serviceAccounts().inNamespace("default").withName("rel-spark-c1").get())
                .isNull();
    }
}
