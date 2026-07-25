package com.cascada.fabric.application.port.out;

import java.util.List;

/**
 * <b>Secondary (driven) port</b> for whatever actually talks to the cluster control plane.
 *
 * <p>The port speaks in <em>rendered manifest documents</em> ({@code String}s of YAML) and resource
 * coordinates ({@code namespace}, {@code workloadName}) — never in Fabric8 or Kubernetes types. That
 * restriction is the whole value: the application layer can express "apply this cluster, then scale the
 * driver to zero" without importing a Kubernetes client, and swapping Fabric8 for the official Java
 * client, for a shell-out to {@code kubectl}, or for a test double is a new adapter with no change to
 * the lifecycle logic.
 *
 * <p>Implemented by {@code adapter.out.kubernetes.FabricClusterDeployer}.
 */
public interface ClusterOrchestratorPort {

    /**
     * Create-or-replace every manifest ({@code kubectl apply -f}).
     *
     * @return the applied resources as {@code Kind/name} strings, for the operator log
     */
    List<String> apply(List<String> manifestDocuments);

    /** Delete every manifest ({@code kubectl delete -f}). */
    void delete(List<String> manifestDocuments);

    /** Scale a workload to {@code replicas} — 0 to stop, 1 to start. */
    void scaleWorkload(String namespace, String workloadName, int replicas);

    /** Rolling-restart a workload, so it re-reads mounted ConfigMaps. */
    void restartWorkload(String namespace, String workloadName);
}
