package com.cascada.fabric.application.port.in;

import com.cascada.fabric.domain.ClusterValues;

import java.util.List;

/**
 * <b>Primary (driving) port</b> — the five things an operator can do to a Cascada cluster.
 *
 * <p>This is the seam a CLI, a REST admin endpoint, or a Kubernetes operator reconcile loop drives.
 * Each method takes the {@link ClusterValues} that identify and size the cluster, so the use case is
 * stateless and one instance can manage many clusters.
 *
 * <p>{@link #stop} and {@link #start} deliberately do <em>not</em> delete anything: they scale the
 * driver workload to zero and back, leaving RBAC and ConfigMaps in place. That makes stop/start cheap
 * and reversible, and keeps {@link #delete} as the single destructive operation.
 */
public interface ManageClusterLifecycleUseCase {

    /** Render and apply every manifest; returns the applied {@code Kind/name}s. */
    List<String> deploy(ClusterValues values);

    /** Scale the driver to zero replicas. ConfigMaps and RBAC are retained. */
    void stop(ClusterValues values);

    /** Scale the driver back to one replica after a {@link #stop}. */
    void start(ClusterValues values);

    /** Rolling-restart the driver so it re-reads its Spark configuration ConfigMap. */
    void restart(ClusterValues values);

    /** Tear down every rendered manifest. The only destructive operation here. */
    void delete(ClusterValues values);
}
