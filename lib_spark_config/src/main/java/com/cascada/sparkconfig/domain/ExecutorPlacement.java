package com.cascada.sparkconfig.domain;

/**
 * The third of the "three knobs" (plan §6.6): where executors may land. Each placement carries the
 * dynamic-allocation budget it implies and the Kubernetes scheduling hint it compiles to, so the
 * customer never writes a {@code nodeSelector}, an affinity block, or a min/max-executors number.
 *
 * <p>{@link #DEDICATED_NODE_POOL} is the reference placement that, with RAM=18 and cores=18,
 * reproduces the golden {@code spark.json} (min 2 / max 3 executors).
 */
public enum ExecutorPlacement {

    /** Pin to the driver's node — smallest blast radius, single executor. */
    SAME_NODE_AS_DRIVER(1, 1, "cascada.io/placement", "same-node"),

    /** Spread across the general pool. */
    SPREAD_ACROSS_NODES(2, 8, "cascada.io/placement", "spread"),

    /** A dedicated pool reserved for this tenant (the golden reference). */
    DEDICATED_NODE_POOL(2, 3, "cascada.io/node-pool", "dedicated"),

    /** Allow preemptible/spot nodes — widest budget, tuned for restarts. */
    SPOT_OK(2, 16, "cascada.io/lifecycle", "spot");

    private final int minimumExecutors;
    private final int maximumExecutors;
    private final String nodeSelectorLabel;
    private final String nodeSelectorValue;

    ExecutorPlacement(int minimumExecutors, int maximumExecutors, String nodeSelectorLabel, String nodeSelectorValue) {
        this.minimumExecutors = minimumExecutors;
        this.maximumExecutors = maximumExecutors;
        this.nodeSelectorLabel = nodeSelectorLabel;
        this.nodeSelectorValue = nodeSelectorValue;
    }

    public int minimumExecutors() {
        return minimumExecutors;
    }

    public int maximumExecutors() {
        return maximumExecutors;
    }

    public String nodeSelectorLabel() {
        return nodeSelectorLabel;
    }

    public String nodeSelectorValue() {
        return nodeSelectorValue;
    }
}
