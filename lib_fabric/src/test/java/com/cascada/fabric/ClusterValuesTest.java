package com.cascada.fabric;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterValuesTest {

    @Test
    void defaultsApplyWhenNoEnvironmentIsSet() {
        ClusterValues v = ClusterValues.fromEnvironment(key -> null);
        assertThat(v.namespace()).isEqualTo("default");
        assertThat(v.get("serviceAccountName")).isEqualTo("cascada-spark-copy1");
        assertThat(v.get("executorMemory")).isEqualTo("8g");
        assertThat(v.get("executorLimitCores")).isEqualTo("4"); // default 3 cores + 1
    }

    @Test
    void derivesResourceNamesAndDriverHostFromReleaseAndSuffix() {
        ClusterValues v = ClusterValues.fromEnvironment(Map.of(
                "CASCADA_RELEASE_NAME", "rel",
                "CASCADA_COPY_SUFFIX", "c2",
                "CASCADA_NAMESPACE", "ns")::get);
        assertThat(v.driverWorkloadName()).isEqualTo("rel-driver-c2");
        assertThat(v.get("driverServiceName")).isEqualTo("rel-spark-driver-c2");
        assertThat(v.get("roleName")).isEqualTo("rel-namespace-role-c2");
        assertThat(v.get("driverHost")).isEqualTo("rel-spark-driver-c2.ns.svc.cluster.local");
        assertThat(v.get("podNamePrefix")).isEqualTo("rel-exec-c2");
    }

    @Test
    void limitCoresTracksExecutorCoresAndFallsBackOnGarbage() {
        assertThat(ClusterValues.fromEnvironment(Map.of("CASCADA_EXECUTOR_CORES", "7")::get)
                .get("executorLimitCores")).isEqualTo("8");
        assertThat(ClusterValues.fromEnvironment(Map.of("CASCADA_EXECUTOR_CORES", "notanumber")::get)
                .get("executorLimitCores")).isEqualTo("4");
    }

    @Test
    void blankEnvironmentValueFallsBackToDefault() {
        ClusterValues v = ClusterValues.fromEnvironment(Map.of("CASCADA_NAMESPACE", "  ")::get);
        assertThat(v.namespace()).isEqualTo("default");
    }
}
