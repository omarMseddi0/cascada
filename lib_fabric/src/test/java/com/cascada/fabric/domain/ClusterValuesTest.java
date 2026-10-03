package com.cascada.fabric.domain;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterValuesTest {

    @Test
    void defaultsApplyWhenNoSettingsAreProvided() {
        ClusterValues values = ClusterValues.defaults();
        assertThat(values.namespace()).isEqualTo("default");
        assertThat(values.get("serviceAccountName")).isEqualTo("cascada-spark-copy1");
        assertThat(values.get("executorMemory")).isEqualTo("8g");
        assertThat(values.get("executorLimitCores")).isEqualTo("4"); // default 3 cores + 1
    }

    @Test
    void derivesResourceNamesAndDriverHostFromReleaseAndSuffix() {
        ClusterValues values = ClusterValues.fromSettings(Map.of(
                "CASCADA_RELEASE_NAME", "rel",
                "CASCADA_COPY_SUFFIX", "c2",
                "CASCADA_NAMESPACE", "ns"));
        assertThat(values.driverWorkloadName()).isEqualTo("rel-driver-c2");
        assertThat(values.get("driverServiceName")).isEqualTo("rel-spark-driver-c2");
        assertThat(values.get("roleName")).isEqualTo("rel-namespace-role-c2");
        assertThat(values.get("driverHost")).isEqualTo("rel-spark-driver-c2.ns.svc.cluster.local");
        assertThat(values.get("podNamePrefix")).isEqualTo("rel-exec-c2");
    }

    @Test
    void limitCoresTracksExecutorCores() {
        assertThat(ClusterValues.fromSettings(Map.of("CASCADA_EXECUTOR_CORES", "7"))
                .get("executorLimitCores")).isEqualTo("8");
    }

    @Test
    void rejectsMalformedAndOutOfRangeNumericSettings() {
        assertInvalid("CASCADA_EXECUTOR_CORES", "notanumber");
        assertInvalid("CASCADA_EXECUTOR_CORES", "2147483647");
        assertInvalid("CASCADA_REPLICAS", "0");
        assertInvalid("CASCADA_DRIVER_PORT", "65536");
        assertInvalid("CASCADA_BLOCKMGR_PORT", "abc");
        assertInvalid("CASCADA_MIN_EXECUTORS", "-1");
        assertInvalid("CASCADA_MAX_EXECUTORS", "2147483648");
        assertThatThrownBy(() -> ClusterValues.fromSettings(Map.of(
                "CASCADA_MIN_EXECUTORS", "4", "CASCADA_MAX_EXECUTORS", "3")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CASCADA_MIN_EXECUTORS");
    }

    @Test
    void acceptsRepresentableExecutorAndReplicaCounts() {
        ClusterValues values = ClusterValues.fromSettings(Map.of(
                "CASCADA_EXECUTOR_CORES", "2147483646",
                "CASCADA_MAX_EXECUTORS", "2147483647",
                "CASCADA_REPLICAS", "2147483647"));

        assertThat(values.get("executorLimitCores")).isEqualTo("2147483647");
        assertThat(values.get("maxExecutors")).isEqualTo("2147483647");
        assertThat(values.get("replicas")).isEqualTo("2147483647");
    }

    @Test
    void initialExecutorTargetAlwaysFitsTheDynamicAllocationBounds() {
        ClusterValues singleExecutor = ClusterValues.fromSettings(Map.of(
                "CASCADA_MIN_EXECUTORS", "0", "CASCADA_MAX_EXECUTORS", "1"));
        assertThat(singleExecutor.get("initialExecutors")).isEqualTo("1");

        ClusterValues raisedMinimum = ClusterValues.fromSettings(Map.of(
                "CASCADA_MIN_EXECUTORS", "4", "CASCADA_MAX_EXECUTORS", "8"));
        assertThat(raisedMinimum.get("initialExecutors")).isEqualTo("4");
    }

    @Test
    void placeholdersAreReadOnly() {
        ClusterValues values = ClusterValues.defaults();
        assertThatThrownBy(() -> values.placeholders().put("namespace", "other"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private static void assertInvalid(String key, String value) {
        assertThatThrownBy(() -> ClusterValues.fromSettings(Map.of(key, value)))
                .as("invalid setting %s=%s", key, value)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(key);
    }

    @Test
    void blankSettingFallsBackToDefault() {
        ClusterValues values = ClusterValues.fromSettings(Map.of("CASCADA_NAMESPACE", "  "));
        assertThat(values.namespace()).isEqualTo("default");
    }
}
