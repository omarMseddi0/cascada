package com.cascada.fabric.application.service;

import com.cascada.fabric.domain.ClusterValues;
import com.cascada.sparkconfig.application.service.SparkConfigurationDeriver;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterSettingsReaderTest {

    @Test
    void readsSettingsThroughThePortAndBuildsDomainValues() {
        Map<String, String> environment = Map.of(
                "CASCADA_RELEASE_NAME", "release",
                "CASCADA_NAMESPACE", "analytics",
                "CASCADA_EXECUTOR_CORES", "6");

        ClusterValues values = new ClusterSettingsReader(environment::get, new ClusterSettingsService(new SparkConfigurationDeriver()::deriveMemorySplit)).read();

        assertThat(values.namespace()).isEqualTo("analytics");
        assertThat(values.driverWorkloadName()).isEqualTo("release-driver-copy1");
        assertThat(values.get("executorLimitCores")).isEqualTo("7");
        assertThat(values.get("executorMemory")).isEqualTo("6g");
    }

    @Test
    void defaultsWhenTheEnvironmentPortHasNoValues() {
        ClusterValues values = new ClusterSettingsReader(name -> null, new ClusterSettingsService(new SparkConfigurationDeriver()::deriveMemorySplit)).read();

        assertThat(values.namespace()).isEqualTo("default");
        assertThat(values.get("driverPort")).isEqualTo("8002");
    }
}
