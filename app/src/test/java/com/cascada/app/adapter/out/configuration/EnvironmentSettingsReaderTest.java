package com.cascada.app.adapter.out.configuration;

import com.cascada.app.config.EngineSettings;
import com.cascada.app.config.CacheBackend;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EnvironmentSettingsReaderTest {

    @Test
    void rejectsMistypedRunModes() {
        assertThatThrownBy(() -> read(Map.of("CASCADA_RUN_MODE", "clustr")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("CASCADA_RUN_MODE");
    }

    @Test
    void storageAndSparkPlacementCanBeSelectedIndependently() {
        EngineSettings localWithValkey = read(Map.of("CASCADA_CACHE_BACKEND", "valkey"));
        assertThat(localWithValkey.useLocalSpark()).isTrue();
        assertThat(localWithValkey.cacheBackend()).isEqualTo(CacheBackend.VALKEY);
        EngineSettings clusterWithMemory = read(Map.of("CASCADA_RUN_MODE", "cluster", "CASCADA_CACHE_BACKEND", "memory"));
        assertThat(clusterWithMemory.useLocalSpark()).isFalse();
        assertThat(clusterWithMemory.cacheBackend()).isEqualTo(CacheBackend.MEMORY);
    }

    @Test
    void rejectsUnknownStorageChoices() {
        assertThatThrownBy(() -> read(Map.of("CASCADA_CACHE_BACKEND", "disk")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("CASCADA_CACHE_BACKEND");
    }

    @Test
    void validLongBucketWidthIsNotNarrowedToAnInt() {
        EngineSettings settings = read(Map.of("CASCADA_BUCKET_SECONDS", "4294967296"));

        assertThat(settings.cacheExecution().bucketSeconds()).isEqualTo(4_294_967_296L);
    }

    @Test
    void fixedStepRejectsLongValuesOutsideTheIntRangeInsteadOfWrapping() {
        assertThatThrownBy(() -> read(Map.of("CASCADA_FIXED_STEP_SECONDS", "4294967297")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CASCADA_FIXED_STEP_SECONDS");
    }

    @Test
    void warmingTopNRejectsLongValuesOutsideTheIntRangeInsteadOfWrapping() {
        assertThatThrownBy(() -> read(Map.of("CASCADA_WARMING_TOP_N", "4294967297")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CASCADA_WARMING_TOP_N");
    }

    private static EngineSettings read(Map<String, String> values) {
        return new EnvironmentSettingsReader(values::get).read();
    }
}
