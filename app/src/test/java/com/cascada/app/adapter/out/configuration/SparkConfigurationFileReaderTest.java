package com.cascada.app.adapter.out.configuration;

import com.cascada.app.exception.ConfigurationException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class SparkConfigurationFileReaderTest {
    @Test
    void readsTheMountedFlatSparkConfiguration() throws Exception {
        Path config = Files.createTempFile("cascada-spark", ".json");
        Files.writeString(config, "{\"spark.driver.host\":\"driver\",\"spark.executor.memory\":\"8g\"}");

        assertThat(new SparkConfigurationFileReader().read(config.toString()))
                .containsEntry("spark.driver.host", "driver")
                .containsEntry("spark.executor.memory", "8g");
    }

    @Test
    void rejectsNonSparkOrNonStringValues() throws Exception {
        Path config = Files.createTempFile("cascada-spark-invalid", ".json");
        Files.writeString(config, "{\"not.spark\": 1}");

        assertThatThrownBy(() -> new SparkConfigurationFileReader().read(config.toString()))
                .isInstanceOf(com.cascada.app.exception.ConfigurationException.class);
    }

    @Test
    void readsGroupedConfigurationAndPreservesLastValueForDuplicateSettings() throws Exception {
        Path config = Files.createTempFile("cascada-spark-grouped", ".json");
        Files.writeString(config, """
                {"driver":{"spark.driver.host":"driver","nested":{"spark.executor.memory":"4g"}},
                 "executor":{"spark.executor.memory":"8g"}}
                """);

        assertThat(new SparkConfigurationFileReader().read(config.toString()))
                .containsExactlyInAnyOrderEntriesOf(java.util.Map.of(
                        "spark.driver.host", "driver", "spark.executor.memory", "8g"));
    }

    @Test
    void rejectsInvalidSettingInsideNestedGroup() throws Exception {
        Path config = Files.createTempFile("cascada-spark-nested-invalid", ".json");
        Files.writeString(config, "{\"executor\":{\"nested\":{\"spark.executor.cores\":4}}}");

        assertThatThrownBy(() -> new SparkConfigurationFileReader().read(config.toString()))
                .isInstanceOf(ConfigurationException.class);
    }
}
