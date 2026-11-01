package com.cascada.app.adapter.out.configuration;

import com.cascada.app.exception.ConfigurationException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class SparkConfigurationFileReaderTest {
    private final SparkConfigurationFileReader reader = new SparkConfigurationFileReader();

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

    @Test
    void readsInlineGroupedCommonNativeAndJvmValues() throws Exception {
        JsonNode section = SettingValues.JSON.readTree("""
                {"common":{"spark.executor.memory":"6g"},
                 "native":{"spark.plugins":"plugin"},
                 "jvm":{"nested":{"spark.driver.memory":"2g"}}}
                """);

        assertThat(reader.readSection(section)).containsExactlyInAnyOrderEntriesOf(Map.of(
                "spark.executor.memory", "6g",
                "spark.plugins", "plugin",
                "spark.driver.memory", "2g"));
    }

    @Test
    void inlineAndFileReadsUseIdenticalParsingAndValidation() throws Exception {
        String json = """
                {"spark.driver.host":"driver",
                 "common":{"spark.executor.memory":"4g"},
                 "native":{"nested":{"spark.plugins":"plugin"}}}
                """;
        Path config = Files.createTempFile("cascada-spark-parity", ".json");
        Files.writeString(config, json);
        JsonNode section = SettingValues.JSON.readTree(json);

        assertThat(reader.readSection(section)).isEqualTo(reader.read(config.toString()));

        JsonNode invalid = SettingValues.JSON.readTree("{\"common\":{\"spark.executor.cores\":4}}");
        assertThatThrownBy(() -> reader.readSection(invalid))
                .isInstanceOf(ConfigurationException.class);
        Files.writeString(config, "{\"common\":{\"spark.executor.cores\":4}}");
        assertThatThrownBy(() -> reader.read(config.toString()))
                .isInstanceOf(ConfigurationException.class);
    }

    @Test
    void missingAndNullInlineSectionsAreEmpty() throws Exception {
        assertThat(reader.readSection(null)).isEmpty();
        assertThat(reader.readSection(MissingNode.getInstance())).isEmpty();
        assertThat(reader.readSection(SettingValues.JSON.readTree("null"))).isEmpty();
    }

    @Test
    void rejectsWrongInlineSectionAndNestedValueTypes() throws Exception {
        for (String json : java.util.List.of("false", "[]", "{\"native\":false}",
                "{\"native\":{\"spark.executor.cores\":4}}")) {
            JsonNode section = SettingValues.JSON.readTree(json);
            assertThatThrownBy(() -> reader.readSection(section))
                    .isInstanceOf(ConfigurationException.class);
        }
    }
}
