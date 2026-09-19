package com.cascada.app.bootstrap;

import org.junit.jupiter.api.Test;
import java.nio.file.Files;
import java.nio.file.Path;
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
                .isInstanceOf(IllegalArgumentException.class);
    }
}
