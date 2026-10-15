package com.cascada.app.adapter.out.configuration;

import com.cascada.app.exception.ConfigurationException;
import java.nio.file.*;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.*;

class ApplicationSettingsReaderTest {
    @TempDir Path directory;
    @Test void bindsGroupedSettingsAndExplicitPortOverrides() throws Exception {
        Path file = directory.resolve("application.json");
        Files.writeString(file, """
                {"server":{"httpPort":8100,"grpcPort":9100},
                 "warming":{"interval":"PT1S","lookback":"P2D","maximumPatterns":32,
                            "validationSql":"SELECT SUM(bytes) FROM traffic WHERE ts >= {start} AND ts <= {end}",
                            "popularityRetention":"P10D"},
                 "cacheSafety":{"highCardinalityColumns":["subscriberId"],"liquidClusteredFilterColumns":["imsi"],
                                "minimumCacheableTimeRangeSeconds":3600,"maximumCachedRows":1000},
                 "tables":[{"logicalName":"traffic","deltaPath":"hdfs://lake/traffic",
                            "columnMappings":{"ts":"ts","country":"C5"},"timeColumns":["ts"]}]}
                """);
        var settings = new ApplicationSettingsReader().read(Map.of("CASCADA_CONFIG_PATH", file.toString(), "CASCADA_HTTP_PORT", "8200")::get);
        assertThat(settings.server().httpPort()).isEqualTo(8200);
        assertThat(settings.warming().lookback()).isEqualTo(Duration.ofDays(2));
        assertThat(settings.tables().get(0).columnMappings()).containsEntry("country", "C5");
        assertThat(settings.warming().hasValidationSql()).isTrue();
        assertThat(settings.warming().popularityRetention()).isEqualTo(Duration.ofDays(10));
        assertThat(settings.cacheSafety().highCardinalityColumns()).containsExactly("subscriberId");
        assertThat(settings.cacheSafety().liquidClusteredFilterColumns()).containsExactly("imsi");
        assertThat(settings.cacheSafety().minimumCacheableTimeRangeSeconds()).isEqualTo(3600L);
        assertThat(settings.cacheSafety().maximumCachedRows()).isEqualTo(1000);
    }
    @Test void rejectsWrongTypesRatherThanSilentlyUsingDefaults() throws Exception {
        Path file = directory.resolve("invalid.json");
        for (String content : java.util.List.of("{\"server\":false}", "{\"server\":{\"httpPort\":\"many\"}}", "{\"warming\":{\"interval\":\"never\"}}",
                "{\"cacheSafety\":{\"highCardinalityColumns\":\"subscriberId\"}}",
                "{\"cacheSafety\":{\"minimumCacheableTimeRangeSeconds\":\"hour\"}}")) {
            Files.writeString(file, content);
            assertThatThrownBy(() -> new ApplicationSettingsReader().read(Map.of("CASCADA_CONFIG_PATH", file.toString())::get))
                    .isInstanceOf(ConfigurationException.class);
        }
    }
    @Test void acceptsLegacyGroupedSparkJsonAndRejectsMissingServerFiles() throws Exception {
        Path file = directory.resolve("spark.json");
        Files.writeString(file, "{\"common\":{\"spark.executor.memory\":\"6g\"},\"native\":{\"spark.plugins\":\"plugin\"}}");
        assertThat(new SparkConfigurationFileReader().read(file.toString()))
                .containsEntry("spark.executor.memory", "6g").containsEntry("spark.plugins", "plugin");
        assertThatThrownBy(() -> new SparkConfigurationFileReader().read(directory.resolve("missing.json").toString()))
                .isInstanceOf(ConfigurationException.class);
    }
}
