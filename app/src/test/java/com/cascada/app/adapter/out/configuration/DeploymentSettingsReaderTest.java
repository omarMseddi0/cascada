package com.cascada.app.adapter.out.configuration;

import com.cascada.app.exception.ConfigurationException;
import com.cascada.app.model.config.CacheBackend;
import com.cascada.app.model.config.EngineSettings;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;

class DeploymentSettingsReaderTest {
    @TempDir Path directory;

    @Test
    void readsAllSettingsFromOneFileAndOneSnapshot() throws Exception {
        Path file = directory.resolve("cascada.json");
        Files.writeString(file, """
                {"engine":{"runMode":"cluster","mainTableName":"events","mainTablePath":"/lake/events"},
                 "cache":{"backend":"valkey","redisUri":"redis://cache:6379","bucketSeconds":3600,
                          "fixedStepSeconds":60,"timeColumnName":"event_time","mergeWorkers":8},
                 "server":{"httpPort":8100,"grpcPort":9100},
                 "warming":{"enabled":false,"topNQueries":12},
                 "spark":{"common":{"spark.executor.memory":"6g"},"jvm":{"spark.executor.cores":"4"}}}
                """);
        AtomicInteger configPathLookups = new AtomicInteger();
        var settings = new DeploymentSettingsReader().read(name -> {
            if (name.equals("CASCADA_CONFIG_PATH")) { configPathLookups.incrementAndGet(); return file.toString(); }
            if (name.equals("SPARK_CONFIG_PATH")) throw new AssertionError("central Spark section must be used");
            return null;
        });
        assertThat(configPathLookups.get()).isEqualTo(1);
        assertThat(settings.engine().useLocalSpark()).isFalse();
        assertThat(settings.engine().mainTableName()).isEqualTo("events");
        assertThat(settings.engine().mainTablePath()).isEqualTo("/lake/events");
        assertThat(settings.engine().redisUri()).isEqualTo("redis://cache:6379");
        assertThat(settings.engine().cacheBackend()).isEqualTo(CacheBackend.VALKEY);
        assertThat(settings.engine().cacheExecution().bucketSeconds()).isEqualTo(3600);
        assertThat(settings.engine().cacheExecution().fixedStepSeconds()).isEqualTo(60);
        assertThat(settings.engine().cacheExecution().timeColumnName()).isEqualTo("event_time");
        assertThat(settings.engine().cacheExecution().mergeWorkerCount()).isEqualTo(8);
        assertThat(settings.engine().warmingTopNQueries()).isEqualTo(12);
        assertThat(settings.application().server().httpPort()).isEqualTo(8100);
        assertThat(settings.application().warming().enabled()).isFalse();
        assertThat(settings.sparkProperties()).containsEntry("spark.executor.memory", "6g")
                .containsEntry("spark.executor.cores", "4");
        assertThatThrownBy(() -> settings.sparkProperties().put("spark.x", "y"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void existingEnvironmentOverridesRemainExplicitAndCompatible() throws Exception {
        Path file = directory.resolve("override.json");
        Files.writeString(file, "{\"cache\":{\"mergeWorkers\":8},\"server\":{\"httpPort\":8100}}");
        var settings = new DeploymentSettingsReader().read(Map.of("CASCADA_CONFIG_PATH", file.toString(),
                "CASCADA_MERGE_WORKERS", "12", "CASCADA_HTTP_PORT", "8200")::get);
        assertThat(settings.engine().cacheExecution().mergeWorkerCount()).isEqualTo(12);
        assertThat(settings.application().server().httpPort()).isEqualTo(8200);
    }

    @Test
    void legacySparkPathAndDefaultsStillWorkWhenCentralSparkIsAbsent() throws Exception {
        Path spark = directory.resolve("legacy-spark.json");
        Files.writeString(spark, "{\"spark.executor.memory\":\"5g\"}");
        var settings = new DeploymentSettingsReader().read(Map.of("SPARK_CONFIG_PATH", spark.toString())::get);
        assertThat(settings.sparkProperties()).containsExactlyEntriesOf(Map.of("spark.executor.memory", "5g"));
        assertThat(settings.engine()).isEqualTo(EngineSettings.localDefaults());
    }

    @Test
    void rejectsInvalidCentralCacheSettingsBeforeStartup() throws Exception {
        Path file = directory.resolve("bad.json");
        for (String content : java.util.List.of("{\"cache\":false}",
                "{\"cache\":{\"mergeWorkers\":0}}", "{\"cache\":{\"mergeWorkers\":\"eight\"}}",
                "{\"cache\":{\"fixedStepSeconds\":4294967297}}")) {
            Files.writeString(file, content);
            assertThatThrownBy(() -> new DeploymentSettingsReader().read(Map.of("CASCADA_CONFIG_PATH", file.toString())::get))
                    .as(content).isInstanceOf(ConfigurationException.class);
        }
    }
}
