package com.cascada.spark.config;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the Spark config contract without a Spark runtime: Delta is always enabled, local and cluster
 * differ only by master, grouped spark.json layers in, and env overrides win with the documented
 * precedence (env > spark.json > defaults). This is the unit-tested core of the local==cluster premise.
 */
class SparkSessionConfigBuilderTest {

    @Test
    void localAndKubernetesDifferOnlyByMaster() {
        SparkSessionConfig local = SparkSessionConfigBuilder.forLocal()
                .withEnvironment(name -> null).build();
        SparkSessionConfig cluster = SparkSessionConfigBuilder.forKubernetes()
                .withEnvironment(name -> null).build();

        assertThat(local.isLocal()).isTrue();
        assertThat(local.master()).isEqualTo("local[*]");
        assertThat(cluster.isKubernetes()).isTrue();
        assertThat(cluster.master()).startsWith("k8s://");
        // Same Delta wiring on both — the executor code is identical.
        assertThat(local.sparkProperties()).containsAllEntriesOf(deltaWiring());
        assertThat(cluster.sparkProperties()).containsAllEntriesOf(deltaWiring());
    }

    @Test
    void deltaExtensionsAreEnabledByDefault() {
        SparkSessionConfig config = SparkSessionConfigBuilder.forLocal()
                .withEnvironment(name -> null).build();
        assertThat(config.sparkProperties())
                .containsEntry("spark.sql.extensions", SparkSessionConfigBuilder.DELTA_EXTENSIONS)
                .containsEntry("spark.sql.catalog.spark_catalog", SparkSessionConfigBuilder.DELTA_CATALOG);
    }

    @Test
    void groupedSparkJsonLayersInOverDefaults() {
        SparkSessionConfig config = SparkSessionConfigBuilder.forKubernetes()
                .withEnvironment(name -> null)
                .withGroup(Map.of(
                        "spark.executor.memory", "8g",
                        "spark.executor.cores", "3",
                        "spark.kubernetes.namespace", "review"))
                .build();
        assertThat(config.sparkProperties())
                .containsEntry("spark.executor.memory", "8g")
                .containsEntry("spark.executor.cores", "3")
                .containsEntry("spark.kubernetes.namespace", "review")
                // defaults still present
                .containsEntry("spark.sql.adaptive.enabled", "true");
    }

    @Test
    void envOverrideBeatsSparkJsonWhichBeatsDefaults() {
        Map<String, String> fakeEnv = Map.of("SPARK_EXECUTOR_MEMORY", "28g");
        SparkSessionConfig config = SparkSessionConfigBuilder.forKubernetes()
                .withEnvironment(fakeEnv::get)
                .withGroup(Map.of("spark.executor.memory", "8g")) // spark.json says 8g
                .withEnvOverride("spark.executor.memory", "SPARK_EXECUTOR_MEMORY") // env says 28g -> wins
                .build();
        assertThat(config.sparkProperties()).containsEntry("spark.executor.memory", "28g");
    }

    @Test
    void absentEnvOverrideLeavesTheExistingValue() {
        SparkSessionConfig config = SparkSessionConfigBuilder.forKubernetes()
                .withEnvironment(name -> null) // nothing set
                .withGroup(Map.of("spark.executor.memory", "8g"))
                .withEnvOverride("spark.executor.memory", "SPARK_EXECUTOR_MEMORY")
                .build();
        assertThat(config.sparkProperties()).containsEntry("spark.executor.memory", "8g");
    }

    @Test
    void sparkMasterEnvVarOverridesTheChosenMaster() {
        // Even when built forLocal(), an explicit SPARK_MASTER env points it at the cluster — exactly
        // the spark_manager.py behaviour (os.getenv("SPARK_MASTER", default)).
        Map<String, String> fakeEnv = Map.of("SPARK_MASTER", "k8s://https://my-cluster:443");
        SparkSessionConfig config = SparkSessionConfigBuilder.forLocal()
                .withEnvironment(fakeEnv::get).build();
        assertThat(config.isKubernetes()).isTrue();
        assertThat(config.master()).isEqualTo("k8s://https://my-cluster:443");
    }

    private Map<String, String> deltaWiring() {
        return Map.of(
                "spark.sql.extensions", SparkSessionConfigBuilder.DELTA_EXTENSIONS,
                "spark.sql.catalog.spark_catalog", SparkSessionConfigBuilder.DELTA_CATALOG);
    }
}
