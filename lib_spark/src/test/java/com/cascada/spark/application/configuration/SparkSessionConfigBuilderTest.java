package com.cascada.spark.application.configuration;

import com.cascada.spark.domain.SparkSessionConfig;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
                .containsEntry("spark.sql.extensions", SparkSessionConfigBuilder.DELTA_EXTENSIONS + "," + SparkSessionConfigBuilder.GLUTEN_EXTENSIONS)
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
                "spark.sql.extensions", SparkSessionConfigBuilder.DELTA_EXTENSIONS + "," + SparkSessionConfigBuilder.GLUTEN_EXTENSIONS,
                "spark.sql.catalog.spark_catalog", SparkSessionConfigBuilder.DELTA_CATALOG);
    }

    @Test void veloxIsExplicitAndPreservesOtherPluginsAndDelta() {
        var config = SparkSessionConfigBuilder.forLocal().withProperty("spark.plugins", "example.OtherPlugin")
                .withVelox(536870912L).withVelox(536870912L).build();
        assertThat(config.sparkProperties()).containsEntry("spark.plugins", "example.OtherPlugin,org.apache.gluten.GlutenPlugin")
                .containsEntry("spark.shuffle.manager", SparkSessionConfigBuilder.GLUTEN_SHUFFLE)
                .containsEntry("spark.memory.offHeap.enabled", "true")
                .containsEntry("spark.gluten.memory.isolation", "true")
                .containsEntry("spark.gluten.sql.columnar.backend.velox.spillStrategy", "auto")
                .containsEntry("spark.sql.ansi.enabled", "false")
                .containsEntry("spark.memory.offHeap.size", "536870912")
                .containsAllEntriesOf(deltaWiring());
        assertThat(SparkSessionConfigBuilder.forLocal().withEnvironment(Map.of("CASCADA_GLUTEN_ENABLED", "false")::get)
                .build().sparkProperties()).doesNotContainKey("spark.plugins");
        assertThat(SparkSessionConfigBuilder.forLocal().withProperty("spark.gluten.memory.isolation", "false")
                .withVelox(536870912L).build().sparkProperties())
                .containsEntry("spark.gluten.memory.isolation", "false");
    }

    @Test void veloxEnvironmentEnablesTheSharedSessionConfiguration() {
        Map<String, String> env = Map.of("CASCADA_GLUTEN_ENABLED", "true", "CASCADA_GLUTEN_OFFHEAP_BYTES", "268435456",
                "CASCADA_GLUTEN_JAR", "/opt/gluten/gluten-spark42.jar", "SPARK_MASTER", "local[2]");
        var config = SparkSessionConfigBuilder.forLocal().withEnvironment(env::get)
                .withProperty("spark.jars", "/opt/other.jar").build();
        assertThat(config.master()).isEqualTo("local[2]");
        assertThat(config.sparkProperties()).containsEntry("spark.memory.offHeap.size", "268435456")
                .containsEntry("spark.jars", "/opt/other.jar,/opt/gluten/gluten-spark42.jar");
    }

    @Test void enablingVeloxWithoutAMemoryOverridePreservesConfiguredBudget() {
        var config = SparkSessionConfigBuilder.forLocal()
                .withProperty("spark.memory.offHeap.size", "6g")
                .withEnvironment(Map.of("CASCADA_GLUTEN_ENABLED", "true")::get).build();
        assertThat(config.sparkProperties()).containsEntry("spark.memory.offHeap.size", "6g")
                .containsEntry("spark.gluten.sql.columnar.maxBatchSize", "4096")
                .containsEntry("spark.gluten.memory.dynamic.offHeap.sizing.enabled", "false");
    }

    @Test void explicitlyDisabledVeloxRemovesOnlyItsPluginAndShuffle() {
        var config = SparkSessionConfigBuilder.forLocal().withVelox(536870912)
                .withProperty("spark.plugins", "example.OtherPlugin," + SparkSessionConfigBuilder.GLUTEN_PLUGIN)
                .withEnvironment(Map.of("CASCADA_GLUTEN_ENABLED", "false")::get).build();
        assertThat(config.sparkProperties()).containsEntry("spark.plugins", "example.OtherPlugin")
                .doesNotContainKey("spark.shuffle.manager")
                .containsEntry("spark.sql.extensions", SparkSessionConfigBuilder.DELTA_EXTENSIONS);
    }

    @Test void explicitDeltaExtensionFromJsonDoesNotSilentlyDisableNativeRules() {
        var config = SparkSessionConfigBuilder.forLocal()
                .withGroup(Map.of("spark.sql.extensions", SparkSessionConfigBuilder.DELTA_EXTENSIONS)).build();
        assertThat(config.sparkProperties()).containsEntry("spark.sql.extensions",
                SparkSessionConfigBuilder.DELTA_EXTENSIONS + "," + SparkSessionConfigBuilder.GLUTEN_EXTENSIONS);
    }

    @Test void operatorsCanExplicitlyKeepAnsiSemanticsWithNativeFallback() {
        var config = SparkSessionConfigBuilder.forLocal().withGroup(Map.of("spark.sql.ansi.enabled", "true")).build();
        assertThat(config.sparkProperties()).containsEntry("spark.sql.ansi.enabled", "true");
    }

    @Test void invalidNativeMemoryAndEnableValuesAreRejected() {
        assertThatThrownBy(() -> SparkSessionConfigBuilder.forLocal().withVelox(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SparkSessionConfigBuilder.forLocal()
                .withEnvironment(Map.of("CASCADA_GLUTEN_ENABLED", "yes")::get).build()).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SparkSessionConfigBuilder.forLocal().withEnvironment(Map.of(
                "CASCADA_GLUTEN_ENABLED", "true", "CASCADA_GLUTEN_OFFHEAP_BYTES", "bad")::get).build())
                .isInstanceOf(IllegalArgumentException.class);
    }
}
