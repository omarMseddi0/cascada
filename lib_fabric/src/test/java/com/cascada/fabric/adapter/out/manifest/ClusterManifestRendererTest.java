package com.cascada.fabric.adapter.out.manifest;

import com.cascada.fabric.application.service.ClusterSettingsService;
import com.cascada.fabric.domain.model.ClusterValues;
import com.cascada.sparkconfig.application.service.SparkConfigurationDeriver;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterManifestRendererTest {

    @Test
    void rejectsDocumentJsonAndXmlInjectionInPlaceholderValues() {
        for (String attack : List.of("safe\n---\nkind: Pod", "x\",\"spark.plugins\":\"evil", "/tmp/# comment", "hdfs://x</value><property>")) {
            org.assertj.core.api.Assertions.assertThatThrownBy(
                    () -> renderer.render(new ClusterValues(Map.of("namespace", attack))))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> values(Map.of("CASCADA_NAMESPACE", "default\n---\nkind: Pod")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void keepsUnicodeTableNamesAndPathsInEmbeddedApplicationConfiguration() throws Exception {
        String table = "table_\u6771\u4eac", path = "hdfs://lake/\u6771\u4eac";
        var documents = renderer.render(values(Map.of("CASCADA_MAIN_TABLE_NAME", table, "CASCADA_MAIN_TABLE_PATH", path)));
        var config = io.fabric8.kubernetes.client.utils.Serialization.unmarshal(
                documents.get(6), io.fabric8.kubernetes.api.model.ConfigMap.class);
        var application = new com.fasterxml.jackson.databind.ObjectMapper().readTree(config.getData().get("application.json"));
        assertThat(application.path("tables").get(0).path("logicalName").textValue()).isEqualTo(table);
        assertThat(application.path("tables").get(0).path("deltaPath").textValue()).isEqualTo(path);
    }

    private final ClusterManifestRenderer renderer = new ClusterManifestRenderer();

    private ClusterValues values(Map<String, String> environmentValues) {
        return new ClusterSettingsService(new SparkConfigurationDeriver()::deriveMemorySplit).prepareClusterSettings(environmentValues);
    }

    @Test
    void rendersTenManifestsInApplyOrderWithNoLeftoverPlaceholders() {
        List<String> manifests = renderer.render(values(Map.of()));
        assertThat(manifests).hasSize(10);
        assertThat(manifests.get(0)).contains("kind: ServiceAccount");
        assertThat(manifests.get(9)).contains("kind: Service");
        assertThat(String.join("\n", manifests)).doesNotContain("kind: ClusterRoleBinding");
        assertThat(String.join("\n", manifests)).doesNotContain("${");
    }

    @Test
    void combinedYamlSeparatesEveryDocument() {
        String combined = renderer.renderCombined(values(Map.of()));
        long separators = combined.lines().filter(l -> l.equals("---")).count();
        assertThat(separators).isEqualTo(10);
    }

    @Test
    void environmentVariablesOverrideDefaults() {
        ClusterValues clusterValues = values(Map.of(
                "CASCADA_RELEASE_NAME", "al-collector2",
                "CASCADA_COPY_SUFFIX", "copy2",
                "CASCADA_NAMESPACE", "review",
                "CASCADA_EXECUTOR_RAM_GB", "64",
                "CASCADA_EXECUTOR_MEMORY", "16g",
                "CASCADA_EXECUTOR_CORES", "5"));
        String combined = renderer.renderCombined(clusterValues);

        assertThat(combined)
                .contains("name: al-collector2-spark-copy2")          // derived SA name
                .contains("namespace: review")
                .contains("\"spark.executor.memory\": \"16g\"")
                .contains("\"spark.executor.cores\": \"5\"")
                .contains("\"spark.kubernetes.executor.limit.cores\": \"6\"") // cores + 1
                .contains("al-collector2-spark-driver-copy2.review.svc.cluster.local");
    }

    @Test
    void rbacRulesAreExpandedForTheSparkDriver() {
        String role = renderer.render(values(Map.of())).get(1); // namespace-role.yaml
        assertThat(role)
                .contains("kind: Role")
                .contains("\"pods\", \"pods/log\", \"pods/exec\", \"pods/status\"")
                .contains("sparkoperator.k8s.io")
                .contains("deployments/scale");
    }

    @Test
    void embeddedFilesAreIndentedUnderTheirConfigMapBlock() {
        List<String> manifests = renderer.render(values(Map.of()));
        String sparkConfigMap = manifests.get(6); // configmap-spark-config.yaml
        assertThat(sparkConfigMap)
                .contains("spark.json: |")
                .contains("    {")                        // json indented 4 under the block scalar
                .contains("    \"spark.submit.deployMode\": \"client\"");

        String podTemplateCm = manifests.get(5); // configmap-executor-pod-template.yaml
        assertThat(podTemplateCm)
                .contains("executor-pod-template.yaml: |")
                .contains("    kind: Pod")
                .contains("    - name: HADOOP_CONF_DIR")
                .contains("      value: /opt/spark/work-dir");

        String deployment = manifests.get(8);
        assertThat(deployment)
                .contains("- name: HADOOP_CONF_DIR")
                .contains("value: /opt/spark/work-dir");
    }

    @Test
    void renderedSparkConfigSetsAnInitialExecutorTargetWithinItsBounds() {
        ClusterValues values = values(Map.of(
                "CASCADA_MIN_EXECUTORS", "0",
                "CASCADA_MAX_EXECUTORS", "1"));
        String sparkConfigMap = renderer.render(values).get(6);

        assertThat(sparkConfigMap)
                .contains("\"spark.executor.instances\": \"1\"")
                .contains("\"spark.dynamicAllocation.minExecutors\": \"0\"")
                .contains("\"spark.dynamicAllocation.maxExecutors\": \"1\"");
    }

    @Test
    void renderedSparkAndHadoopConfigLeaveBlockReplicationToStoragePolicy() {
        String combined = renderer.renderCombined(values(Map.of()));

        assertThat(combined)
                .doesNotContain("spark.hadoop.dfs.replication")
                .doesNotContain("<name>dfs.replication</name>");
    }

    @Test
    void nativeDeploymentEnablesPluginAndFitsTheExecutorBudget() {
        String yaml = renderer.renderCombined(new ClusterSettingsService(new SparkConfigurationDeriver()::deriveMemorySplit).prepareClusterSettings(Map.of()));
        assertThat(yaml).contains("org.apache.gluten.GlutenPlugin")
                .contains("org.apache.spark.shuffle.sort.ColumnarShuffleManager")
                .contains("\"spark.memory.offHeap.size\": \"9g\"")
                .contains("\"spark.executor.memoryOverhead\": \"1g\"")
                .contains("containerPort: 9090").contains("containerPort: 8000");
        String vanilla = renderer.renderCombined(values(Map.of("CASCADA_GLUTEN_ENABLED", "false")));
        assertThat(vanilla).doesNotContain("org.apache.gluten.GlutenPlugin")
                .contains("\"spark.memory.offHeap.enabled\": \"false\"");
    }

    @Test
    void fillReplacesEveryTokenAndIsPrefixSafe() {
        String out = ClusterManifestRenderer.fill("${a}/${ab}", Map.of("a", "X", "ab", "Y"));
        assertThat(out).isEqualTo("X/Y");
    }
    @Test
    void embedsApplicationAndFairSchedulingConfigurationAsRealConfigMapKeys() throws Exception {
        var documents = renderer.render(new ClusterSettingsService(new SparkConfigurationDeriver()::deriveMemorySplit).prepareClusterSettings(Map.of()));
        io.fabric8.kubernetes.api.model.ConfigMap spark = io.fabric8.kubernetes.client.utils.Serialization
                .unmarshal(documents.get(6), io.fabric8.kubernetes.api.model.ConfigMap.class);
        io.fabric8.kubernetes.api.model.ConfigMap logging = io.fabric8.kubernetes.client.utils.Serialization
                .unmarshal(documents.get(7), io.fabric8.kubernetes.api.model.ConfigMap.class);
        assertThat(spark.getData()).containsKey("application.json");
        assertThat(logging.getData()).containsKey("fairscheduler.xml");
        assertThat(logging.getData().get("fairscheduler.xml")).contains("<schedulingMode>FAIR</schedulingMode>");
        var json = new com.fasterxml.jackson.databind.ObjectMapper().readTree(spark.getData().get("application.json"));
        assertThat(json.path("server").path("httpPort").intValue()).isEqualTo(8000);
    }
}
