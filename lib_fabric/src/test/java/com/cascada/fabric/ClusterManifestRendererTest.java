package com.cascada.fabric;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterManifestRendererTest {

    private final ClusterManifestRenderer renderer = new ClusterManifestRenderer();

    private ClusterValues values(Map<String, String> env) {
        UnaryOperator<String> lookup = env::get;
        return ClusterValues.fromEnvironment(lookup);
    }

    @Test
    void rendersElevenManifestsInApplyOrderWithNoLeftoverPlaceholders() {
        List<String> manifests = renderer.render(values(Map.of()));
        assertThat(manifests).hasSize(11);
        assertThat(manifests.get(0)).contains("kind: ServiceAccount");
        assertThat(manifests.get(10)).contains("kind: Service");
        assertThat(String.join("\n", manifests)).doesNotContain("${");
    }

    @Test
    void combinedYamlSeparatesEveryDocument() {
        String combined = renderer.renderCombined(values(Map.of()));
        long separators = combined.lines().filter(l -> l.equals("---")).count();
        assertThat(separators).isEqualTo(11);
    }

    @Test
    void environmentVariablesOverrideDefaults() {
        ClusterValues v = values(Map.of(
                "CASCADA_RELEASE_NAME", "al-collector2",
                "CASCADA_COPY_SUFFIX", "copy2",
                "CASCADA_NAMESPACE", "review",
                "CASCADA_EXECUTOR_MEMORY", "16g",
                "CASCADA_EXECUTOR_CORES", "5"));
        String combined = renderer.renderCombined(v);

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
        String role = renderer.render(values(Map.of())).get(2); // namespace-role.yaml
        assertThat(role)
                .contains("kind: Role")
                .contains("\"pods\", \"pods/log\", \"pods/exec\", \"pods/status\"")
                .contains("sparkoperator.k8s.io")
                .contains("deployments/scale");
    }

    @Test
    void embeddedFilesAreIndentedUnderTheirConfigMapBlock() {
        List<String> manifests = renderer.render(values(Map.of()));
        String sparkConfigMap = manifests.get(7); // configmap-spark-config.yaml
        assertThat(sparkConfigMap)
                .contains("spark.json: |")
                .contains("    {")                        // json indented 4 under the block scalar
                .contains("    \"spark.submit.deployMode\": \"client\"");

        String podTemplateCm = manifests.get(6); // configmap-executor-pod-template.yaml
        assertThat(podTemplateCm)
                .contains("executor-pod-template.yaml: |")
                .contains("    kind: Pod");
    }

    @Test
    void indentSkipsBlankLinesAndPreservesThem() {
        assertThat(ClusterManifestRenderer.indent("a\n\nb", 2)).isEqualTo("  a\n\n  b");
    }

    @Test
    void fillReplacesEveryTokenAndIsPrefixSafe() {
        String out = ClusterManifestRenderer.fill("${a}/${ab}", Map.of("a", "X", "ab", "Y"));
        assertThat(out).isEqualTo("X/Y");
    }
}
