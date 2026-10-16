package com.cascada.app.architecture;

import com.cascada.sparkconfig.domain.SparkReference;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The JVM module flags and Gluten defaults live in formats that cannot import each other (Maven,
 * Dockerfile, the Kubernetes template). The root pom's {@code agrona.jvm.args} is the single source;
 * scripts read it directly, and this test fails the build when any other copy drifts from it.
 */
class DeploymentConsistencyTest {

    /** The reactor root: Surefire runs from {@code app/} when forked, from the root when in-process. */
    private static final Path ROOT = repositoryRoot();

    private static Path repositoryRoot() {
        for (Path dir = Path.of("").toAbsolutePath(); dir != null; dir = dir.getParent()) {
            if (Files.isDirectory(dir.resolve("lib_fabric")) && Files.isRegularFile(dir.resolve("pom.xml"))) return dir;
        }
        throw new IllegalStateException("Cannot locate the Cascada repository root");
    }

    @Test
    void everyJvmEntryPointCarriesThePomModuleFlags() throws IOException {
        List<String> flags = pomJvmFlags();
        assertThat(flags).hasSize(10);

        assertThat(flagsIn(read("Dockerfile"), "ENV JAVA_TOOL_OPTIONS=\"([^\"]*)\"")).as("Dockerfile")
                .containsExactlyInAnyOrderElementsOf(flags);
        String deployment = read("lib_fabric/src/main/resources/fabric/deployment.yaml");
        assertThat(flagsIn(deployment, "\"spark\\.executor\\.extraJavaOptions\": \"([^\"]*)\"")).as("executors")
                .containsExactlyInAnyOrderElementsOf(flags);
        assertThat(flagsIn(deployment, "name: JAVA_TOOL_OPTIONS\\s+value: \"-Xmx\\$\\{driverMemory\\} ([^\"]*)\""))
                .as("driver").containsExactlyInAnyOrderElementsOf(flags);
    }

    @Test
    void scriptsReadTheFlagsFromThePomInsteadOfCopyingThem() throws IOException {
        try (var scripts = Files.list(ROOT.resolve("scripts"))) {
            for (Path script : scripts.filter(path -> path.toString().endsWith(".ps1")).toList()) {
                assertThat(Files.readString(script)).as(script.getFileName().toString())
                        .doesNotContain("--add-opens=java.base/");
            }
        }
    }

    @Test
    void theDeployedGlutenProfileMatchesTheSessionBuilderDefaults() throws IOException {
        String deployment = read("lib_fabric/src/main/resources/fabric/deployment.yaml");
        SparkReference.GLUTEN_TUNING_DEFAULTS.forEach((key, value) -> {
            // ANSI mode is applied by the session builder whenever Gluten is on; it is not a deployed key.
            if (!key.equals("spark.sql.ansi.enabled")) {
                assertThat(deployment).as(key).contains("\"" + key + "\": \"" + value + "\"");
            }
        });
        assertThat(deployment).as("the driver must see the deployed Gluten switch")
                .contains("name: CASCADA_GLUTEN_ENABLED\n          value: \"${glutenEnabled}\"");
    }

    private static List<String> pomJvmFlags() throws IOException {
        Matcher property = Pattern.compile("<agrona\\.jvm\\.args>([^<]*)</agrona\\.jvm\\.args>").matcher(read("pom.xml"));
        assertThat(property.find()).isTrue();
        return Arrays.asList(property.group(1).trim().split("\\s+"));
    }

    private static List<String> flagsIn(String text, String regex) {
        Matcher matcher = Pattern.compile(regex).matcher(text);
        assertThat(matcher.find()).as(regex).isTrue();
        return Arrays.asList(matcher.group(1).trim().split("\\s+"));
    }

    private static String read(String relativePath) throws IOException {
        return Files.readString(ROOT.resolve(relativePath)).replace("\r\n", "\n");
    }
}
