package com.cascada.fabric.adapter.out.manifest;

import com.cascada.fabric.application.port.out.ClusterManifestRenderingPort;
import com.cascada.fabric.domain.ClusterValues;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Loads the single deployment.yaml source, substitutes settings, and preserves apply order.
 * Spark JSON, Hadoop XML, logging, and executor pod template are embedded in that file.
 */
public final class ClusterManifestRenderer implements ClusterManifestRenderingPort {

    /** The single source contains all resources, including embedded configuration files. */
    @Override
    public List<String> render(ClusterValues values) {
        Objects.requireNonNull(values, "values");
        String combined = fill(load("/fabric/deployment.yaml"), values.placeholders());
        if (combined.contains("${")) throw new IllegalStateException("unresolved Fabric placeholder");
        return java.util.Arrays.stream(combined.split("(?m)^---\\s*$"))
                .filter(document -> !document.isBlank()).map(String::stripLeading).toList();
    }

    /** The manifests joined into one multi-document YAML stream — what you would feed to {@code apply -f}. */
    public String renderCombined(ClusterValues values) {
        return render(values).stream()
                .map(manifest -> "---\n" + manifest + (manifest.endsWith("\n") ? "" : "\n"))
                .collect(Collectors.joining());
    }

    private static String load(String resourcePath) {
        try (InputStream in = ClusterManifestRenderer.class.getResourceAsStream(resourcePath)) {
            if (in == null) {
                throw new IllegalStateException("missing Fabric resource: " + resourcePath);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read Fabric resource: " + resourcePath, e);
        }
    }

    /** Replace every {@code ${key}} with its value; the closing brace keeps key prefixes unambiguous. */
    static String fill(String template, Map<String, String> values) {
        String out = template;
        for (Map.Entry<String, String> entry : values.entrySet()) {
            out = out.replace("${" + entry.getKey() + "}", entry.getValue());
        }
        return out;
    }

    /** Indent every non-empty line by {@code spaces}, so a file can be embedded under a YAML {@code |}. */
    static String indent(String text, int spaces) {
        String padding = " ".repeat(spaces);
        return Arrays.stream(text.split("\n", -1))
                .map(line -> line.isEmpty() ? line : padding + line)
                .collect(Collectors.joining("\n"));
    }
}
