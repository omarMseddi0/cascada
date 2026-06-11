package com.cascada.fabric;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Fills the {@code src/main/resources/fabric} YAML templates from {@link ClusterValues} and returns the
 * complete, apply-ready manifest set (plan §6.7). The templates are real {@code kubectl apply -f} YAML;
 * this class only substitutes {@code ${placeholder}} tokens and indents the embedded block-scalar files
 * (the HDFS XML, {@code spark.json}, log4j, and the executor pod template) into their ConfigMaps.
 *
 * <p>No YAML is built in Java and there is no Kubernetes dependency here — that lives in
 * {@code com.cascada.fabric.adapter.FabricClusterDeployer}. This class is pure string templating, so it
 * is fast and trivially unit-testable.
 */
public final class ClusterManifestRenderer {

    /** Manifest templates in {@code kubectl apply} order (RBAC → ConfigMaps → Deployment → Service). */
    private static final List<String> TEMPLATES = List.of(
            "service-account.yaml",
            "cluster-role-binding.yaml",
            "namespace-role.yaml",
            "role-binding.yaml",
            "configmap-core-site.yaml",
            "configmap-hdfs-site.yaml",
            "configmap-executor-pod-template.yaml",
            "configmap-spark-config.yaml",
            "configmap-log4j.yaml",
            "deployment-driver.yaml",
            "service-driver.yaml");

    /** Render every manifest, in apply order, as a list of YAML documents. */
    public List<String> render(ClusterValues values) {
        Map<String, String> v = new LinkedHashMap<>(values.placeholders());
        // Resolve the embedded files first, indented to sit under their ConfigMap `data:` block scalar.
        v.put("coreSiteXml", indent(fill(loadFile("core-site.xml"), v), 4));
        v.put("hdfsSiteXml", indent(fill(loadFile("hdfs-site.xml"), v), 4));
        v.put("log4j", indent(loadFile("log4j2.properties"), 4));
        v.put("sparkJson", indent(fill(loadFile("spark.json"), v), 4));
        v.put("executorPodTemplate", indent(fill(loadTemplate("pod-template.yaml"), v), 4));

        List<String> manifests = new ArrayList<>(TEMPLATES.size());
        for (String template : TEMPLATES) {
            manifests.add(fill(loadTemplate(template), v));
        }
        return manifests;
    }

    /** The manifests joined into one multi-document YAML stream — what you would feed to {@code apply -f}. */
    public String renderCombined(ClusterValues values) {
        StringBuilder out = new StringBuilder();
        for (String manifest : render(values)) {
            out.append("---\n").append(manifest);
            if (out.charAt(out.length() - 1) != '\n') {
                out.append('\n');
            }
        }
        return out.toString();
    }

    private static String loadTemplate(String name) {
        return load("/fabric/templates/" + name);
    }

    private static String loadFile(String name) {
        return load("/fabric/files/" + name);
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
        String pad = " ".repeat(spaces);
        StringBuilder out = new StringBuilder();
        String[] lines = text.split("\n", -1);
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            if (!line.isEmpty()) {
                out.append(pad).append(line);
            }
            if (i < lines.length - 1) {
                out.append('\n');
            }
        }
        return out.toString();
    }
}
