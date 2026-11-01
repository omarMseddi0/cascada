package com.cascada.app.adapter.out.configuration;

import com.cascada.app.exception.ConfigurationException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/** Reads the flat Spark JSON ConfigMap mounted by the Kubernetes deployment. */
public final class SparkConfigurationFileReader {
    private static final ObjectMapper JSON = SettingValues.JSON;

    public Map<String, String> read(String path) {
        if (path == null || path.isBlank()) return Map.of();
        try {
            JsonNode root = JSON.readTree(Files.readString(Path.of(path)));
            if (root == null || root.isNull()) {
                throw new IllegalArgumentException("Spark configuration must be a flat map of spark.* strings");
            }
            return parseSection(root);
        } catch (IOException | IllegalArgumentException failure) {
            throw new ConfigurationException("Cannot read Spark configuration: " + path, failure);
        }
    }

    /** Parses an inline Spark configuration section without reading files or process settings. */
    public Map<String, String> readSection(JsonNode section) {
        if (section == null || section.isMissingNode() || section.isNull()) return Map.of();
        try {
            return parseSection(section);
        } catch (IllegalArgumentException failure) {
            throw new ConfigurationException("Cannot read Spark configuration section", failure);
        }
    }

    private static Map<String, String> parseSection(JsonNode section) {
        if (!section.isObject()) {
            throw new IllegalArgumentException("Spark configuration must be a flat map of spark.* strings");
        }
        Map<String, Object> raw = JSON.convertValue(section, new TypeReference<>() { });
        Map<String, String> properties = new LinkedHashMap<>();
        flatten(raw, properties);
        return Map.copyOf(properties);
    }

    private static void flatten(Map<?, ?> group, Map<String, String> properties) {
        for (var entry : group.entrySet()) {
            String key = entry.getKey().toString();
            Object value = entry.getValue();
            if (key.startsWith("spark.") && value instanceof String text) properties.put(key, text);
            else if (!key.startsWith("spark.") && value instanceof Map<?, ?> nested) {
                flatten(nested, properties);
            } else throw new IllegalArgumentException("Spark configuration must contain spark.* string values");
        }
    }
}
