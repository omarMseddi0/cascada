package com.cascada.app.adapter.out.configuration;

import com.cascada.app.exception.ConfigurationException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/** Reads the flat Spark JSON ConfigMap mounted by the Kubernetes deployment. */
public final class SparkConfigurationFileReader {
    private static final ObjectMapper JSON = new ObjectMapper();

    public Map<String, String> read(String path) {
        if (path == null || path.isBlank()) return Map.of();
        try {
            Map<String, Object> raw = JSON.readValue(Files.readString(Path.of(path)), new TypeReference<>() { });
            if (raw == null) {
                throw new IllegalArgumentException("Spark configuration must be a flat map of spark.* strings");
            }
            Map<String, String> properties = new LinkedHashMap<>();
            flatten(raw, properties);
            return Map.copyOf(properties);
        } catch (IOException | IllegalArgumentException failure) {
            throw new ConfigurationException("Cannot read Spark configuration: " + path, failure);
        }
    }
    private static void flatten(Map<String, Object> group, Map<String, String> properties) {
        group.forEach((key, value) -> {
            if (key.startsWith("spark.") && value instanceof String text) properties.put(key, text);
            else if (!key.startsWith("spark.") && value instanceof Map<?, ?> nested) {
                Map<String, Object> section = new LinkedHashMap<>();
                nested.forEach((name, setting) -> section.put(name.toString(), setting));
                flatten(section, properties);
            } else throw new IllegalArgumentException("Spark configuration must contain spark.* string values");
        });
    }
}
