package com.cascada.app.bootstrap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/** Reads the flat Spark JSON ConfigMap mounted by the Kubernetes deployment. */
final class SparkConfigurationFileReader {
    private static final ObjectMapper JSON = new ObjectMapper();

    Map<String, String> read(String path) {
        if (path == null || path.isBlank()) return Map.of();
        try {
            Map<String, Object> raw = JSON.readValue(Files.readString(Path.of(path)), new TypeReference<>() { });
            Map<String, String> properties = new LinkedHashMap<>();
            raw.forEach((key, value) -> {
                if (!key.startsWith("spark.") || !(value instanceof String)) {
                    throw new IllegalArgumentException("Spark configuration must be a flat map of spark.* strings");
                }
                properties.put(key, (String) value);
            });
            return Map.copyOf(properties);
        } catch (IOException failure) {
            throw new IllegalArgumentException("cannot read SPARK_CONFIG_PATH: " + path, failure);
        }
    }
}
