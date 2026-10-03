package com.cascada.sparkconfig.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/** Loads and flattens the required, versioned Spark profile used by the parity tests. */
final class SparkJsonGoldenFixture {

    private SparkJsonGoldenFixture() {
    }

    static Map<String, String> loadFlattenedConfiguration() throws IOException {
        try (InputStream input = SparkJsonGoldenFixture.class.getResourceAsStream("spark.json")) {
            if (input == null) {
                throw new IllegalStateException("Required test fixture spark.json is missing from the module");
            }

            JsonNode root = new ObjectMapper().readTree(input);
            if (!root.isObject()) {
                throw new IOException("Required test fixture spark.json must contain a JSON object");
            }

            Map<String, String> flattened = new TreeMap<>();
            Iterator<Map.Entry<String, JsonNode>> groups = root.fields();
            while (groups.hasNext()) {
                Map.Entry<String, JsonNode> group = groups.next();
                if (!group.getValue().isObject()) {
                    throw new IOException("Spark profile section must be a JSON object: " + group.getKey());
                }

                Iterator<Map.Entry<String, JsonNode>> leaves = group.getValue().fields();
                while (leaves.hasNext()) {
                    Map.Entry<String, JsonNode> leaf = leaves.next();
                    // The profile intentionally repeats some keys in later groups; the final group wins,
                    // matching the order in which the source configuration is assembled.
                    flattened.put(leaf.getKey(), leaf.getValue().asText());
                }
            }
            return Collections.unmodifiableMap(flattened);
        }
    }
}
