package com.cascada.sparkconfig.domain;

import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * An immutable, sorted view of a derived Spark configuration: the assembled {@code spark.*} key/value
 * map that the orchestrator would hand to {@code SparkSession.builder}. Reproducing the golden
 * {@code data_collector/src/spark.json} from the three knobs is the whole point of the derivation.
 */
public record SparkConfiguration(Map<String, String> entries) {

    public SparkConfiguration {
        entries = new TreeMap<>(entries);
    }

    public Optional<String> get(String key) {
        return Optional.ofNullable(entries.get(key));
    }

    public String require(String key) {
        return get(key).orElseThrow(() ->
                new IllegalStateException("derived Spark configuration is missing required key: " + key));
    }

    public boolean containsKey(String key) {
        return entries.containsKey(key);
    }

    public int size() {
        return entries.size();
    }
}
