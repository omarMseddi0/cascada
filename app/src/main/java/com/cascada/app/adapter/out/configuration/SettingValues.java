package com.cascada.app.adapter.out.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Parsing rules shared by the environment, application-settings and Spark-config readers, so every
 * numeric setting is read the same way: surrounding blanks ignored, an unset or blank value means the
 * default, anything else must be an integer within range.
 */
final class SettingValues {

    /** The one JSON mapper of the configuration readers. */
    static final ObjectMapper JSON = new ObjectMapper();

    private SettingValues() {
    }

    static com.fasterxml.jackson.databind.JsonNode applicationConfiguration(java.util.function.Function<String, String> environment) {
        String path = environment.apply("CASCADA_CONFIG_PATH");
        try {
            var configuration = path == null || path.isBlank() ? JSON.createObjectNode()
                    : JSON.readTree(java.nio.file.Files.readString(java.nio.file.Path.of(path)));
            if (configuration == null || !configuration.isObject())
                throw new IllegalArgumentException("Application configuration must be an object");
            return configuration;
        } catch (java.io.IOException | IllegalArgumentException failure) {
            throw new com.cascada.app.exception.ConfigurationException("Invalid application configuration", failure);
        }
    }

    static com.fasterxml.jackson.databind.JsonNode section(com.fasterxml.jackson.databind.JsonNode root, String name) {
        var section = root.path(name);
        if (!section.isMissingNode() && !section.isObject())
            throw new IllegalArgumentException(name + " must be an object");
        return section;
    }

    static long jsonLong(com.fasterxml.jackson.databind.JsonNode section, String name, long fallback) {
        if (!section.has(name)) return fallback;
        var value = section.get(name);
        if (!value.isIntegralNumber() || !value.canConvertToLong())
            throw new IllegalArgumentException(name + " must be an integer");
        return value.longValue();
    }

    static int jsonInt(com.fasterxml.jackson.databind.JsonNode section, String name, int fallback) {
        long value = jsonLong(section, name, fallback);
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE)
            throw new IllegalArgumentException(name + " must be a 32-bit integer");
        return (int) value;
    }

    static String jsonText(com.fasterxml.jackson.databind.JsonNode section, String name, String fallback) {
        if (!section.has(name)) return fallback;
        if (!section.get(name).isTextual()) throw new IllegalArgumentException(name + " must be a string");
        return section.get(name).textValue();
    }

    static long longValue(String name, String raw, long fallback, long minimum, long maximum) {
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        long value;
        try {
            value = Long.parseLong(raw.trim());
        } catch (NumberFormatException notANumber) {
            // Fail loudly: silently falling back would start the engine with a setting the operator
            // did not ask for (a wrong bucket width writes every key under the wrong grain).
            throw new IllegalArgumentException(name + " must be an integer, but was: '" + raw + "'", notANumber);
        }
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException(name + " must be between " + minimum + " and " + maximum
                    + ", but was: " + value);
        }
        return value;
    }

    static int intValue(String name, String raw, int fallback, int minimum, int maximum) {
        return (int) longValue(name, raw, fallback, minimum, maximum);
    }
}
