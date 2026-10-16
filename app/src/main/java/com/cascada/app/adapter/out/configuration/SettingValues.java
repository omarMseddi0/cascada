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
