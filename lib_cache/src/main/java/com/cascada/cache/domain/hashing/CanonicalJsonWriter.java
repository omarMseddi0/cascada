package com.cascada.cache.domain.hashing;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A minimal, dependency-free canonical JSON serializer that reproduces the exact byte shape of
 * Python's {@code json.dumps(obj, sort_keys=True, separators=(',', ':'))} for the subset of values
 * the cache hashing needs (strings, integers, string lists, and string-keyed maps).
 *
 * <p>Object keys are emitted in sorted order; there is no whitespace; strings are double-quoted
 * with JSON escaping. This determinism is what makes the logic hash stable across runs and across
 * JVMs (ARCHITECTURE §"Hashing determinism"). It lives in the domain on purpose — pulling in a JSON
 * framework here would violate the hexagon's "no framework in domain" rule.
 */
public final class CanonicalJsonWriter {

    private CanonicalJsonWriter() {
    }

    @SuppressWarnings("unchecked")
    public static String write(Object value) {
        StringBuilder builder = new StringBuilder();
        append(builder, value);
        return builder.toString();
    }

    @SuppressWarnings("unchecked")
    private static void append(StringBuilder builder, Object value) {
        switch (value) {
            case null -> builder.append("null");
            case String string -> appendString(builder, string);
            case Integer integer -> builder.append(integer.intValue());
            case Long longValue -> builder.append(longValue.longValue());
            case Boolean booleanValue -> builder.append(booleanValue.booleanValue());
            case Map<?, ?> map -> appendObject(builder, (Map<String, Object>) map);
            case List<?> list -> appendArray(builder, list);
            default -> throw new IllegalArgumentException(
                    "unsupported value type for canonical json: " + value.getClass());
        }
    }

    private static void appendObject(StringBuilder builder, Map<String, Object> map) {
        // sort_keys=True semantics: keys in ascending order.
        Map<String, Object> sorted = new TreeMap<>(map);
        builder.append('{');
        boolean first = true;
        for (Map.Entry<String, Object> entry : sorted.entrySet()) {
            if (!first) {
                builder.append(',');
            }
            first = false;
            appendString(builder, entry.getKey());
            builder.append(':');
            append(builder, entry.getValue());
        }
        builder.append('}');
    }

    private static void appendArray(StringBuilder builder, List<?> list) {
        builder.append('[');
        boolean first = true;
        for (Object element : list) {
            if (!first) {
                builder.append(',');
            }
            first = false;
            append(builder, element);
        }
        builder.append(']');
    }

    private static void appendString(StringBuilder builder, String string) {
        builder.append('"');
        for (int index = 0; index < string.length(); index++) {
            char character = string.charAt(index);
            switch (character) {
                case '"' -> builder.append("\\\"");
                case '\\' -> builder.append("\\\\");
                case '\n' -> builder.append("\\n");
                case '\r' -> builder.append("\\r");
                case '\t' -> builder.append("\\t");
                default -> {
                    if (character < 0x20) {
                        builder.append(String.format("\\u%04x", (int) character));
                    } else {
                        builder.append(character);
                    }
                }
            }
        }
        builder.append('"');
    }
}
