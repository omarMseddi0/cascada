package com.cascada.cache.domain.frame;

import java.util.List;
import java.util.Locale;

/**
 * The one definition of result-column identity used by merging, AVG/composite reconstruction and
 * the cube: backticks and whitespace are ignored and case does not matter, matching how Spark echoes
 * an aggregate expression back as a column name.
 */
public final class ColumnNames {

    private ColumnNames() {
    }

    /** Identity key of a column name or aggregate expression: no backticks, no whitespace, upper case. */
    public static String identity(String name) {
        return name.replace("`", "").replaceAll("\\s+", "").toUpperCase(Locale.ROOT);
    }

    /**
     * The SQL identifier a possibly quoted and qualified reference names: {@code t.`SubscriberID`}
     * and {@code "subscriberid"} both become {@code subscriberid}.
     */
    public static String unqualified(String reference) {
        String cleaned = reference.replace("`", "").replace("\"", "").trim();
        return cleaned.substring(cleaned.lastIndexOf('.') + 1).toLowerCase(Locale.ROOT);
    }

    /**
     * The frame column {@code token} refers to: an exact match first, otherwise the single column with
     * the same {@link #identity}. Returns {@code null} when none matches and throws when several do.
     */
    public static String resolve(String token, List<String> columns) {
        if (columns.contains(token)) {
            return token;
        }
        String wanted = identity(token);
        String resolved = null;
        for (String column : columns) {
            if (identity(column).equals(wanted)) {
                if (resolved != null) {
                    throw new IllegalArgumentException("Ambiguous result column: " + token);
                }
                resolved = column;
            }
        }
        return resolved;
    }
}
