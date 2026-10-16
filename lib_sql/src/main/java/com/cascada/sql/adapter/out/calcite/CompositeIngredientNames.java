package com.cascada.sql.adapter.out.calcite;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** Stable private names keep intermediate aggregates separate from user-selected aliases. */
final class CompositeIngredientNames {
    private CompositeIngredientNames() { }
    static String alias(String expression) {
        try {
            return "__cascada_component_" + HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(expression.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }
}
