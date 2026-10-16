package com.cascada.sql.adapter.out.calcite;

/**
 * Lexical view of raw SQL text used by text-level rewrites (reserved-word quoting, MySQL function
 * translation): which characters are code rather than string literals, quoted identifiers or comments.
 * A backslash escapes the next character inside {@code '...'} and {@code "..."} strings, as in MySQL,
 * but not inside {@code `...`} identifiers; a doubled quote character always escapes itself.
 */
public final class SqlTextScanner {

    private SqlTextScanner() {
    }

    /** {@code true} at every position that is SQL code; quotes, their contents and comments are not. */
    public static boolean[] codePositions(String sql) {
        boolean[] code = new boolean[sql.length()];
        int position = 0;
        while (position < sql.length()) {
            char current = sql.charAt(position);
            if (current == '\'' || current == '"' || current == '`') {
                position = endOfQuoted(sql, position, current);
            } else if (sql.startsWith("--", position)) {
                while (position < sql.length() && sql.charAt(position) != '\n') {
                    position++;
                }
            } else if (sql.startsWith("/*", position)) {
                int commentEnd = sql.indexOf("*/", position + 2);
                position = commentEnd < 0 ? sql.length() : commentEnd + 2;
            } else {
                code[position++] = true;
            }
        }
        return code;
    }

    private static int endOfQuoted(String sql, int start, char quote) {
        int position = start + 1;
        while (position < sql.length()) {
            char next = sql.charAt(position++);
            if (next == '\\' && quote != '`' && position < sql.length()) {
                position++;
            } else if (next == quote) {
                if (position < sql.length() && sql.charAt(position) == quote) {
                    position++;
                } else {
                    break;
                }
            }
        }
        return position;
    }

    /** The first position at or after {@code index} that is code and not whitespace. */
    public static int skipWhitespace(String sql, boolean[] code, int index) {
        while (index < sql.length() && code[index] && Character.isWhitespace(sql.charAt(index))) {
            index++;
        }
        return index;
    }

    public static boolean isIdentifierStart(char value) {
        return Character.isLetter(value) || value == '_';
    }

    public static boolean isIdentifierPart(char value) {
        return Character.isLetterOrDigit(value) || value == '_';
    }
}
