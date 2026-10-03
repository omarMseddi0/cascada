package com.cascada.sql.adapter.calcite;

import com.cascada.sql.domain.UnsupportedSqlException;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.SparkSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

/**
 * The single Apache Calcite entry point for the SQL library — the Java analogue of
 * {@code sqlglot.parse_one(...)} / {@code ast.sql(dialect="spark")} used throughout the Python
 * {@code smart_cache} module.
 *
 * <p>Calcite replaces JSqlParser as the parsing/AST engine because, like sqlglot, it exposes a
 * full, navigable, transformable {@code SqlNode} tree and a dialect-aware unparser. Two services are
 * offered:
 *
 * <ul>
 *   <li>{@link #parseQuery(String)} — parse a {@code SELECT} (optionally wrapped in {@code ORDER BY /
 *       LIMIT}) into a {@code SqlNode}, using MySQL lexing (back-tick quoting, case-insensitive,
 *       case-preserving) and the lenient conformance so the customer's MySQL-flavoured SQL parses;</li>
 *   <li>{@link #parseExpression(String)} — parse a stand-alone boolean/scalar expression, the analogue
 *       of {@code sqlglot.parse_one("a >= 1 AND a <= 2")};</li>
 *   <li>{@link #unparse(SqlNode)} — render a node back to Spark SQL on a single line, quoting an
 *       identifier only when it truly needs it. Single-line, minimally-quoted output is required so the
 *       gap predicate stays {@code ts >= N AND ts <= M} (the cache-correctness oracle reads that shape).</li>
 * </ul>
 *
 * <p>Anything Calcite cannot parse is surfaced as {@link UnsupportedSqlException} so the caller
 * bypasses to Spark rather than caching a guess — identical to the Python try/except-and-bypass guard.
 */
public final class CalciteSql {

    /** Spark is the physical execution dialect; all generated SQL targets it. */
    public static final SqlDialect SPARK_DIALECT = new SparkSqlDialect(SparkSqlDialect.DEFAULT_CONTEXT.withIdentifierQuoteString("`")) {
        @Override
        protected boolean identifierNeedsQuote(String identifier) {
            return !identifier.matches("[A-Za-z_][A-Za-z0-9_]*") || super.identifierNeedsQuote(identifier);
        }
    };

    /**
     * MySQL lexing matches the customer's input dialect: back-tick quoting, case-insensitive name
     * resolution, but identifiers keep their original casing on the way out (so {@code appName} stays
     * {@code appName}). LENIENT conformance tolerates the looser grammar real queries use.
     */
    private static final SqlParser.Config PARSER_CONFIG = SqlParser.config()
            .withLex(Lex.MYSQL)
            .withConformance(SqlConformanceEnum.LENIENT);

    private CalciteSql() {
    }

    /** Parse a {@code SELECT} (possibly wrapped by {@code ORDER BY}/{@code LIMIT}) into a node. */
    public static SqlNode parseQuery(String sql) {
        try {
            return SqlParser.create(sql, PARSER_CONFIG).parseQuery();
        } catch (SqlParseException notParseable) {
            String quotedReservedIdentifier = quoteReservedTimestampIdentifiers(sql);
            if (!quotedReservedIdentifier.equals(sql)) {
                try {
                    return SqlParser.create(quotedReservedIdentifier, PARSER_CONFIG).parseQuery();
                } catch (SqlParseException stillNotParseable) {
                    // Preserve the original parser diagnostics below.
                }
            }
            throw new UnsupportedSqlException("SQL did not parse; bypassing cache", notParseable);
        }
    }

    /** Parse a stand-alone expression (e.g. a WHERE predicate) into a node. */
    public static SqlNode parseExpression(String expression) {
        try {
            return SqlParser.create(expression, PARSER_CONFIG).parseExpression();
        } catch (SqlParseException notParseable) {
            String quotedReservedIdentifier = quoteReservedTimestampIdentifiers(expression);
            if (!quotedReservedIdentifier.equals(expression)) {
                try {
                    return SqlParser.create(quotedReservedIdentifier, PARSER_CONFIG).parseExpression();
                } catch (SqlParseException stillNotParseable) {
                    // Preserve the original parser diagnostics below.
                }
            }
            throw new UnsupportedSqlException("expression did not parse: " + expression, notParseable);
        }
    }

    /** True iff the SQL parses as a query — the Calcite analogue of the old {@code isParseable} check. */
    public static boolean isParseable(String sql) {
        try {
            SqlParser.create(sql, PARSER_CONFIG).parseStmt();
            return true;
        } catch (SqlParseException notParseable) {
            String quotedReservedIdentifier = quoteReservedTimestampIdentifiers(sql);
            if (quotedReservedIdentifier.equals(sql)) {
                return false;
            }
            try {
                SqlParser.create(quotedReservedIdentifier, PARSER_CONFIG).parseStmt();
                return true;
            } catch (SqlParseException stillNotParseable) {
                return false;
            }
        }
    }

    /** Calcite's MySQL lexer treats TIMESTAMP as a reserved type keyword, including in column refs. */
    private static String quoteReservedTimestampIdentifiers(String sql) {
        StringBuilder result = new StringBuilder(sql.length());
        int index = 0;
        while (index < sql.length()) {
            char current = sql.charAt(index);
            if (current == '\'' || current == '"' || current == '`') {
                index = copyQuoted(sql, index, result, current);
                continue;
            }
            if (sql.startsWith("--", index)) {
                index = copyLineComment(sql, index, result);
                continue;
            }
            if (sql.startsWith("/*", index)) {
                index = copyBlockComment(sql, index, result);
                continue;
            }
            if (isIdentifierStart(current)) {
                int end = index + 1;
                while (end < sql.length() && isIdentifierPart(sql.charAt(end))) {
                    end++;
                }
                String word = sql.substring(index, end);
                if (word.equalsIgnoreCase("timestamp") && shouldQuoteTimestamp(sql, index, end)) {
                    result.append('`').append(word).append('`');
                } else {
                    result.append(word);
                }
                index = end;
                continue;
            }
            result.append(current);
            index++;
        }
        return result.toString();
    }

    private static boolean shouldQuoteTimestamp(String sql, int start, int end) {
        int next = skipWhitespace(sql, end);
        if (next < sql.length() && (sql.charAt(next) == '\'' || sql.charAt(next) == '(')) {
            return false;
        }
        int previous = previousWordStart(sql, start);
        if (previous >= 0 && sql.substring(previous, start).trim().equalsIgnoreCase("AS")
                && next < sql.length() && sql.charAt(next) == ')') {
            return false;
        }
        return true;
    }

    private static int copyQuoted(String sql, int start, StringBuilder result, char quote) {
        int index = start;
        result.append(sql.charAt(index++));
        while (index < sql.length()) {
            char current = sql.charAt(index++);
            result.append(current);
            if (current == '\\' && quote != '`' && index < sql.length()) {
                result.append(sql.charAt(index++));
            } else if (current == quote) {
                if (index < sql.length() && sql.charAt(index) == quote) {
                    result.append(sql.charAt(index++));
                } else {
                    break;
                }
            }
        }
        return index;
    }

    private static int copyLineComment(String sql, int start, StringBuilder result) {
        int index = start;
        while (index < sql.length()) {
            char current = sql.charAt(index++);
            result.append(current);
            if (current == '\n') {
                break;
            }
        }
        return index;
    }

    private static int copyBlockComment(String sql, int start, StringBuilder result) {
        int index = start;
        while (index < sql.length()) {
            char current = sql.charAt(index++);
            result.append(current);
            if (current == '*' && index < sql.length() && sql.charAt(index) == '/') {
                result.append(sql.charAt(index++));
                break;
            }
        }
        return index;
    }

    private static int skipWhitespace(String sql, int index) {
        while (index < sql.length() && Character.isWhitespace(sql.charAt(index))) {
            index++;
        }
        return index;
    }

    private static int previousWordStart(String sql, int end) {
        int index = end - 1;
        while (index >= 0 && Character.isWhitespace(sql.charAt(index))) {
            index--;
        }
        int wordEnd = index + 1;
        while (index >= 0 && isIdentifierPart(sql.charAt(index))) {
            index--;
        }
        return wordEnd == index + 1 ? -1 : index + 1;
    }

    private static boolean isIdentifierStart(char value) {
        return Character.isLetter(value) || value == '_';
    }

    private static boolean isIdentifierPart(char value) {
        return Character.isLetterOrDigit(value) || value == '_';
    }

    /** Render a node to single-line Spark SQL, quoting identifiers only when strictly necessary. */
    public static String unparse(SqlNode node) {
        return node.toSqlString(config -> config
                .withDialect(SPARK_DIALECT)
                .withQuoteAllIdentifiers(false)
                .withClauseStartsLine(false)
                .withSelectListItemsOnSeparateLines(false)
                .withUpdateSetListNewline(false)
                .withIndentation(0)
                .withLineFolding(org.apache.calcite.sql.SqlWriterConfig.LineFolding.WIDE)
        ).getSql().trim();
    }
}

