package com.cascada.sql.calcite;

import com.cascada.sql.canonical.UnsupportedSqlException;
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
    public static final SqlDialect SPARK_DIALECT = SparkSqlDialect.DEFAULT;

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
            throw new UnsupportedSqlException("SQL did not parse; bypassing cache", notParseable);
        }
    }

    /** Parse a stand-alone expression (e.g. a WHERE predicate) into a node. */
    public static SqlNode parseExpression(String expression) {
        try {
            return SqlParser.create(expression, PARSER_CONFIG).parseExpression();
        } catch (SqlParseException notParseable) {
            throw new UnsupportedSqlException("expression did not parse: " + expression, notParseable);
        }
    }

    /** True iff the SQL parses as a query — the Calcite analogue of the old {@code isParseable} check. */
    public static boolean isParseable(String sql) {
        try {
            SqlParser.create(sql, PARSER_CONFIG).parseStmt();
            return true;
        } catch (SqlParseException notParseable) {
            return false;
        }
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
        ).getSql().replace('\n', ' ').replaceAll("\\s+", " ").trim();
    }
}
