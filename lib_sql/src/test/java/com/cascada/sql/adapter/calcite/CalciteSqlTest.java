package com.cascada.sql.calcite;

import com.cascada.sql.canonical.UnsupportedSqlException;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Aggressive coverage of the Apache Calcite facade that replaces sqlglot's {@code parse_one} /
 * {@code .sql(dialect="spark")}: parsing, single-line minimally-quoted unparsing, expression parsing,
 * the parseability probe, and the bypass-on-unparseable contract.
 */
class CalciteSqlTest {

    @Test
    void parsesAPlainSelectIntoASelectNode() {
        SqlNode node = CalciteSql.parseQuery("SELECT a, b FROM t WHERE a >= 1");
        assertThat(node).isInstanceOf(SqlSelect.class);
    }

    @Test
    void unparseIsSingleLineWithoutNewlines() {
        String sql = CalciteSql.unparse(CalciteSql.parseQuery(
                "SELECT appName, SUM(bytes) AS b FROM traffic WHERE ts >= 0 AND ts <= 100 GROUP BY appName"));
        assertThat(sql).doesNotContain("\n");
        assertThat(sql).isEqualTo(
                "SELECT appName, SUM(bytes) b FROM traffic WHERE ts >= 0 AND ts <= 100 GROUP BY appName");
    }

    @Test
    void unparseKeepsTimePredicateInOracleReadableShape() {
        // The cache-correctness oracle reads `ts >= N AND ts <= M` from the gap SQL — it must not be quoted.
        String sql = CalciteSql.unparse(CalciteSql.parseExpression("ts >= 0 AND ts <= 86399"));
        assertThat(sql).isEqualTo("ts >= 0 AND ts <= 86399");
    }

    @Test
    void unparseDoesNotQuoteSimpleMixedCaseIdentifiers() {
        String sql = CalciteSql.unparse(CalciteSql.parseExpression("deviceType = 'mobile'"));
        assertThat(sql).isEqualTo("deviceType = 'mobile'");
    }

    @Test
    void parseExpressionParsesABooleanPredicate() {
        assertThat(CalciteSql.unparse(CalciteSql.parseExpression("a > 1 OR b < 2")))
                .isEqualTo("a > 1 OR b < 2");
    }

    @Test
    void isParseableIsTrueForValidSqlAndFalseForGarbage() {
        assertThat(CalciteSql.isParseable("SELECT coalesce(a, b) FROM t WHERE ts >= 1")).isTrue();
        assertThat(CalciteSql.isParseable("NOT SQL AT ALL ;;;")).isFalse();
    }

    @Test
    void parseQueryBypassesUnparseableSql() {
        assertThatThrownBy(() -> CalciteSql.parseQuery("this is not sql"))
                .isInstanceOf(UnsupportedSqlException.class);
    }

    @Test
    void parseExpressionBypassesAnUnparseableExpression() {
        assertThatThrownBy(() -> CalciteSql.parseExpression(") ( malformed"))
                .isInstanceOf(UnsupportedSqlException.class);
    }
}
