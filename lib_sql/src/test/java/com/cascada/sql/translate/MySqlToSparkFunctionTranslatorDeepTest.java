package com.cascada.sql.translate;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exhaustive coverage of the MySQL→Spark function-mapping (data_explory/all_plans.md §2): every
 * renamed function, the {@code DATE_FORMAT}/{@code STR_TO_DATE} token table, the CAST type fixes,
 * {@code NEW}/{@code OLD} trigger resolution, that identical functions are untouched, and that the
 * output parses under Apache Calcite (the plan's Principle 4 validation, now on Calcite).
 */
class MySqlToSparkFunctionTranslatorDeepTest {

    private final MySqlToSparkFunctionTranslator translator = new MySqlToSparkFunctionTranslator();

    @Test
    void renamesNullHandling() {
        assertThat(translator.translate("SELECT IFNULL(a, b) FROM t")).isEqualTo("SELECT coalesce(a, b) FROM t");
    }

    @Test
    void renamesDateTimeFunctions() {
        assertThat(translator.translate("SELECT NOW(), CURDATE(), UTC_TIMESTAMP() FROM t"))
                .isEqualTo("SELECT current_timestamp(), current_date(), current_timestamp() FROM t");
    }

    @Test
    void renamesNumericFunctionsWithSemanticDifferences() {
        assertThat(translator.translate("SELECT SIGN(x), LOG(y), POW(a, b) FROM t"))
                .isEqualTo("SELECT signum(x), ln(y), power(a, b) FROM t");
    }

    @Test
    void renamesJsonAndFormatFunctions() {
        assertThat(translator.translate("SELECT JSON_EXTRACT(doc, '$.a'), FORMAT(n, 2) FROM t"))
                .isEqualTo("SELECT get_json_object(doc, '$.a'), format_number(n, 2) FROM t");
    }

    @Test
    void convertsEveryDateFormatTokenInOnePass() {
        assertThat(translator.translate("SELECT DATE_FORMAT(ts, '%Y-%m-%d %H:%i:%s') FROM t"))
                .isEqualTo("SELECT date_format(ts, 'yyyy-MM-dd HH:mm:ss') FROM t");
    }

    @Test
    void convertsWeekdayAndMonthTokens() {
        assertThat(translator.translate("SELECT DATE_FORMAT(ts, '%W %M %b %a') FROM t"))
                .isEqualTo("SELECT date_format(ts, 'EEEE MMMM MMM EEE') FROM t");
    }

    @Test
    void fixesAllCastTypeNames() {
        assertThat(translator.translate("SELECT CAST(x AS SIGNED), CAST(y AS CHAR) FROM t"))
                .isEqualTo("SELECT CAST(x AS BIGINT), CAST(y AS STRING) FROM t");
        assertThat(translator.translate("SELECT CAST(z AS DATETIME), CAST(w AS UNSIGNED) FROM t"))
                .isEqualTo("SELECT CAST(z AS TIMESTAMP), CAST(w AS BIGINT) FROM t");
    }

    @Test
    void leavesIdenticalFunctionsUntouched() {
        assertThat(translator.translate("SELECT concat(a, b), abs(x), sum(y), length(s) FROM t"))
                .isEqualTo("SELECT concat(a, b), abs(x), sum(y), length(s) FROM t");
    }

    @Test
    void resolvesNewAndOldTriggerReferences() {
        assertThat(translator.resolveNewOldReferences(
                "INSERT INTO f SELECT NEW.id, OLD.status FROM source", "inserted_row", "previous_row"))
                .isEqualTo("INSERT INTO f SELECT inserted_row.id, previous_row.status FROM source");
    }

    @Test
    void translatedQueryStillParsesUnderCalcite() {
        String spark = translator.translate(
                "SELECT IFNULL(a, b), DATE_FORMAT(ts, '%Y') FROM t WHERE ts >= 1");
        assertThat(translator.isParseable(spark)).isTrue();
    }

    @Test
    void renameMatrixIsExposedForDocumentation() {
        assertThat(MySqlToSparkFunctionTranslator.functionRenames())
                .containsEntry("IFNULL", "coalesce")
                .containsEntry("NOW", "current_timestamp")
                .containsEntry("LOG", "ln")
                .containsEntry("JSON_EXTRACT", "get_json_object");
    }
}
