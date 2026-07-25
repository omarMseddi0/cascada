package com.cascada.sql.translate;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the MySQL→Spark function-mapping table (data_explory/all_plans.md §2), the DATE_FORMAT
 * format-string conversion, CAST type fixes, and NEW/OLD trigger resolution — and that the output
 * still parses (Principle 4).
 */
class MySqlToSparkFunctionTranslatorTest {

    private final MySqlToSparkFunctionTranslator translator = new MySqlToSparkFunctionTranslator();

    @Test
    void renamesNullHandlingAndDateTimeFunctions() {
        assertThat(translator.translate("SELECT IFNULL(a, b) FROM t"))
                .isEqualTo("SELECT coalesce(a, b) FROM t");
        assertThat(translator.translate("SELECT NOW(), CURDATE() FROM t"))
                .isEqualTo("SELECT current_timestamp(), current_date() FROM t");
    }

    @Test
    void renamesNumericFunctionsWithSemanticDifferences() {
        assertThat(translator.translate("SELECT SIGN(x), LOG(y) FROM t"))
                .isEqualTo("SELECT signum(x), ln(y) FROM t");
    }

    @Test
    void convertsDateFormatFormatStrings() {
        assertThat(translator.translate("SELECT DATE_FORMAT(ts, '%Y-%m-%d %H:%i:%s') FROM t"))
                .isEqualTo("SELECT date_format(ts, 'yyyy-MM-dd HH:mm:ss') FROM t");
    }

    @Test
    void fixesCastTypeNames() {
        assertThat(translator.translate("SELECT CAST(x AS SIGNED), CAST(y AS CHAR) FROM t"))
                .isEqualTo("SELECT CAST(x AS BIGINT), CAST(y AS STRING) FROM t");
        assertThat(translator.translate("SELECT CAST(z AS DATETIME) FROM t"))
                .isEqualTo("SELECT CAST(z AS TIMESTAMP) FROM t");
    }

    @Test
    void leavesIdenticalFunctionsUntouched() {
        assertThat(translator.translate("SELECT concat(a, b), abs(x), sum(y) FROM t"))
                .isEqualTo("SELECT concat(a, b), abs(x), sum(y) FROM t");
    }

    @Test
    void resolvesNewAndOldTriggerReferences() {
        String resolved = translator.resolveNewOldReferences(
                "INSERT INTO f SELECT NEW.id, OLD.status FROM source", "inserted_row", "previous_row");
        assertThat(resolved).isEqualTo("INSERT INTO f SELECT inserted_row.id, previous_row.status FROM source");
    }

    @Test
    void translatedQueryStillParses() {
        String spark = translator.translate("SELECT IFNULL(a, b), DATE_FORMAT(ts, '%Y') FROM t WHERE ts >= 1");
        assertThat(translator.isParseable(spark)).isTrue();
    }

    @Test
    void exposesTheRenameMatrixForDocumentationAndCoverage() {
        assertThat(MySqlToSparkFunctionTranslator.functionRenames())
                .containsEntry("IFNULL", "coalesce")
                .containsEntry("NOW", "current_timestamp")
                .containsEntry("LOG", "ln");
    }
}
