package com.cascada.cache.application.service.merge;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.frame.ResultFrameBuilder;
import com.cascada.cache.domain.merge.AverageIngredients;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Rebuilds {@code AVG(x) AS alias} from merged SUM, COUNT and {@code typeof(x)} ingredients with
 * Spark's result rules: DOUBLE for integral and floating input, and for {@code decimal(p,s)} input a
 * {@code decimal(p+4,s+4)} computed as Spark does — the sum (scale s) divided by the count as
 * {@code decimal(20,0)}, rounded HALF_UP to Spark's division scale and then to the result scale.
 * A COUNT of zero yields NULL, as in SQL.
 */
public final class SparkAverage {

    private static final Pattern DECIMAL_TYPE = Pattern.compile("decimal\\((\\d+),\\s*(\\d+)\\)");
    private static final int MAX_PRECISION = 38;
    private static final int MINIMUM_ADJUSTED_SCALE = 6;
    /** Spark's Decimal division keeps MAX_SCALE + 1 digits before the result type is applied. */
    private static final int DIVISION_WORKING_SCALE = MAX_PRECISION + 1;
    /** COUNT is cast to decimal(20,0) before the division. */
    private static final int COUNT_PRECISION = 20;

    private SparkAverage() {
    }

    /** Replaces each alias's three ingredient columns with the alias, at the SUM column's position. */
    public static ResultFrame finish(ResultFrame merged, List<String> aliases) {
        if (aliases.isEmpty()) {
            return merged;
        }
        Map<Integer, AverageColumn> averageAtSumColumn = new HashMap<>();
        Set<Integer> droppedColumns = new HashSet<>();
        for (String alias : aliases) {
            int typeColumn = merged.columnIndex(AverageIngredients.typeColumn(alias));
            String inputType = inputType(merged, typeColumn);
            AverageColumn average = new AverageColumn(alias,
                    merged.columnIndex(AverageIngredients.sumColumn(alias)),
                    merged.columnIndex(AverageIngredients.countColumn(alias)),
                    inputType, merged.rowCount() == 0
                            && merged.columnTypeAt(merged.columnIndex(AverageIngredients.sumColumn(alias))) == ColumnType.DECIMAL
                            ? ColumnType.DECIMAL : resultType(inputType));
            averageAtSumColumn.put(average.sumColumn(), average);
            droppedColumns.add(average.countColumn());
            droppedColumns.add(typeColumn);
        }

        ResultFrameBuilder builder = ResultFrame.builder().expectedRows(merged.rowCount());
        List<Integer> outputColumns = new ArrayList<>();
        for (int column = 0; column < merged.columnNames().size(); column++) {
            if (droppedColumns.contains(column)) {
                continue;
            }
            outputColumns.add(column);
            AverageColumn average = averageAtSumColumn.get(column);
            if (average == null) {
                builder.column(merged.columnNames().get(column), merged.columnTypeAt(column));
            } else {
                builder.column(average.alias(), average.resultType());
            }
        }

        for (int row = 0; row < merged.rowCount(); row++) {
            for (int column : outputColumns) {
                AverageColumn average = averageAtSumColumn.get(column);
                if (average == null) {
                    builder.appendCell(merged, row, column);
                } else {
                    appendAverage(builder, merged, row, average);
                }
            }
        }
        return builder.build();
    }

    private static void appendAverage(ResultFrameBuilder builder, ResultFrame merged, int row, AverageColumn average) {
        long count = merged.isNullAt(row, average.countColumn()) ? 0L : merged.longAt(row, average.countColumn());
        if (count < 0) {
            throw new IllegalArgumentException("AVG count ingredient cannot be negative");
        }
        if (count == 0 || merged.isNullAt(row, average.sumColumn())) {
            builder.appendNull();
            return;
        }
        Object sum = merged.valueAt(row, average.sumColumn());
        if (average.resultType() == ColumnType.DECIMAL) {
            builder.appendDecimal(decimalAverage((BigDecimal) sum, count, average.inputType()));
        } else {
            builder.appendDouble(((Number) sum).doubleValue() / count);
        }
    }

    /** Spark's AVG over decimal(p,s); null on overflow of decimal(p+4,s+4), as with ANSI mode off. */
    static BigDecimal decimalAverage(BigDecimal sum, long count, String inputType) {
        Matcher decimal = DECIMAL_TYPE.matcher(inputType);
        if (!decimal.matches()) throw new IllegalArgumentException("not a decimal type: " + inputType);
        int precision = Integer.parseInt(decimal.group(1));
        int scale = Integer.parseInt(decimal.group(2));
        int sumPrecision = Math.min(precision + 10, MAX_PRECISION);
        int resultPrecision = Math.min(precision + 4, MAX_PRECISION);
        int resultScale = Math.min(scale + 4, MAX_PRECISION);

        // Divide(decimal(sumPrecision, scale), decimal(20,0)) with spark.sql.decimalOperations.allowPrecisionLoss.
        int divisionScale = Math.max(MINIMUM_ADJUSTED_SCALE, scale + COUNT_PRECISION + 1);
        int divisionPrecision = sumPrecision - scale + divisionScale;
        if (divisionPrecision > MAX_PRECISION) {
            int integerDigits = divisionPrecision - divisionScale;
            divisionScale = Math.max(MAX_PRECISION - integerDigits, Math.min(divisionScale, MINIMUM_ADJUSTED_SCALE));
        }
        BigDecimal average = sum.divide(BigDecimal.valueOf(count), DIVISION_WORKING_SCALE, RoundingMode.HALF_UP)
                .setScale(divisionScale, RoundingMode.HALF_UP)
                .setScale(resultScale, RoundingMode.HALF_UP);
        return average.precision() - average.scale() > resultPrecision - resultScale ? null : average;
    }

    private static String inputType(ResultFrame merged, int typeColumn) {
        for (int row = 0; row < merged.rowCount(); row++) {
            if (!merged.isNullAt(row, typeColumn)) return merged.stringAt(row, typeColumn).toLowerCase(Locale.ROOT);
        }
        return "double";
    }

    private static ColumnType resultType(String inputType) {
        if (DECIMAL_TYPE.matcher(inputType).matches()) return ColumnType.DECIMAL;
        return switch (inputType) {
            case "tinyint", "smallint", "int", "bigint", "float", "double" -> ColumnType.DOUBLE;
            default -> throw new IllegalArgumentException("AVG over " + inputType + " is not rebuilt from cache");
        };
    }
}
