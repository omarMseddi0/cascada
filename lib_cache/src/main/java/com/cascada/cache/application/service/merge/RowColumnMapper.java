package com.cascada.cache.application.service.merge;

import com.cascada.cache.application.service.merge.columnar.DictionaryEncoder;
import com.cascada.cache.application.service.merge.columnar.GroupedResult;
import java.util.Map;
import java.util.TreeMap;

/** Shared row-to-column encoding; callers retain ownership of their primitive storage. */
final class RowColumnMapper {
    private RowColumnMapper() { }

    static void encodeDimensions(Map<String, String> values, String[] names, int[][] codes,
                                 DictionaryEncoder dictionary, int row) {
        for (int column = 0; column < names.length; column++) {
            String value = values.get(names[column]);
            codes[column][row] = value == null ? DictionaryEncoder.ABSENT : dictionary.encode(value);
        }
    }

    static void encodeMeasures(Map<String, Double> values, String[] names, double[][] measures,
                               boolean[][] present, int row) {
        for (int column = 0; column < names.length; column++) {
            Double value = values.get(names[column]);
            if (value != null) {
                measures[column][row] = value;
                present[column][row] = true;
            }
        }
    }

    static Map<String, String> decodeDimensions(GroupedResult grouped, int group, String[] names,
                                                 DictionaryEncoder dictionary) {
        Map<String, String> values = new TreeMap<>();
        for (int column = 0; column < names.length; column++) {
            int code = grouped.dimensionCode(group, column);
            if (code != DictionaryEncoder.ABSENT) values.put(names[column], dictionary.decode(code));
        }
        return values;
    }

    static Map<String, Double> decodeMeasures(GroupedResult grouped, int group, String[] names) {
        Map<String, Double> values = new TreeMap<>();
        for (int column = 0; column < names.length; column++) {
            if (grouped.measurePresent()[column][group]) {
                values.put(names[column], grouped.measureAccumulators()[column][group]);
            }
        }
        return values;
    }
}
