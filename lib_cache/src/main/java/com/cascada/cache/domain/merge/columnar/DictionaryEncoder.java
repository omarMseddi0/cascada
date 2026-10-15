package com.cascada.cache.domain.merge.columnar;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Dictionary-encodes dimension strings to dense int codes, the same trick Spark/Presto use before
 * hash aggregation: the per-row hot loop then compares and hashes 4-byte ints instead of strings.
 * Encoding cost is paid once per <em>distinct</em> value, not once per row.
 *
 * <p>{@link #ABSENT} is a reserved code for "this row has no value for this dimension", so presence
 * participates in group-key equality without a sentinel string that could collide with real data.
 */
public final class DictionaryEncoder {

    /** Reserved code meaning "no value present" — never returned by {@link #encode}. */
    public static final int ABSENT = -1;

    private final Map<String, Integer> codesByValue = new HashMap<>();
    private final List<String> valuesByCode = new ArrayList<>();

    /** Returns the stable code for {@code value}, assigning the next dense code on first sight. */
    public int encode(String value) {
        Integer existing = codesByValue.get(value);
        if (existing != null) {
            return existing;
        }
        int code = valuesByCode.size();
        codesByValue.put(value, code);
        valuesByCode.add(value);
        return code;
    }

    /** The original string for a code produced by {@link #encode}. */
    public String decode(int code) {
        return valuesByCode.get(code);
    }

    public int size() {
        return valuesByCode.size();
    }
}
