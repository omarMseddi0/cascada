package com.cascada.cache.domain.merge;

import com.cascada.cache.domain.frame.ColumnType;

/** Where one AVG's ingredients sit in a merged frame, and its Spark input and result types. */
record AverageColumn(String alias, int sumColumn, int countColumn, String inputType, ColumnType resultType) {
}
