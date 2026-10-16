package com.cascada.sql.adapter.out.calcite;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;

/** One Calcite parse entry point (query, expression or statement), retried by {@link CalciteSql}. */
@FunctionalInterface
interface CalciteParse {
    SqlNode parse(String text) throws SqlParseException;
}
