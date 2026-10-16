package com.cascada.sql.adapter.out.calcite;

import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;

/** A recognised epoch bucket: the time column, its step in seconds, and the CAST type if it had one. */
record TimeBucket(SqlIdentifier column, int step, SqlDataTypeSpec castType) { }
