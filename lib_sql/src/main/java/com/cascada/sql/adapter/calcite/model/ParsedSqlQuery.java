package com.cascada.sql.adapter.calcite.model;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;

public record ParsedSqlQuery(SqlSelect select, SqlOrderBy orderBy) {

    public SqlNode root() {
        return orderBy == null ? select : orderBy;
    }

    public SqlNode fetch() {
        return orderBy == null ? select.getFetch() : orderBy.fetch;
    }

    public SqlNodeList orderList() {
        return orderBy == null ? select.getOrderList() : orderBy.orderList;
    }
}
