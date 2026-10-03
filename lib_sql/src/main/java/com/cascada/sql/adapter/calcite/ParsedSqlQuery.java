package com.cascada.sql.adapter.calcite;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;

record ParsedSqlQuery(SqlSelect select, SqlOrderBy orderBy) {

    SqlNode root() {
        return orderBy == null ? select : orderBy;
    }

    SqlNode fetch() {
        return orderBy == null ? select.getFetch() : orderBy.fetch;
    }

    SqlNodeList orderList() {
        return orderBy == null ? select.getOrderList() : orderBy.orderList;
    }
}
