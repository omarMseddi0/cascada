package com.cascada.sql.adapter.calcite;

import com.cascada.sql.domain.TimeDimensionMap;
import com.cascada.sql.domain.UnsupportedSqlException;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/** Fails closed when a parsed query cannot be represented by the cache's single-window model. */
final class SqlCacheabilityGuard {

    private static final Set<String> BUILT_IN_AGGREGATES = Set.of("SUM", "COUNT", "AVG", "MIN", "MAX");
    private static final Set<String> CONTEXT_DEPENDENT_FUNCTIONS = Set.of(
            "CURRENT_TIMESTAMP", "CURRENT_TIME", "CURRENT_DATE",
            "LOCALTIME", "LOCALTIMESTAMP", "CURRENT_USER", "SESSION_USER",
            "SYSTEM_USER", "CURRENT_SCHEMA", "CURRENT_CATALOG", "CURRENT_PATH", "USER");

    void validate(SqlNode root, SqlSelect select, TimeDimensionMap dimensions) {
        rejectMultipleTimeColumns(root, dimensions);
        rejectUnsafeFunctions(root);
        rejectUnsupportedTimeGrouping(select, dimensions);
        rejectMultiSourceTimeReferences(root, select.getFrom(), dimensions);
    }

    private void rejectUnsafeFunctions(SqlNode node) {
        if (node == null) {
            return;
        }
        if (isContextDependent(node)) {
            throw new UnsupportedSqlException("context-dependent SQL expressions cannot be cached");
        }
        if (node instanceof SqlNodeList nodeList) {
            for (SqlNode child : nodeList) {
                rejectUnsafeFunctions(child);
            }
            return;
        }
        if (!(node instanceof SqlCall call)) {
            return;
        }
        SqlOperator operator = functionMetadata(call);
        if (!operator.isDeterministic() || operator.isDynamicFunction()) {
            throw new UnsupportedSqlException("non-deterministic SQL expressions cannot be cached");
        }
        for (SqlNode operand : call.getOperandList()) {
            rejectUnsafeFunctions(operand);
        }
    }

    private boolean isContextDependent(SqlNode node) {
        // Calcite leaves some SQL special functions as unquoted identifiers before validation.
        if (node instanceof SqlIdentifier identifier && identifier.isSimple()
                && !identifier.isComponentQuoted(0)) {
            return CONTEXT_DEPENDENT_FUNCTIONS.contains(identifier.getSimple().toUpperCase(Locale.ROOT));
        }
        String kindName = node.getKind().name().toUpperCase(Locale.ROOT);
        if (CONTEXT_DEPENDENT_FUNCTIONS.contains(kindName)) {
            return true;
        }
        return node instanceof SqlCall call
                && CONTEXT_DEPENDENT_FUNCTIONS.contains(call.getOperator().getName().toUpperCase(Locale.ROOT));
    }

    /** Calcite parses unresolved calls as user-defined functions until a catalog validates them. */
    private SqlOperator functionMetadata(SqlCall call) {
        SqlOperator parsedOperator = call.getOperator();
        if (!(parsedOperator instanceof SqlFunction function) || !function.getFunctionType().isUserDefined()) {
            return parsedOperator;
        }

        SqlIdentifier name = function.getSqlIdentifier();
        if (name == null || !name.isSimple()) {
            throw new UnsupportedSqlException("qualified SQL functions are not classified for caching");
        }
        String functionName = name.getSimple().toUpperCase(Locale.ROOT);
        // These aggregate names are SQL syntax, and COUNT(*) does not match the standard table's
        // ordinary function arity. Their merge behavior is separately constrained by the factory.
        if (BUILT_IN_AGGREGATES.contains(functionName)) {
            return parsedOperator;
        }

        for (SqlOperator standard : SqlStdOperatorTable.instance().getOperatorList()) {
            if (standard.getName().equalsIgnoreCase(name.getSimple())
                    && standard.getSyntax() == function.getSyntax()
                    && standard.getOperandCountRange().isValidCount(call.getOperandList().size())) {
                return standard;
            }
        }
        throw new UnsupportedSqlException("user-defined SQL functions are not classified for caching");
    }

    private void rejectUnsupportedTimeGrouping(SqlSelect select, TimeDimensionMap dimensions) {
        SqlNodeList group = select.getGroup();
        if (group == null) {
            return;
        }
        for (SqlNode expression : group) {
            if (expression instanceof SqlIdentifier identifier && isTimeColumn(identifier, dimensions)) {
                continue;
            }
            if (CanonicalTimeSeriesAnalyzer.floorBucketStep(expression, dimensions).isPresent()) {
                continue;
            }
            if (referencesTimeColumn(expression, dimensions)) {
                throw new UnsupportedSqlException("unsupported time-column grouping expression");
            }
        }
    }

    private void rejectMultipleTimeColumns(SqlNode root, TimeDimensionMap dimensions) {
        Set<String> referencedColumns = new HashSet<>();
        collectTimeColumnReferences(root, dimensions, referencedColumns);
        if (referencedColumns.size() > 1) {
            throw new UnsupportedSqlException("queries referencing multiple time columns cannot be cached");
        }
    }

    private void rejectMultiSourceTimeReferences(SqlNode root, SqlNode from, TimeDimensionMap dimensions) {
        if (countTableSources(from) > 1 && referencesTimeColumn(root, dimensions)) {
            throw new UnsupportedSqlException(
                    "queries referencing time columns across multiple sources cannot be cached");
        }
    }

    private int countTableSources(SqlNode from) {
        if (from == null) {
            return 0;
        }
        if (from instanceof SqlIdentifier) {
            return 1;
        }
        if (from instanceof SqlSelect select) {
            return countTableSources(select.getFrom());
        }
        if (from instanceof SqlNodeList nodeList) {
            int count = 0;
            for (SqlNode child : nodeList) {
                count += countTableSources(child);
            }
            return count;
        }
        if (from instanceof SqlCall call) {
            if (call.getKind() == org.apache.calcite.sql.SqlKind.AS && call instanceof SqlBasicCall asCall) {
                return countTableSources(asCall.operand(0));
            }
            if (call instanceof org.apache.calcite.sql.SqlJoin join) {
                return countTableSources(join.getLeft()) + countTableSources(join.getRight());
            }
            return call.getOperandList().stream().mapToInt(this::countTableSources).sum();
        }
        return 0;
    }

    private boolean referencesTimeColumn(SqlNode node, TimeDimensionMap dimensions) {
        if (node == null) {
            return false;
        }
        if (node instanceof SqlIdentifier identifier) {
            return isTimeColumn(identifier, dimensions);
        }
        if (node instanceof SqlNodeList nodeList) {
            for (SqlNode child : nodeList) {
                if (referencesTimeColumn(child, dimensions)) {
                    return true;
                }
            }
        } else if (node instanceof SqlCall call) {
            for (SqlNode operand : call.getOperandList()) {
                if (referencesTimeColumn(operand, dimensions)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void collectTimeColumnReferences(SqlNode node, TimeDimensionMap dimensions, Set<String> collector) {
        if (node == null) {
            return;
        }
        if (node instanceof SqlIdentifier identifier) {
            if (isTimeColumn(identifier, dimensions)) {
                collector.add(simpleName(identifier).toLowerCase(Locale.ROOT));
            }
            return;
        }
        if (node instanceof SqlNodeList nodeList) {
            for (SqlNode child : nodeList) {
                collectTimeColumnReferences(child, dimensions, collector);
            }
        } else if (node instanceof SqlCall call) {
            if (call.getKind() == org.apache.calcite.sql.SqlKind.AS && call instanceof SqlBasicCall asCall) {
                collectTimeColumnReferences(asCall.operand(0), dimensions, collector);
                return;
            }
            for (SqlNode operand : call.getOperandList()) {
                collectTimeColumnReferences(operand, dimensions, collector);
            }
        }
    }

    private boolean isTimeColumn(SqlIdentifier identifier, TimeDimensionMap dimensions) {
        return dimensions.isTimeColumn(simpleName(identifier));
    }

    private String simpleName(SqlIdentifier identifier) {
        return identifier.names.get(identifier.names.size() - 1);
    }
}
