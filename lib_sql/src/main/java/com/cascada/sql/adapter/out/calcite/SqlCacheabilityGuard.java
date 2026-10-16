package com.cascada.sql.adapter.out.calcite;

import com.cascada.sql.domain.TimeDimensionMap;
import com.cascada.sql.exception.UnsupportedSqlException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/** Fails closed when a parsed query cannot be represented by the cache's single-window model. */
final class SqlCacheabilityGuard {

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
        if (com.cascada.cache.domain.merge.AggregateFunction.fromSqlName(functionName).isPresent()) {
            return parsedOperator;
        }

        return SqlStdOperatorTable.instance().getOperatorList().stream()
                .filter(standard -> standard.getName().equalsIgnoreCase(name.getSimple()))
                .filter(standard -> standard.getSyntax() == function.getSyntax())
                .filter(standard -> standard.getOperandCountRange().isValidCount(call.getOperandList().size()))
                .findFirst()
                .orElseThrow(() -> new UnsupportedSqlException(
                        "user-defined SQL functions are not classified for caching"));
    }

    private void rejectUnsupportedTimeGrouping(SqlSelect select, TimeDimensionMap dimensions) {
        SqlNodeList group = select.getGroup();
        if (group == null) {
            return;
        }
        for (SqlNode expression : group) {
            if (expression instanceof SqlIdentifier identifier && SqlTrees.isTimeColumn(identifier, dimensions)) {
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
        SqlTrees.forEachColumnReference(root, identifier -> {
            if (SqlTrees.isTimeColumn(identifier, dimensions)) {
                referencedColumns.add(SqlTrees.lastName(identifier).toLowerCase(Locale.ROOT));
            }
        });
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
        if (SqlTrees.containsDerivedSource(from)) {
            throw new UnsupportedSqlException("derived-table time scopes cannot be safely rewritten for caching");
        }
        SqlNode source = SqlTrees.stripAlias(from);
        if (source instanceof SqlIdentifier) {
            return 1;
        }
        if (source instanceof org.apache.calcite.sql.SqlJoin join) {
            return countTableSources(join.getLeft()) + countTableSources(join.getRight());
        }
        if (source instanceof SqlNodeList nodeList) {
            return nodeList.stream().mapToInt(this::countTableSources).sum();
        }
        if (source instanceof SqlCall call) {
            return call.getOperandList().stream().mapToInt(this::countTableSources).sum();
        }
        return 0;
    }

    private boolean referencesTimeColumn(SqlNode node, TimeDimensionMap dimensions) {
        return SqlTrees.anyColumnReference(node, identifier -> SqlTrees.isTimeColumn(identifier, dimensions));
    }
}
