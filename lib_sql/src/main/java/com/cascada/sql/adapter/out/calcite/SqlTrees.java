package com.cascada.sql.adapter.out.calcite;

import com.cascada.cache.domain.merge.AggregateFunction;
import com.cascada.sql.domain.model.TimeDimensionMap;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;

/**
 * The Calcite tree questions every cache SQL component asks — which SELECTs a query contains, whether
 * a FROM reads a derived relation, what a projection is called, which identifiers are time columns —
 * answered once, so canonicalisation, the cacheability guard and the gap rewriter cannot disagree.
 */
final class SqlTrees {

    static final Set<SqlKind> SET_OPERATIONS = EnumSet.of(SqlKind.UNION, SqlKind.INTERSECT, SqlKind.EXCEPT);
    static final Set<SqlKind> ARITHMETIC = EnumSet.of(SqlKind.PLUS, SqlKind.MINUS, SqlKind.TIMES,
            SqlKind.DIVIDE, SqlKind.MINUS_PREFIX, SqlKind.PLUS_PREFIX);

    private SqlTrees() {
    }

    static boolean isSetOperation(SqlNode node) {
        return node != null && SET_OPERATIONS.contains(node.getKind());
    }

    /** {@code SUM}, {@code COUNT}, {@code AVG}, {@code MIN} or {@code MAX}, with or without DISTINCT. */
    static boolean isAggregateCall(SqlNode node) {
        return node instanceof SqlBasicCall call
                && AggregateFunction.fromSqlName(call.getOperator().getName()).isPresent();
    }

    /** {@code relation AS alias} (with or without a column list) is read as {@code relation}. */
    static SqlNode stripAlias(SqlNode node) {
        if (node != null && node.getKind() == SqlKind.AS && node instanceof SqlCall call) {
            return call.operand(0);
        }
        return node;
    }

    /** The unqualified name an identifier refers to ({@code t.ts} names {@code ts}). */
    static String lastName(SqlIdentifier identifier) {
        return identifier.names.get(identifier.names.size() - 1);
    }

    static boolean isTimeColumn(SqlIdentifier identifier, TimeDimensionMap dimensions) {
        return dimensions.isTimeColumn(lastName(identifier));
    }

    /** The projection item without its alias. */
    static SqlNode unaliased(SqlNode item) {
        return item.getKind() == SqlKind.AS && item instanceof SqlCall as ? as.operand(0) : item;
    }

    /**
     * The result-column name a projection item produces: its alias, the last name of a plain column,
     * otherwise the expression text.
     */
    static String outputName(SqlNode item) {
        if (item.getKind() == SqlKind.AS && item instanceof SqlCall as && as.operand(1) instanceof SqlIdentifier alias) {
            return alias.getSimple();
        }
        if (item instanceof SqlIdentifier identifier) {
            return lastName(identifier);
        }
        return CalciteSql.unparse(item);
    }

    /** Every SELECT reachable through query roots, set operations, ORDER BY, WITH and FROM clauses. */
    static List<SqlSelect> allSelects(SqlNode root) {
        List<SqlSelect> selects = new ArrayList<>();
        collectSelects(root, selects);
        return selects;
    }

    private static void collectSelects(SqlNode node, List<SqlSelect> collector) {
        if (node == null) {
            return;
        }
        if (node instanceof SqlSelect select) {
            collector.add(select);
            collectSelects(select.getFrom(), collector);
            return;
        }
        if (node instanceof SqlOrderBy orderBy) {
            collectSelects(orderBy.query, collector);
            return;
        }
        if (node instanceof SqlNodeList list) {
            list.forEach(child -> collectSelects(child, collector));
            return;
        }
        if (node instanceof SqlCall call) {
            for (SqlNode operand : call.getOperandList()) {
                collectSelects(operand, collector);
            }
        }
    }

    /**
     * True when a FROM clause reads a derived relation anywhere, including either side of a join:
     * a subquery, an ordered query, a set operation or a WITH query.
     */
    static boolean containsDerivedSource(SqlNode from) {
        SqlNode source = stripAlias(from);
        if (source == null) {
            return false;
        }
        if (source instanceof SqlJoin join) {
            return containsDerivedSource(join.getLeft()) || containsDerivedSource(join.getRight());
        }
        return source instanceof SqlSelect || source instanceof SqlOrderBy || source instanceof SqlWith
                || isSetOperation(source);
    }

    /** The conjuncts of an AND tree of any arity; a non-AND predicate is its own single conjunct. */
    static List<SqlNode> conjuncts(SqlNode predicate) {
        List<SqlNode> conjuncts = new ArrayList<>();
        collectConjuncts(predicate, conjuncts);
        return conjuncts;
    }

    private static void collectConjuncts(SqlNode node, List<SqlNode> collector) {
        if (node == null) {
            return;
        }
        if (node.getKind() == SqlKind.AND && node instanceof SqlCall and) {
            for (SqlNode operand : and.getOperandList()) {
                collectConjuncts(operand, collector);
            }
        } else {
            collector.add(node);
        }
    }

    /**
     * True when any column reference under {@code node} matches. An alias ({@code AS name}) names an
     * output, not a column, so it is never treated as a reference.
     */
    static boolean anyColumnReference(SqlNode node, Predicate<SqlIdentifier> matches) {
        if (node == null) {
            return false;
        }
        if (node instanceof SqlIdentifier identifier) {
            return !identifier.isStar() && matches.test(identifier);
        }
        if (node instanceof SqlNodeList list) {
            for (SqlNode child : list) {
                if (anyColumnReference(child, matches)) return true;
            }
            return false;
        }
        if (node instanceof SqlCall call) {
            if (call.getKind() == SqlKind.AS) {
                return anyColumnReference(call.operand(0), matches);
            }
            for (SqlNode operand : call.getOperandList()) {
                if (anyColumnReference(operand, matches)) return true;
            }
        }
        return false;
    }

    /** Visits every column reference under {@code node}, skipping aliases as {@link #anyColumnReference} does. */
    static void forEachColumnReference(SqlNode node, java.util.function.Consumer<SqlIdentifier> visitor) {
        anyColumnReference(node, identifier -> {
            visitor.accept(identifier);
            return false;
        });
    }

    /**
     * Collects simple identifiers under {@code node} that name one of {@code lowercaseAliases} (output
     * aliases referenced from ORDER BY, GROUP BY or HAVING), compared case-insensitively.
     */
    static void collectAliasReferences(SqlNode node, Set<String> lowercaseAliases,
                                       Set<? super SqlIdentifier> collector) {
        forEachColumnReference(node, identifier -> {
            if (identifier.isSimple() && lowercaseAliases.contains(identifier.getSimple().toLowerCase(Locale.ROOT))) {
                collector.add(identifier);
            }
        });
    }

    /** {@code delta.`path`}, the Spark table reference of a registered Delta location. */
    static String deltaSource(String deltaPath) {
        return "delta.`" + deltaPath.replace("`", "``") + "`";
    }

    /**
     * A placeholder identifier that does not occur in {@code sql} (case-insensitively), used to swap a
     * table for its Delta path after Calcite has unparsed the tree.
     */
    static String unusedSentinel(String sql, String base) {
        String normalizedSql = sql.toUpperCase(Locale.ROOT);
        String candidate = base;
        int suffix = 0;
        while (normalizedSql.contains(candidate.toUpperCase(Locale.ROOT))) {
            candidate = base + "_" + ++suffix;
        }
        return candidate;
    }
}
