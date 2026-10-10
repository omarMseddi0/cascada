package com.cascada.sql.adapter.calcite;

import com.cascada.sql.domain.RegisteredTable;
import com.cascada.cache.application.port.out.LogicalSqlTranslatorPort;
import com.cascada.sql.domain.TableCatalog;
import com.cascada.sql.domain.UnsupportedSqlException;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Ports the essence of {@code SmartSQLProcessorSqlglot._transform_ast} onto <b>Apache Calcite</b>:
 * rewrite a query written in the customer's <em>logical</em> terms (logical table + column names, e.g.
 * {@code SELECT country FROM traffic ...}) into the <em>physical</em> SQL that runs against Delta
 * (physical column names, the Delta path as the table, and the time column wrapped in a
 * {@code CAST(FLOOR(ts/N)*N AS BIGINT)} bucket when it is grouped).
 *
 * <p>This is the "the user never writes a path or a physical name" promise. The cache then
 * canonicalises the <em>translated</em> SQL (matching the Python {@code analyze_and_rewrite}, which
 * extracts components from the translated AST so hashes use physical names and survive schema renames).
 *
 * <p>Two Calcite realities are handled explicitly: a {@link SqlShuttle} rewrites identifiers by
 * returning a new tree (Calcite nodes are not renamed in place), and the back-tick quoting around a
 * Delta path is dropped on unparse — so the table is emitted via a sentinel token that is swapped for
 * {@code delta.`/the/path`} afterwards, guaranteeing valid Spark SQL. Anything Calcite cannot parse
 * bypasses to Spark via {@link UnsupportedSqlException}.
 */
public final class LogicalToPhysicalSqlTranslator implements LogicalSqlTranslatorPort {

    private static final String DELTA_TABLE_SENTINEL_PREFIX = "CASCADA_DELTA_TABLE_SENTINEL";

    private final int bucketStepSeconds;
    private final TableCatalog boundCatalog;

    /**
     * Bare translator: the catalog is supplied per call via {@link #translate(String, TableCatalog)}.
     * Use {@link #LogicalToPhysicalSqlTranslator(int, TableCatalog)} when you need the
     * {@link LogicalSqlTranslatorPort} form.
     */
    public LogicalToPhysicalSqlTranslator(int bucketStepSeconds) {
        this(bucketStepSeconds, null);
    }

    /**
     * Binds a catalog so this translator satisfies {@link LogicalSqlTranslatorPort} — the seam the
     * cache side depends on. The port method takes only the SQL string, so the catalog (a piece of
     * deployment configuration, not query input) is captured here at wiring time.
     */
    public LogicalToPhysicalSqlTranslator(int bucketStepSeconds, TableCatalog boundCatalog) {
        this.bucketStepSeconds = bucketStepSeconds;
        this.boundCatalog = boundCatalog;
    }

    @Override
    public String translateToPhysicalSql(String logicalSql) {
        if (boundCatalog == null) {
            throw new IllegalStateException(
                    "no TableCatalog was bound at construction; use translate(sql, catalog) instead");
        }
        return translate(logicalSql, boundCatalog);
    }

    public String translate(String logicalSql, TableCatalog catalog) {
        SqlNode parsed = CalciteSql.parseQuery(logicalSql);
        SqlSelect select = extractSelect(parsed);
        rejectNestedSelects(parsed, select);

        SqlNode from = select.getFrom();
        SqlNode tableNode = stripAlias(from);
        if (!(tableNode instanceof SqlIdentifier tableIdentifier)) {
            throw new UnsupportedSqlException("only a single-table FROM can be translated to a physical path");
        }
        if (!tableIdentifier.isSimple()) {
            throw new UnsupportedSqlException("schema-qualified table names require an explicit catalog mapping");
        }
        String logicalTableName = lastName(tableIdentifier);
        RegisteredTable registeredTable = catalog.findByLogicalName(logicalTableName)
                .orElseThrow(() -> new UnsupportedSqlException(
                        "no registered table for logical name '" + logicalTableName + "'"));

        // 1) swap the table for a collision-free sentinel, 2) rename columns (shuttle yields a new tree),
        // 3) bucket the grouped time column (in place on the new tree).
        Set<String> projectionAliases = projectionAliases(select);
        SqlNodeList orderList = parsed instanceof SqlOrderBy orderBy ? orderBy.orderList : select.getOrderList();
        Set<SqlIdentifier> aliasReferences = aliasReferencesToPreserve(select, orderList,
                projectionAliases, registeredTable);
        String sentinel = newTableSentinel(logicalSql);
        replaceTableWithSentinel(select, from, sentinel);
        SqlNode renamedRoot = parsed.accept(renamer(registeredTable, aliasReferences));
        applyTimeBucketingIfGrouped(extractSelect(renamedRoot), registeredTable);

        String physicalSql = CalciteSql.unparse(renamedRoot);
        String escapedPath = registeredTable.deltaPath().replace("`", "``");
        return physicalSql.replace(sentinel, "delta.`" + escapedPath + "`");
    }

    private SqlSelect extractSelect(SqlNode node) {
        SqlNode root = node instanceof SqlOrderBy orderBy ? orderBy.query : node;
        if (root instanceof SqlSelect select) {
            return select;
        }
        throw new UnsupportedSqlException("only a simple SELECT can be translated to a physical path");
    }

    // --- table -> delta path (via sentinel) ------------------------------------------------------

    private String newTableSentinel(String logicalSql) {
        String normalizedSql = logicalSql.toLowerCase(Locale.ROOT);
        String candidate = DELTA_TABLE_SENTINEL_PREFIX;
        int suffix = 0;
        while (normalizedSql.contains(candidate.toLowerCase(Locale.ROOT))) {
            candidate = DELTA_TABLE_SENTINEL_PREFIX + "_" + ++suffix;
        }
        return candidate;
    }

    private void replaceTableWithSentinel(SqlSelect select, SqlNode from, String sentinelName) {
        SqlIdentifier sentinel = new SqlIdentifier(sentinelName, SqlParserPos.ZERO);
        if (from.getKind() == SqlKind.AS && from instanceof SqlBasicCall asCall) {
            asCall.setOperand(0, sentinel);
        } else {
            select.setFrom(sentinel);
        }
    }

    // --- column renaming -------------------------------------------------------------------------

    private SqlShuttle renamer(RegisteredTable table, Set<SqlIdentifier> aliasReferences) {
        return new SqlShuttle() {
            @Override
            public SqlNode visit(SqlCall call) {
                if (call.getKind() == SqlKind.AS && call instanceof SqlBasicCall asCall) {
                    SqlNode expression = asCall.operand(0).accept(this);
                    // AS operand 1 is a result or relation alias. It is output metadata, not a source
                    // column, so it must keep the spelling the caller requested.
                    return SqlStdOperatorTable.AS.createCall(call.getParserPosition(), expression, asCall.operand(1));
                }
                return super.visit(call);
            }

            @Override
            public SqlNode visit(SqlIdentifier identifier) {
                if (aliasReferences.contains(identifier)) {
                    return identifier;
                }
                String simple = lastName(identifier);
                Optional<String> physical = table.physicalColumnFor(simple);
                return physical
                        .<SqlNode>map(name -> new SqlIdentifier(name, identifier.getParserPosition()))
                        .orElse(identifier);
            }
        };
    }

    private Set<String> projectionAliases(SqlSelect select) {
        return select.getSelectList().stream()
                .map(this::projectionAlias)
                .flatMap(Optional::stream)
                .collect(Collectors.toCollection(HashSet::new));
    }

    private Optional<String> projectionAlias(SqlNode item) {
        if (item instanceof SqlBasicCall asCall && item.getKind() == SqlKind.AS
                && asCall.operand(1) instanceof SqlIdentifier alias) {
            return Optional.of(lastName(alias));
        }
        return Optional.empty();
    }

    private Set<SqlIdentifier> aliasReferencesToPreserve(SqlSelect select, SqlNodeList orderList,
                                                         Set<String> aliases, RegisteredTable table) {
        Set<SqlIdentifier> preserved = Collections.newSetFromMap(new IdentityHashMap<>());
        if (aliases.isEmpty()) {
            return preserved;
        }
        collectAliasReferences(orderList, aliases, preserved);
        rejectAmbiguousAliasBindings(select.getGroup(), aliases, table);
        rejectAmbiguousAliasBindings(select.getHaving(), aliases, table);
        return preserved;
    }

    private void collectAliasReferences(SqlNode node, Set<String> aliases, Set<SqlIdentifier> collector) {
        if (node == null) {
            return;
        }
        if (node instanceof SqlIdentifier identifier) {
            if (identifier.isSimple() && aliases.contains(lastName(identifier))) {
                collector.add(identifier);
            }
        } else if (node instanceof org.apache.calcite.sql.SqlNodeList nodeList) {
            for (SqlNode child : nodeList) {
                collectAliasReferences(child, aliases, collector);
            }
        } else if (node instanceof SqlCall call) {
            for (SqlNode operand : call.getOperandList()) {
                collectAliasReferences(operand, aliases, collector);
            }
        }
    }

    private void rejectAmbiguousAliasBindings(SqlNode expression, Set<String> aliases, RegisteredTable table) {
        if (expression == null) {
            return;
        }
        if (expression instanceof SqlIdentifier identifier) {
            String name = lastName(identifier);
            if (identifier.isSimple() && aliases.contains(name) && table.physicalColumnFor(name).isPresent()) {
                throw new UnsupportedSqlException(
                        "alias reference '" + name + "' collides with a mapped source column in GROUP BY/HAVING");
            }
        } else if (expression instanceof org.apache.calcite.sql.SqlNodeList nodeList) {
            for (SqlNode child : nodeList) {
                rejectAmbiguousAliasBindings(child, aliases, table);
            }
        } else if (expression instanceof SqlCall call) {
            for (SqlNode operand : call.getOperandList()) {
                rejectAmbiguousAliasBindings(operand, aliases, table);
            }
        }
    }

    private void rejectNestedSelects(SqlNode root, SqlSelect outerSelect) {
        if (containsNestedSelect(root, outerSelect)) {
            throw new UnsupportedSqlException("nested SELECT scopes are not supported by logical table translation");
        }
    }

    private boolean containsNestedSelect(SqlNode node, SqlSelect outerSelect) {
        if (node == null) {
            return false;
        }
        if (node instanceof SqlSelect select && select != outerSelect) {
            return true;
        }
        if (node instanceof org.apache.calcite.sql.SqlNodeList nodeList) {
            return nodeList.stream().anyMatch(child -> containsNestedSelect(child, outerSelect));
        }
        if (node instanceof SqlCall call) {
            return call.getOperandList().stream().anyMatch(operand -> containsNestedSelect(operand, outerSelect));
        }
        return false;
    }

    // --- time bucketing --------------------------------------------------------------------------

    private void applyTimeBucketingIfGrouped(SqlSelect select, RegisteredTable table) {
        SqlNodeList group = select.getGroup();
        Set<String> declaredPhysicalTimeColumns = table.physicalTimeColumns();
        if (group == null || declaredPhysicalTimeColumns.isEmpty()) {
            return;
        }

        Set<String> groupedTimeColumns = new HashSet<>();
        for (SqlNode expression : group) {
            for (String physicalTime : declaredPhysicalTimeColumns) {
                if (isColumnNamed(expression, physicalTime)) {
                    groupedTimeColumns.add(physicalTime);
                }
            }
        }
        if (groupedTimeColumns.isEmpty()) {
            return;
        }

        // Bucket only the declared time columns actually present in GROUP BY. Tables can expose
        // more than one time field (for example, start and stop), and set order is not semantic.
        SqlNodeList selectList = select.getSelectList();
        for (int index = 0; index < selectList.size(); index++) {
            SqlNode item = selectList.get(index);
            for (String physicalTime : groupedTimeColumns) {
                if (item.getKind() == SqlKind.AS && item instanceof SqlBasicCall asCall
                        && isColumnNamed(asCall.operand(0), physicalTime)) {
                    asCall.setOperand(0, bucketExpression(physicalTime));
                } else if (isColumnNamed(item, physicalTime)) {
                    selectList.set(index, org.apache.calcite.sql.fun.SqlStdOperatorTable.AS.createCall(
                            SqlParserPos.ZERO, bucketExpression(physicalTime),
                            new SqlIdentifier(physicalTime, SqlParserPos.ZERO)));
                }
            }
        }
        for (int index = 0; index < group.size(); index++) {
            for (String physicalTime : groupedTimeColumns) {
                if (isColumnNamed(group.get(index), physicalTime)) {
                    group.set(index, bucketExpression(physicalTime));
                }
            }
        }
    }

    private SqlNode bucketExpression(String physicalTime) {
        String column = physicalTime.matches("[A-Za-z_][A-Za-z0-9_]*")
                ? physicalTime
                : "`" + physicalTime.replace("`", "``") + "`";
        return CalciteSql.parseExpression(
                "CAST(FLOOR(" + column + " / " + bucketStepSeconds + ") * " + bucketStepSeconds
                        + " AS BIGINT)");
    }

    private boolean isColumnNamed(SqlNode node, String physicalName) {
        return node instanceof SqlIdentifier identifier
                && lastName(identifier).equalsIgnoreCase(physicalName);
    }

    // --- helpers ---------------------------------------------------------------------------------

    private SqlNode stripAlias(SqlNode from) {
        if (from != null && from.getKind() == SqlKind.AS && from instanceof SqlBasicCall call) {
            return call.operand(0);
        }
        return from;
    }

    private String lastName(SqlIdentifier identifier) {
        return identifier.names.get(identifier.names.size() - 1).toLowerCase(Locale.ROOT);
    }
}
