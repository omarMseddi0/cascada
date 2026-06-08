package com.cascada.sql.translate;

import com.cascada.sql.calcite.CalciteSql;
import com.cascada.sql.canonical.UnsupportedSqlException;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.Locale;
import java.util.Optional;

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
public final class LogicalToPhysicalSqlTranslator {

    private static final String DELTA_TABLE_SENTINEL = "CASCADA_DELTA_TABLE_SENTINEL";

    private final int bucketStepSeconds;

    public LogicalToPhysicalSqlTranslator(int bucketStepSeconds) {
        this.bucketStepSeconds = bucketStepSeconds;
    }

    public String translate(String logicalSql, TableCatalog catalog) {
        SqlNode parsed = CalciteSql.parseQuery(logicalSql);
        SqlSelect select = extractSelect(parsed);

        SqlNode from = select.getFrom();
        SqlNode tableNode = stripAlias(from);
        if (!(tableNode instanceof SqlIdentifier tableIdentifier)) {
            throw new UnsupportedSqlException("only a single-table FROM can be translated to a physical path");
        }
        String logicalTableName = lastName(tableIdentifier);
        RegisteredTable registeredTable = catalog.findByLogicalName(logicalTableName)
                .orElseThrow(() -> new UnsupportedSqlException(
                        "no registered table for logical name '" + logicalTableName + "'"));

        // 1) swap the table for a sentinel (in place), 2) rename columns (shuttle yields a new tree),
        // 3) bucket the grouped time column (in place on the new tree).
        replaceTableWithSentinel(select, from);
        SqlNode renamedRoot = parsed.accept(renamer(registeredTable));
        applyTimeBucketingIfGrouped(extractSelect(renamedRoot), registeredTable);

        String physicalSql = CalciteSql.unparse(renamedRoot);
        return physicalSql.replace(DELTA_TABLE_SENTINEL, "delta.`" + registeredTable.deltaPath() + "`");
    }

    private SqlSelect extractSelect(SqlNode node) {
        SqlNode root = node instanceof SqlOrderBy orderBy ? orderBy.query : node;
        if (root instanceof SqlSelect select) {
            return select;
        }
        throw new UnsupportedSqlException("only a simple SELECT can be translated to a physical path");
    }

    // --- table -> delta path (via sentinel) ------------------------------------------------------

    private void replaceTableWithSentinel(SqlSelect select, SqlNode from) {
        SqlIdentifier sentinel = new SqlIdentifier(DELTA_TABLE_SENTINEL, SqlParserPos.ZERO);
        if (from.getKind() == SqlKind.AS && from instanceof SqlBasicCall asCall) {
            asCall.setOperand(0, sentinel);
        } else {
            select.setFrom(sentinel);
        }
    }

    // --- column renaming -------------------------------------------------------------------------

    private SqlShuttle renamer(RegisteredTable table) {
        return new SqlShuttle() {
            @Override
            public SqlNode visit(SqlIdentifier identifier) {
                String simple = lastName(identifier);
                Optional<String> physical = table.physicalColumnFor(simple);
                return physical
                        .<SqlNode>map(name -> new SqlIdentifier(name, identifier.getParserPosition()))
                        .orElse(identifier);
            }
        };
    }

    // --- time bucketing --------------------------------------------------------------------------

    private void applyTimeBucketingIfGrouped(SqlSelect select, RegisteredTable table) {
        SqlNodeList group = select.getGroup();
        Optional<String> physicalTimeColumn = table.physicalTimeColumn();
        if (group == null || physicalTimeColumn.isEmpty()) {
            return;
        }
        String physicalTime = physicalTimeColumn.get();

        boolean timeColumnGrouped = false;
        for (SqlNode expression : group) {
            if (isColumnNamed(expression, physicalTime)) {
                timeColumnGrouped = true;
                break;
            }
        }
        if (!timeColumnGrouped) {
            return;
        }

        // Replace the bare physical time column with the bucket expression in SELECT and GROUP BY.
        SqlNodeList selectList = select.getSelectList();
        for (int index = 0; index < selectList.size(); index++) {
            SqlNode item = selectList.get(index);
            if (item.getKind() == SqlKind.AS && item instanceof SqlBasicCall asCall
                    && isColumnNamed(asCall.operand(0), physicalTime)) {
                asCall.setOperand(0, bucketExpression(physicalTime));
            } else if (isColumnNamed(item, physicalTime)) {
                selectList.set(index, bucketExpression(physicalTime));
            }
        }
        for (int index = 0; index < group.size(); index++) {
            if (isColumnNamed(group.get(index), physicalTime)) {
                group.set(index, bucketExpression(physicalTime));
            }
        }
    }

    private SqlNode bucketExpression(String physicalTime) {
        return CalciteSql.parseExpression(
                "CAST(FLOOR(`" + physicalTime + "` / " + bucketStepSeconds + ") * " + bucketStepSeconds
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
