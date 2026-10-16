package com.cascada.sql.adapter.out.calcite;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;

/**
 * Expands parsed {@code AVG} calls into {@code SUM} and {@code COUNT} hash ingredients.
 *
 * <p>This is the linchpin of the AVG-correctness story: an average is never stored or hashed as
 * {@code AVG}; it is decomposed into the two distributive ingredients that <em>can</em> be merged
 * across buckets, and reconstructed only at the end (Root Cause 4).
 */
public final class AggregateNormalizer {

    /** The SUM ingredient of {@code AVG(argument)}; also what gap and storage queries select. */
    static String sumIngredient(String argument) {
        return "SUM(" + argument + ")";
    }

    /** The COUNT ingredient of {@code AVG(argument)}. */
    static String countIngredient(String argument) {
        return "COUNT(" + argument + ")";
    }

    /** AVG -> (SUM, COUNT); dedupe and sort the hash forms while retaining the parsed expression tree. */
    public NormalizedAggregates normalize(List<SqlBasicCall> aggregates) {
        List<String> normalized = new ArrayList<>();
        List<String> originalAverages = new ArrayList<>();

        for (SqlBasicCall aggregate : aggregates) {
            String expression = CalciteSql.unparse(aggregate);
            if (!aggregate.getOperator().getName().equalsIgnoreCase("AVG")) {
                normalized.add(expression);
                continue;
            }

            originalAverages.add(expression);
            SqlNode argument = aggregate.operand(0);
            String renderedArgument = CalciteSql.unparse(argument);
            if (aggregate.getFunctionQuantifier() == null) {
                normalized.add(sumIngredient(renderedArgument));
                normalized.add(countIngredient(renderedArgument));
            } else {
                normalized.add(expression);
            }
        }

        List<String> sortedDeduped = new ArrayList<>(new TreeSet<>(normalized));
        return new NormalizedAggregates(sortedDeduped, originalAverages);
    }
}
