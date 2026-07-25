package com.cascada.sql.translate;

import com.cascada.sql.calcite.CalciteSql;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Translates MySQL SQL into Spark SQL, implementing the function-mapping table in
 * {@code data_explory/all_plans.md} §2. The plan's own design (lines ~1560-1601) applies ordered
 * regex transforms and then validates the result with a real parser (Principle 4); this class does
 * exactly that — word-boundary, case-insensitive function renames followed by a JSqlParser parse
 * check.
 *
 * <p>Only functions whose <em>name or semantics differ</em> are rewritten; the many identical
 * functions ({@code concat}, {@code length}, {@code abs}, …) need no change because Spark accepts
 * them as-is. Constructs JSqlParser cannot represent (the JSON {@code ->} operator) are documented as
 * unsupported and left to a downstream bypass, never silently mistranslated.
 */
public final class MySqlToSparkFunctionTranslator {

    /** Renames applied as {@code FUNC(} -> {@code spark(} (the trailing paren anchors the call site). */
    private static final Map<String, String> FUNCTION_RENAMES = buildFunctionRenames();

    /** DATE_FORMAT / STR_TO_DATE format token conversions (MySQL {@code %Y} -> Spark {@code yyyy}). */
    private static final Map<String, String> DATE_FORMAT_TOKENS = buildDateFormatTokens();

    private static final Pattern DATE_FORMAT_CALL =
            Pattern.compile("(?i)\\b(date_format|str_to_date)\\s*\\(([^,]+),\\s*'([^']*)'\\s*\\)");
    private static final Pattern NEW_REFERENCE = Pattern.compile("(?i)\\bNEW\\.([A-Za-z_][A-Za-z0-9_]*)");
    private static final Pattern OLD_REFERENCE = Pattern.compile("(?i)\\bOLD\\.([A-Za-z_][A-Za-z0-9_]*)");

    private static Map<String, String> buildFunctionRenames() {
        Map<String, String> renames = new LinkedHashMap<>();
        // Date/time
        renames.put("NOW", "current_timestamp");
        renames.put("CURDATE", "current_date");
        renames.put("UTC_TIMESTAMP", "current_timestamp");
        renames.put("DATE", "to_date");
        renames.put("ADDDATE", "date_add");
        renames.put("SUBDATE", "date_sub");
        renames.put("STR_TO_DATE", "to_timestamp");
        // Null handling
        renames.put("IFNULL", "coalesce");
        // Numeric
        renames.put("SIGN", "signum");
        renames.put("LOG", "ln");            // MySQL LOG = natural log
        renames.put("POW", "power");
        // String / format
        renames.put("FORMAT", "format_number");
        // JSON
        renames.put("JSON_EXTRACT", "get_json_object");
        return renames;
    }

    private static Map<String, String> buildDateFormatTokens() {
        Map<String, String> tokens = new LinkedHashMap<>();
        tokens.put("%Y", "yyyy");
        tokens.put("%y", "yy");
        tokens.put("%m", "MM");
        tokens.put("%d", "dd");
        tokens.put("%H", "HH");
        tokens.put("%h", "hh");
        tokens.put("%i", "mm");
        tokens.put("%s", "ss");
        tokens.put("%p", "a");
        tokens.put("%W", "EEEE");
        tokens.put("%M", "MMMM");
        tokens.put("%b", "MMM");
        tokens.put("%a", "EEE");
        tokens.put("%j", "DDD");
        tokens.put("%T", "HH:mm:ss");
        tokens.put("%r", "hh:mm:ss a");
        return tokens;
    }

    /** Translate a standalone MySQL expression or SELECT into Spark SQL. */
    public String translate(String mySqlSql) {
        String spark = mySqlSql;
        spark = convertDateFormatStrings(spark);
        spark = renameFunctions(spark);
        spark = fixCastTypes(spark);
        return spark;
    }

    /**
     * Resolve a trigger body's {@code NEW.col} / {@code OLD.col} references to the streaming source
     * aliases (plan §4), then translate functions. {@code NEW} maps to the inserted/updated row alias
     * and {@code OLD} to the prior-row alias.
     */
    public String resolveNewOldReferences(String triggerBodySql, String newRowAlias, String oldRowAlias) {
        String resolved = NEW_REFERENCE.matcher(triggerBodySql).replaceAll(newRowAlias + ".$1");
        resolved = OLD_REFERENCE.matcher(resolved).replaceAll(oldRowAlias + ".$1");
        return resolved;
    }

    private String renameFunctions(String sql) {
        String result = sql;
        for (Map.Entry<String, String> rename : FUNCTION_RENAMES.entrySet()) {
            // \bFUNC\s*\( -> spark(   (case-insensitive; only at a call site)
            Pattern callSite = Pattern.compile("(?i)\\b" + Pattern.quote(rename.getKey()) + "\\s*\\(");
            result = callSite.matcher(result).replaceAll(Matcher.quoteReplacement(rename.getValue() + "("));
        }
        return result;
    }

    private String convertDateFormatStrings(String sql) {
        Matcher matcher = DATE_FORMAT_CALL.matcher(sql);
        StringBuilder rebuilt = new StringBuilder();
        while (matcher.find()) {
            String functionName = matcher.group(1).toLowerCase(java.util.Locale.ROOT);
            String firstArgument = matcher.group(2).trim();
            String formatString = matcher.group(3);
            String convertedFormat = convertFormatTokens(formatString);
            String replacement = functionName + "(" + firstArgument + ", '" + convertedFormat + "')";
            matcher.appendReplacement(rebuilt, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(rebuilt);
        return rebuilt.toString();
    }

    private String convertFormatTokens(String mySqlFormat) {
        String converted = mySqlFormat;
        for (Map.Entry<String, String> token : DATE_FORMAT_TOKENS.entrySet()) {
            converted = converted.replace(token.getKey(), token.getValue());
        }
        return converted;
    }

    private String fixCastTypes(String sql) {
        String result = sql;
        result = Pattern.compile("(?i)\\bAS\\s+SIGNED\\b").matcher(result).replaceAll("AS BIGINT");
        result = Pattern.compile("(?i)\\bAS\\s+UNSIGNED\\b").matcher(result).replaceAll("AS BIGINT");
        result = Pattern.compile("(?i)\\bAS\\s+CHAR\\b").matcher(result).replaceAll("AS STRING");
        result = Pattern.compile("(?i)\\bAS\\s+DATETIME\\b").matcher(result).replaceAll("AS TIMESTAMP");
        return result;
    }

    /** Principle 4 of the plan: the translated SQL must parse (now validated by Apache Calcite). */
    public boolean isParseable(String sql) {
        return CalciteSql.isParseable(sql);
    }

    /** Exposed for the translation-matrix test and the public operator-coverage docs. */
    public static Map<String, String> functionRenames() {
        return Map.copyOf(FUNCTION_RENAMES);
    }
}
