package com.cascada.sql.adapter.dialect;

import com.cascada.sql.adapter.calcite.CalciteSql;
import com.cascada.sql.domain.UnsupportedSqlException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Translates MySQL SQL into Spark SQL, implementing the function-mapping table in
 * {@code data_explory/all_plans.md} §2. The plan applies ordered, code-aware transforms and then
 * validates the result with a parser (Principle 4); this class uses balanced argument parsing for
 * calls whose semantics depend on arity or nested arguments, then translates the remaining names.
 *
 * <p>Only functions whose <em>name or semantics differ</em> are rewritten; the many identical
 * functions ({@code concat}, {@code length}, {@code abs}, …) need no change because Spark accepts
 * them as-is. Unsupported syntax remains for Calcite validation and the downstream bypass path.
 */
public final class MySqlToSparkFunctionTranslator {

    /** Renames applied as {@code FUNC(} -> {@code spark(} (the trailing paren anchors the call site). */
    private static final Map<String, String> FUNCTION_RENAMES = buildFunctionRenames();

    /** DATE_FORMAT / STR_TO_DATE format token conversions (MySQL {@code %Y} -> Spark {@code yyyy}). */
    private static final Map<String, String> DATE_FORMAT_TOKENS = buildDateFormatTokens();

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
        return renames;
    }

    private static Map<String, String> buildDateFormatTokens() {
        Map<String, String> tokens = new LinkedHashMap<>();
        tokens.put("%%", "'%'");
        tokens.put("%Y", "yyyy");
        tokens.put("%y", "yy");
        tokens.put("%m", "MM");
        tokens.put("%c", "M");
        tokens.put("%d", "dd");
        tokens.put("%e", "d");
        tokens.put("%H", "HH");
        tokens.put("%h", "hh");
        tokens.put("%I", "hh");
        tokens.put("%k", "H");
        tokens.put("%l", "h");
        tokens.put("%i", "mm");
        tokens.put("%s", "ss");
        tokens.put("%S", "ss");
        tokens.put("%f", "SSSSSS");
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
        String spark = rewriteSupportedFunctionCalls(mySqlSql);
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
        String resolved = replaceCode(triggerBodySql, NEW_REFERENCE, match -> newRowAlias + "." + match.group(1));
        resolved = replaceCode(resolved, OLD_REFERENCE, match -> oldRowAlias + "." + match.group(1));
        return resolved;
    }

    private String renameFunctions(String sql) {
        String result = sql;
        for (Map.Entry<String, String> rename : FUNCTION_RENAMES.entrySet()) {
            if (rename.getKey().equals("LOG")) {
                // LOG(x) is ln(x), while LOG(base, value) keeps its base in Spark's log function.
                continue;
            }
            // \bFUNC\s*\( -> spark(   (case-insensitive; only at a call site)
            Pattern callSite = Pattern.compile("(?i)\\b" + Pattern.quote(rename.getKey()) + "\\s*\\(");
            result = replaceCode(result, callSite, match -> rename.getValue() + "(");
        }
        return result;
    }

    private String rewriteSupportedFunctionCalls(String sql) {
        boolean[] code = codePositions(sql);
        StringBuilder result = new StringBuilder(sql.length());
        int index = 0;
        while (index < sql.length()) {
            if (!code[index] || !isIdentifierStart(sql.charAt(index))) {
                result.append(sql.charAt(index++));
                continue;
            }
            int nameEnd = index + 1;
            while (nameEnd < sql.length() && code[nameEnd] && isIdentifierPart(sql.charAt(nameEnd))) {
                nameEnd++;
            }
            String name = sql.substring(index, nameEnd);
            String normalizedName = name.toLowerCase(Locale.ROOT);
            boolean supportedRewrite = switch (normalizedName) {
                case "date_format", "str_to_date", "log", "json_extract", "json_unquote" -> true;
                default -> false;
            };
            if (!supportedRewrite) {
                result.append(sql, index, nameEnd);
                index = nameEnd;
                continue;
            }
            int openingParenthesis = skipWhitespace(sql, code, nameEnd);
            if (openingParenthesis >= sql.length() || !code[openingParenthesis]
                    || sql.charAt(openingParenthesis) != '(') {
                result.append(sql, index, nameEnd);
                index = nameEnd;
                continue;
            }
            int closingParenthesis = findClosingParenthesis(sql, code, openingParenthesis);
            if (closingParenthesis < 0) {
                result.append(sql, index, nameEnd);
                index = nameEnd;
                continue;
            }
            List<String> functionArguments = splitArguments(
                    sql, code, openingParenthesis + 1, closingParenthesis);
            switch (normalizedName) {
                case "date_format", "str_to_date" -> {
                    if (functionArguments.size() == 2) {
                        String firstArgument = rewriteSupportedFunctionCalls(functionArguments.get(0).trim());
                        String formatArgument = functionArguments.get(1).trim();
                        if (isSingleQuotedLiteral(formatArgument)) {
                            formatArgument = sqlStringLiteral(convertFormatTokens(
                                    formatArgument.substring(1, formatArgument.length() - 1)));
                        } else {
                            throw new UnsupportedSqlException(
                                    "dynamic MySQL date format expressions cannot be translated safely");
                        }
                        result.append(normalizedName).append('(').append(firstArgument).append(", ")
                                .append(formatArgument).append(')');
                    } else {
                        result.append(sql, index, closingParenthesis + 1);
                    }
                }
                case "log" -> {
                    if (functionArguments.size() == 1 || functionArguments.size() == 2) {
                        List<String> nestedArguments = functionArguments.stream()
                                .map(argument -> rewriteSupportedFunctionCalls(argument.trim())).toList();
                        result.append(functionArguments.size() == 1 ? "ln(" : "log(")
                                .append(String.join(", ", nestedArguments)).append(')');
                    } else {
                        result.append(sql, index, closingParenthesis + 1);
                    }
                }
                case "json_unquote" -> {
                    if (functionArguments.size() != 1) {
                        throw new UnsupportedSqlException("JSON_UNQUOTE requires exactly one argument");
                    }
                    List<String> extractArguments = argumentsOfCall(
                            functionArguments.get(0).trim(), "JSON_EXTRACT");
                    if (extractArguments == null || extractArguments.size() != 2) {
                        throw new UnsupportedSqlException(
                                "only JSON_UNQUOTE(JSON_EXTRACT(document, onePath)) has a safe Spark translation");
                    }
                    String document = rewriteSupportedFunctionCalls(extractArguments.get(0).trim());
                    String path = rewriteSupportedFunctionCalls(extractArguments.get(1).trim());
                    result.append("get_json_object(").append(document).append(", ").append(path).append(')');
                }
                case "json_extract" -> throw new UnsupportedSqlException(
                        "JSON_EXTRACT returns a JSON value; use JSON_UNQUOTE for its string Spark equivalent");
                default -> result.append(sql, index, closingParenthesis + 1);
            }
            index = closingParenthesis + 1;
        }
        return result.toString();
    }

    private int skipWhitespace(String sql, boolean[] code, int index) {
        while (index < sql.length() && code[index] && Character.isWhitespace(sql.charAt(index))) {
            index++;
        }
        return index;
    }

    private int findClosingParenthesis(String sql, boolean[] code, int openingParenthesis) {
        int depth = 0;
        for (int index = openingParenthesis; index < sql.length(); index++) {
            if (!code[index]) {
                continue;
            }
            if (sql.charAt(index) == '(') {
                depth++;
            } else if (sql.charAt(index) == ')') {
                depth--;
                if (depth == 0) {
                    return index;
                }
            }
        }
        return -1;
    }

    private List<String> splitArguments(String sql, boolean[] code, int start, int end) {
        if (sql.substring(start, end).trim().isEmpty()) {
            return List.of();
        }
        List<String> arguments = new ArrayList<>();
        int depth = 0;
        int argumentStart = start;
        for (int index = start; index < end; index++) {
            if (!code[index]) {
                continue;
            }
            char current = sql.charAt(index);
            if (current == '(') {
                depth++;
            } else if (current == ')') {
                depth--;
            } else if (current == ',' && depth == 0) {
                arguments.add(sql.substring(argumentStart, index));
                argumentStart = index + 1;
            }
        }
        if (argumentStart < end || !arguments.isEmpty()) {
            arguments.add(sql.substring(argumentStart, end));
        }
        return arguments;
    }

    private List<String> argumentsOfCall(String expression, String expectedName) {
        int nameStart = 0;
        while (nameStart < expression.length() && Character.isWhitespace(expression.charAt(nameStart))) {
            nameStart++;
        }
        int nameEnd = nameStart;
        while (nameEnd < expression.length() && isIdentifierPart(expression.charAt(nameEnd))) {
            nameEnd++;
        }
        if (!expression.substring(nameStart, nameEnd).equalsIgnoreCase(expectedName)) {
            return null;
        }
        boolean[] code = codePositions(expression);
        int opening = skipWhitespace(expression, code, nameEnd);
        if (opening >= expression.length() || expression.charAt(opening) != '(') {
            return null;
        }
        int closing = findClosingParenthesis(expression, code, opening);
        if (closing < 0 || !expression.substring(closing + 1).trim().isEmpty()) {
            return null;
        }
        return splitArguments(expression, code, opening + 1, closing);
    }

    private boolean isSingleQuotedLiteral(String value) {
        if (value.length() < 2 || value.charAt(0) != '\'' || value.charAt(value.length() - 1) != '\'') {
            return false;
        }
        boolean[] code = codePositions(value);
        for (boolean isCode : code) {
            if (isCode) {
                return false;
            }
        }
        return true;
    }

    private boolean isIdentifierStart(char value) {
        return Character.isLetter(value) || value == '_';
    }

    private boolean isIdentifierPart(char value) {
        return Character.isLetterOrDigit(value) || value == '_';
    }

    private String convertFormatTokens(String mySqlFormat) {
        StringBuilder converted = new StringBuilder(mySqlFormat.length());
        for (int index = 0; index < mySqlFormat.length(); index++) {
            char current = mySqlFormat.charAt(index);
            if (current != '%') {
                converted.append(current);
                continue;
            }
            if (index + 1 >= mySqlFormat.length()) {
                throw new UnsupportedSqlException("incomplete MySQL date format token");
            }
            String token = mySqlFormat.substring(index, index + 2);
            String replacement = DATE_FORMAT_TOKENS.get(token);
            if (replacement == null) {
                throw new UnsupportedSqlException("unsupported MySQL date format token '" + token + "'");
            }
            converted.append(replacement);
            index++;
        }
        return converted.toString();
    }

    private String sqlStringLiteral(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private String fixCastTypes(String sql) {
        String result = sql;
        result = replaceCode(result, Pattern.compile("(?i)\\bAS\\s+SIGNED\\b"), match -> "AS BIGINT");
        result = replaceCode(result, Pattern.compile("(?i)\\bAS\\s+UNSIGNED\\b"), match -> "AS BIGINT");
        result = replaceCode(result, Pattern.compile("(?i)\\bAS\\s+CHAR\\b"), match -> "AS STRING");
        result = replaceCode(result, Pattern.compile("(?i)\\bAS\\s+DATETIME\\b"), match -> "AS TIMESTAMP");
        return result;
    }

    private static String replaceCode(String sql, Pattern pattern,
                                      java.util.function.Function<Matcher, String> replacement) {
        boolean[] code = codePositions(sql);
        Matcher matcher = pattern.matcher(sql);
        StringBuilder out = new StringBuilder();
        while (matcher.find()) {
            if (code[matcher.start()]) matcher.appendReplacement(out, Matcher.quoteReplacement(replacement.apply(matcher)));
        }
        matcher.appendTail(out);
        return out.toString();
    }

    private static boolean[] codePositions(String sql) {
        boolean[] code = new boolean[sql.length()];
        int i = 0;
        while (i < sql.length()) {
            char c = sql.charAt(i);
            if (c == '\'' || c == '"' || c == '`') {
                char quote = c;
                i++;
                while (i < sql.length()) {
                    char next = sql.charAt(i++);
                    if (next == '\\' && i < sql.length()) { i++; continue; }
                    if (next == quote) {
                        if (i < sql.length() && sql.charAt(i) == quote) { i++; continue; }
                        break;
                    }
                }
            } else if (sql.startsWith("--", i)) {
                while (i < sql.length() && sql.charAt(i) != '\n') i++;
            } else if (sql.startsWith("/*", i)) {
                int end = sql.indexOf("*/", i + 2);
                i = end < 0 ? sql.length() : end + 2;
            } else {
                code[i++] = true;
            }
        }
        return code;
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
