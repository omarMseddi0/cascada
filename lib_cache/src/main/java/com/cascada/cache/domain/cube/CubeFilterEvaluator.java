package com.cascada.cache.domain.cube;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Checks whether cube filters can be applied exactly to stored dimensions, then evaluates them. */
final class CubeFilterEvaluator {

    private static final Pattern EQUALITY_FILTER =
            Pattern.compile("\\s*([A-Za-z_][A-Za-z0-9_]*)\\s*=\\s*(.*?)\\s*");
    private static final Pattern IN_FILTER =
            Pattern.compile("(?i)\\s*([A-Za-z_][A-Za-z0-9_]*)\\s+IN\\s*\\((.*)\\)\\s*");

    boolean canNarrow(QueryShape candidate, QueryShape query) {
        return query.filters().containsAll(candidate.filters())
                && extraFiltersAreOnGroupedColumns(candidate, query);
    }

    boolean isFilterNarrowable(QueryShape candidate, QueryShape query) {
        return query.filters().containsAll(candidate.filters());
    }

    boolean extraFiltersAreOnGroupedColumns(QueryShape candidate, QueryShape query) {
        Set<String> extraFilters = extraFilters(candidate, query);
        for (String filter : extraFilters) {
            Optional<String> column = filterColumn(filter);
            if (column.isEmpty() || !candidate.groupBy().contains(column.get())) {
                return false;
            }
        }
        return true;
    }

    Set<String> extraFilters(QueryShape candidate, QueryShape query) {
        Set<String> extra = new HashSet<>(query.filters());
        extra.removeAll(candidate.filters());
        return extra;
    }

    boolean matchesAll(ResultFrame frame, Map<String, Object> row, Set<String> filters) {
        for (String filter : filters) {
            if (!rowMatchesFilter(frame, row, filter)) {
                return false;
            }
        }
        return true;
    }

    private boolean rowMatchesFilter(ResultFrame frame, Map<String, Object> row, String filter) {
        Matcher in = IN_FILTER.matcher(filter);
        if (in.matches()) {
            String column = in.group(1);
            if (!row.containsKey(column)) {
                throw new CubeRollUpUnavailableException("IN filter on absent column '" + column + "'");
            }
            Object cell = row.get(column);
            ColumnType type = frame.columnType(column);
            for (CubeFilterLiteral literal : parseInList(in.group(2))) {
                if (valueEqualsLiteral(cell, type, literal)) {
                    return true;
                }
            }
            return false;
        }
        Matcher equality = EQUALITY_FILTER.matcher(filter);
        if (!equality.matches()) {
            throw new CubeRollUpUnavailableException("filter '" + filter + "' cannot be evaluated in memory");
        }
        String column = equality.group(1);
        if (!row.containsKey(column)) {
            throw new CubeRollUpUnavailableException("equality filter on absent column '" + column + "'");
        }
        return valueEqualsLiteral(row.get(column), frame.columnType(column), parseLiteral(equality.group(2)));
    }

    private List<CubeFilterLiteral> parseInList(String raw) {
        List<CubeFilterLiteral> literals = new ArrayList<>();
        StringBuilder token = new StringBuilder();
        boolean quoted = false;
        boolean wasQuoted = false;
        boolean closedQuote = false;
        for (int index = 0; index < raw.length(); index++) {
            char character = raw.charAt(index);
            if (character == '\'') {
                if (quoted && index + 1 < raw.length() && raw.charAt(index + 1) == '\'') {
                    token.append('\'');
                    index++;
                } else if (quoted) {
                    quoted = false;
                    closedQuote = true;
                } else {
                    if (closedQuote || !token.toString().trim().isEmpty()) {
                        throw new CubeRollUpUnavailableException("malformed quoted member in IN filter");
                    }
                    quoted = true;
                    wasQuoted = true;
                }
                continue;
            }
            if (!quoted && character == ',') {
                literals.add(parseLiteralToken(token.toString(), wasQuoted));
                token.setLength(0);
                wasQuoted = false;
                closedQuote = false;
                continue;
            }
            if (closedQuote && !Character.isWhitespace(character)) {
                throw new CubeRollUpUnavailableException("characters follow a quoted IN member");
            }
            if (quoted || !Character.isWhitespace(character)) {
                token.append(character);
            }
        }
        if (quoted) {
            throw new CubeRollUpUnavailableException("unterminated quote in IN filter");
        }
        literals.add(parseLiteralToken(token.toString(), wasQuoted));
        return literals;
    }

    private CubeFilterLiteral parseLiteral(String raw) {
        String token = raw.trim();
        if (token.startsWith("'")) {
            if (token.length() < 2 || !token.endsWith("'")) {
                throw new CubeRollUpUnavailableException("malformed quoted filter literal");
            }
            String value = token.substring(1, token.length() - 1).replace("''", "'");
            return new CubeFilterLiteral(value, true, false);
        }
        return parseLiteralToken(token, false);
    }

    private CubeFilterLiteral parseLiteralToken(String value, boolean quoted) {
        String token = quoted ? value : value.trim();
        return new CubeFilterLiteral(token, quoted, !quoted && token.equalsIgnoreCase("NULL"));
    }

    private boolean valueEqualsLiteral(Object cell, ColumnType type, CubeFilterLiteral literal) {
        // SQL equality with NULL is UNKNOWN, and therefore never selects a row.
        if (cell == null || literal.sqlNull()) {
            return false;
        }
        try {
            return switch (type) {
                case STRING -> {
                    if (!literal.quoted()) {
                        throw new CubeRollUpUnavailableException("unquoted literal for STRING dimension");
                    }
                    yield cell.equals(literal.value());
                }
                case LONG -> {
                    if (literal.quoted()) {
                        throw new CubeRollUpUnavailableException("quoted literal for numeric dimension");
                    }
                    yield BigDecimal.valueOf(((Number) cell).longValue())
                            .compareTo(new BigDecimal(literal.value())) == 0;
                }
                case DECIMAL -> {
                    if (literal.quoted()) {
                        throw new CubeRollUpUnavailableException("quoted literal for numeric dimension");
                    }
                    yield ((BigDecimal) cell).compareTo(new BigDecimal(literal.value())) == 0;
                }
                case DOUBLE -> {
                    if (literal.quoted()) {
                        throw new CubeRollUpUnavailableException("quoted literal for numeric dimension");
                    }
                    double left = ((Number) cell).doubleValue();
                    double right = Double.parseDouble(literal.value());
                    yield left == right || Double.isNaN(left) && Double.isNaN(right);
                }
            };
        } catch (NumberFormatException malformedNumericLiteral) {
            throw new CubeRollUpUnavailableException("numeric filter literal cannot be parsed for " + type);
        }
    }

    private Optional<String> filterColumn(String filter) {
        Matcher in = IN_FILTER.matcher(filter);
        if (in.matches()) {
            return Optional.of(in.group(1));
        }
        Matcher equality = EQUALITY_FILTER.matcher(filter);
        return equality.matches() ? Optional.of(equality.group(1)) : Optional.empty();
    }
}
