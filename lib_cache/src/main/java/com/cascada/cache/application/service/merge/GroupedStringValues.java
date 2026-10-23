package com.cascada.cache.application.service.merge;

import java.util.Arrays;

/**
 * STRING MIN/MAX winners: one shared value, with pages allocated only for groups whose winner
 * differs. SQL NULL/presence belongs to the group's existing validity flag. Losing input strings
 * are not retained in a dictionary. Each instance has one partition owner.
 */
final class GroupedStringValues {
    private final AggregateOperation operation;
    private final int pageShift;
    private final int pageMask;
    private String constant;
    private String[][] exceptions = new String[0][];

    GroupedStringValues(AggregateOperation operation, int pageShift) {
        if (operation != AggregateOperation.MINIMUM && operation != AggregateOperation.MAXIMUM) {
            throw new IllegalArgumentException("STRING aggregate column requires MIN or MAX, got " + operation);
        }
        this.operation = operation;
        this.pageShift = pageShift;
        pageMask = (1 << pageShift) - 1;
    }

    void accumulate(int group, String incoming, boolean initialized) {
        if (constant == null) constant = incoming;
        String winner = incoming;
        if (initialized) {
            String previous = value(group);
            if (previous == incoming || previous.equals(incoming)) return;
            int comparison = SparkValues.compareStrings(previous, incoming);
            if (operation == AggregateOperation.MINIMUM ? comparison <= 0 : comparison >= 0) return;
        }
        int page = group >>> pageShift;
        if (constant == winner || constant.equals(winner)) {
            if (page < exceptions.length && exceptions[page] != null
                    && (group & pageMask) < exceptions[page].length) {
                exceptions[page][group & pageMask] = null;
            }
            return;
        }
        if (page >= exceptions.length) {
            exceptions = Arrays.copyOf(exceptions, Math.max(page + 1, Math.max(4, exceptions.length * 2)));
        }
        int required = (group & pageMask) + 1;
        if (exceptions[page] == null || required > exceptions[page].length) {
            int capacity = page == 0 ? Math.min(1 << pageShift,
                    Math.max(16, Integer.highestOneBit(required - 1) << 1)) : 1 << pageShift;
            exceptions[page] = exceptions[page] == null ? new String[capacity]
                    : Arrays.copyOf(exceptions[page], capacity);
        }
        exceptions[page][group & pageMask] = winner;
    }

    String value(int group) {
        int page = group >>> pageShift;
        if (page >= exceptions.length || exceptions[page] == null
                || (group & pageMask) >= exceptions[page].length) return constant;
        String value = exceptions[page][group & pageMask];
        return value == null ? constant : value;
    }
}
