package com.cascada.cache.application.service.merge;

import java.util.function.IntBinaryOperator;

/** Stable bottom-up merge sort of primitive row or group ids; no boxing, no per-element adapter. */
public final class IntStableSort {

    private IntStableSort() {
    }

    /** Sorts {@code ids} in place; equal elements keep their input order. */
    public static void sort(int[] ids, IntBinaryOperator comparator) {
        if (ids.length < 2) return;
        int[] source = ids, target = new int[ids.length];
        for (long width = 1; width < ids.length; width *= 2) {
            for (long start = 0; start < ids.length; start += width * 2) {
                int left = (int) start, middle = (int) Math.min(start + width, ids.length);
                int right = middle, end = (int) Math.min(start + width * 2, ids.length), out = left;
                while (left < middle && right < end) {
                    target[out++] = comparator.applyAsInt(source[left], source[right]) <= 0 ? source[left++] : source[right++];
                }
                while (left < middle) target[out++] = source[left++];
                while (right < end) target[out++] = source[right++];
            }
            int[] swap = source; source = target; target = swap;
        }
        if (source != ids) System.arraycopy(source, 0, ids, 0, ids.length);
    }
}
