package com.cascada.cache.benchmark.fast;

import com.cascada.cache.application.service.merge.AggregateOperation;
import com.cascada.cache.application.service.merge.SparkValues;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.FrameColumnReader;
import com.cascada.cache.domain.frame.ResultFrame;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * One hash partition of the merge state. Prototype of a row-major, paged, vectorized group table:
 * <ul>
 *   <li>every group's keys, measures and null flags live contiguously in one {@code long[]} page
 *       (one or two cache lines per group instead of ~26 parallel arrays),</li>
 *   <li>pages are fixed-size, so growth never copies or doubles group state,</li>
 *   <li>a slot stores {@code hashTag<<32 | group+1}, so a probe rejects mismatches without touching
 *       the group,</li>
 *   <li>a frame is processed column-at-a-time: one hash pass, one probe pass, one tight loop per
 *       measure column (no per-cell type switch),</li>
 *   <li>STRING MIN/MAX measures are dictionary ids; strings are compared only when the ids differ.</li>
 * </ul>
 */
final class GroupTable {
    static final int PAGE_SHIFT = 14;
    static final int PAGE_SIZE = 1 << PAGE_SHIFT;
    static final int PAGE_MASK = PAGE_SIZE - 1;

    final Layout layout;
    final int stride;
    long[][] pages = new long[16][];
    String[][] stringKeyPages; // only for STRING keys: PAGE_SIZE * stringKeyCount per page
    long[] slots = new long[1024];
    int slotMask = slots.length - 1;
    int count;
    final List<List<String>> dictionaries = new ArrayList<>();
    final List<Map<String, Integer>> dictionaryIndex = new ArrayList<>();
    final String[] lastString;
    final int[] lastId;

    GroupTable(Layout layout) {
        this.layout = layout;
        this.stride = layout.stride;
        if (layout.stringKeyCount > 0) stringKeyPages = new String[16][];
        lastString = new String[layout.measureCount];
        lastId = new int[layout.measureCount];
        for (int m = 0; m < layout.measureCount; m++) {
            dictionaries.add(new ArrayList<>());
            dictionaryIndex.add(new HashMap<>());
        }
    }

    /** Process the rows of {@code frame} whose hash maps to this partition. */
    void accept(FrameColumnReader[] in, int rows, long[] hashes, int[] groups, int partition, int partitions) {
        // probe pass
        for (int row = 0; row < rows; row++) {
            long hash = hashes[row];
            if (partitions > 1 && Layout.partitionOf(hash, partitions) != partition) {
                groups[row] = -1;
                continue;
            }
            groups[row] = findOrCreate(in, row, hash);
        }
        // one tight loop per measure column
        for (int m = 0; m < layout.measureCount; m++) {
            FrameColumnReader column = in[layout.measureColumns[m]];
            int offset = layout.measureOffset[m];
            long bit = 1L << layout.measureFlag[m];
            int flagOffset = layout.flagOffset;
            AggregateOperation op = layout.measureOps[m];
            switch (layout.measureTypes[m]) {
                case DOUBLE -> {
                    for (int row = 0; row < rows; row++) {
                        int g = groups[row];
                        if (g < 0 || column.isNullAt(row)) continue;
                        double v = column.doubleValue(row);
                        long[] p = pages[g >>> PAGE_SHIFT];
                        int base = (g & PAGE_MASK) * stride;
                        long flags = p[base + flagOffset];
                        if ((flags & bit) == 0) {
                            p[base + offset] = Double.doubleToRawLongBits(v);
                            p[base + flagOffset] = flags | bit;
                        } else if (op == AggregateOperation.SUM || op == AggregateOperation.COUNT) {
                            p[base + offset] = Double.doubleToRawLongBits(Double.longBitsToDouble(p[base + offset]) + v);
                        } else {
                            p[base + offset] = Double.doubleToRawLongBits(op.combine(Double.longBitsToDouble(p[base + offset]), v));
                        }
                    }
                }
                case LONG -> {
                    for (int row = 0; row < rows; row++) {
                        int g = groups[row];
                        if (g < 0 || column.isNullAt(row)) continue;
                        long v = column.longValue(row);
                        long[] p = pages[g >>> PAGE_SHIFT];
                        int base = (g & PAGE_MASK) * stride;
                        long flags = p[base + flagOffset];
                        if ((flags & bit) == 0) {
                            p[base + offset] = v;
                            p[base + flagOffset] = flags | bit;
                        } else {
                            long prev = p[base + offset];
                            p[base + offset] = switch (op) {
                                case SUM, COUNT -> Math.addExact(prev, v);
                                case MINIMUM -> Math.min(prev, v);
                                case MAXIMUM -> Math.max(prev, v);
                            };
                        }
                    }
                }
                case STRING -> {
                    boolean min = op == AggregateOperation.MINIMUM;
                    for (int row = 0; row < rows; row++) {
                        int g = groups[row];
                        if (g < 0 || column.isNullAt(row)) continue;
                        int id = dictionaryId(m, column.stringValue(row));
                        long[] p = pages[g >>> PAGE_SHIFT];
                        int base = (g & PAGE_MASK) * stride;
                        long flags = p[base + flagOffset];
                        if ((flags & bit) == 0) {
                            p[base + offset] = id;
                            p[base + flagOffset] = flags | bit;
                        } else if (p[base + offset] != id) {
                            List<String> dict = dictionaries.get(m);
                            int c = SparkValues.compareStrings(dict.get((int) p[base + offset]), dict.get(id));
                            if (min ? c > 0 : c < 0) p[base + offset] = id;
                        }
                    }
                }
                case DECIMAL -> throw new UnsupportedOperationException("prototype: DECIMAL measures");
            }
        }
    }

    int dictionaryId(int measure, String value) {
        if (value == lastString[measure]) return lastId[measure];
        Integer id = dictionaryIndex.get(measure).get(value);
        if (id == null) {
            id = dictionaries.get(measure).size();
            dictionaries.get(measure).add(value);
            dictionaryIndex.get(measure).put(value, id);
        }
        lastString[measure] = value;
        lastId[measure] = id;
        return id;
    }

    int findOrCreate(FrameColumnReader[] in, int row, long hash) {
        long tag = hash >>> 32;
        int slot = (int) hash & slotMask;
        while (true) {
            long entry = slots[slot];
            if (entry == 0) break;
            if ((entry >>> 32) == tag) {
                int group = (int) entry - 1;
                if (sameKeys(group, in, row)) return group;
            }
            slot = (slot + 1) & slotMask;
        }
        if ((count + 1) * 2L > slots.length) {
            growSlots();
            slot = (int) hash & slotMask;
            while (slots[slot] != 0) slot = (slot + 1) & slotMask;
        }
        int group = count++;
        int page = group >>> PAGE_SHIFT;
        if (page == pages.length) {
            pages = java.util.Arrays.copyOf(pages, page * 2);
            if (stringKeyPages != null) stringKeyPages = java.util.Arrays.copyOf(stringKeyPages, page * 2);
        }
        if (pages[page] == null) {
            pages[page] = new long[PAGE_SIZE * stride];
            if (stringKeyPages != null) stringKeyPages[page] = new String[PAGE_SIZE * layout.stringKeyCount];
        }
        long[] p = pages[page];
        int base = (group & PAGE_MASK) * stride;
        p[base + layout.hashOffset] = hash;
        long flags = 0;
        for (int k = 0; k < layout.keyCount; k++) {
            FrameColumnReader column = in[layout.keyColumns[k]];
            if (column.isNullAt(row)) continue;
            flags |= 1L << k;
            switch (layout.keyTypes[k]) {
                case LONG -> p[base + layout.keyOffset[k]] = column.longValue(row);
                case DOUBLE -> p[base + layout.keyOffset[k]] = Double.doubleToLongBits(SparkValues.canonicalDouble(column.doubleValue(row)));
                case STRING -> stringKeyPages[page][(group & PAGE_MASK) * layout.stringKeyCount + layout.keyOffset[k]] = column.stringValue(row);
                case DECIMAL -> throw new UnsupportedOperationException("prototype: DECIMAL keys");
            }
        }
        p[base + layout.flagOffset] = flags;
        slots[slot] = (tag << 32) | (group + 1L);
        return group;
    }

    boolean sameKeys(int group, FrameColumnReader[] in, int row) {
        long[] p = pages[group >>> PAGE_SHIFT];
        int base = (group & PAGE_MASK) * stride;
        long flags = p[base + layout.flagOffset];
        for (int k = 0; k < layout.keyCount; k++) {
            FrameColumnReader column = in[layout.keyColumns[k]];
            boolean present = !column.isNullAt(row);
            if (present != ((flags & (1L << k)) != 0)) return false;
            if (!present) continue;
            boolean equal = switch (layout.keyTypes[k]) {
                case LONG -> p[base + layout.keyOffset[k]] == column.longValue(row);
                case DOUBLE -> p[base + layout.keyOffset[k]]
                        == Double.doubleToLongBits(SparkValues.canonicalDouble(column.doubleValue(row)));
                case STRING -> column.stringValue(row).equals(stringKeyPages[group >>> PAGE_SHIFT]
                        [(group & PAGE_MASK) * layout.stringKeyCount + layout.keyOffset[k]]);
                case DECIMAL -> false;
            };
            if (!equal) return false;
        }
        return true;
    }

    void growSlots() {
        long[] grown = new long[slots.length * 2];
        int mask = grown.length - 1;
        for (long entry : slots) {
            if (entry == 0) continue;
            int group = (int) entry - 1;
            long hash = pages[group >>> PAGE_SHIFT][(group & PAGE_MASK) * stride + layout.hashOffset];
            int slot = (int) hash & mask;
            while (grown[slot] != 0) slot = (slot + 1) & mask;
            grown[slot] = entry;
        }
        slots = grown;
        slotMask = mask;
    }

    /** Releases the probe table once accepting is over; only group state is needed to emit. */
    void releaseSlots() {
        slots = null;
    }

    // --- accessors used by the emitter --------------------------------------------------------

    boolean flag(int group, int bit) {
        return (pages[group >>> PAGE_SHIFT][(group & PAGE_MASK) * stride + layout.flagOffset] & (1L << bit)) != 0;
    }

    long word(int group, int offset) {
        return pages[group >>> PAGE_SHIFT][(group & PAGE_MASK) * stride + offset];
    }

    String stringKey(int group, int key) {
        return stringKeyPages[group >>> PAGE_SHIFT][(group & PAGE_MASK) * layout.stringKeyCount + layout.keyOffset[key]];
    }

    int compare(int left, int right) {
        for (int k = 0; k < layout.keyCount; k++) {
            boolean a = flag(left, k), b = flag(right, k);
            int c;
            if (!a || !b) c = a == b ? 0 : a ? 1 : -1;
            else c = switch (layout.keyTypes[k]) {
                case LONG -> Long.compare(word(left, layout.keyOffset[k]), word(right, layout.keyOffset[k]));
                case DOUBLE -> Double.compare(Double.longBitsToDouble(word(left, layout.keyOffset[k])),
                        Double.longBitsToDouble(word(right, layout.keyOffset[k])));
                case STRING -> SparkValues.compareStrings(stringKey(left, k), stringKey(right, k));
                case DECIMAL -> 0;
            };
            if (c != 0) return c;
        }
        return 0;
    }

    /** Group ids sorted by key: same output order as TypedAggregationGroups.sortedGroupIds. */
    int[] sortedGroups() {
        int[] order = new int[count];
        for (int i = 0; i < count; i++) order[i] = i;
        if (layout.keyCount == 0 || count < 2) return order;
        int[] scratch = new int[count];
        mergeSort(order, scratch, 0, count);
        return order;
    }

    private void mergeSort(int[] a, int[] tmp, int from, int to) {
        if (to - from <= 24) {
            for (int i = from + 1; i < to; i++) {
                int v = a[i], j = i - 1;
                while (j >= from && compare(a[j], v) > 0) { a[j + 1] = a[j]; j--; }
                a[j + 1] = v;
            }
            return;
        }
        int mid = (from + to) >>> 1;
        mergeSort(a, tmp, from, mid);
        mergeSort(a, tmp, mid, to);
        if (compare(a[mid - 1], a[mid]) <= 0) return;
        System.arraycopy(a, from, tmp, from, to - from);
        int i = from, j = mid, k = from;
        while (i < mid && j < to) a[k++] = compare(tmp[j], tmp[i]) < 0 ? tmp[j++] : tmp[i++];
        while (i < mid) a[k++] = tmp[i++];
        while (j < to) a[k++] = tmp[j++];
    }

    long retainedBytes() {
        long bytes = slots == null ? 0 : slots.length * 8L;
        for (long[] p : pages) if (p != null) bytes += p.length * 8L;
        if (stringKeyPages != null) for (String[] p : stringKeyPages) if (p != null) bytes += p.length * 4L;
        return bytes;
    }

    static void hash(FrameColumnReader[] in, int rows, Layout layout, long[] hashes) {
        java.util.Arrays.fill(hashes, 0, rows, 0x9E3779B97F4A7C15L);
        for (int k = 0; k < layout.keyCount; k++) {
            FrameColumnReader column = in[layout.keyColumns[k]];
            switch (layout.keyTypes[k]) {
                case LONG -> {
                    for (int row = 0; row < rows; row++)
                        hashes[row] = (hashes[row] ^ (column.isNullAt(row) ? 0x5bd1e995L : column.longValue(row))) * 0xff51afd7ed558ccdL;
                }
                case DOUBLE -> {
                    for (int row = 0; row < rows; row++)
                        hashes[row] = (hashes[row] ^ (column.isNullAt(row) ? 0x5bd1e995L
                                : Double.doubleToLongBits(SparkValues.canonicalDouble(column.doubleValue(row))))) * 0xff51afd7ed558ccdL;
                }
                case STRING -> {
                    for (int row = 0; row < rows; row++)
                        hashes[row] = (hashes[row] ^ (column.isNullAt(row) ? 0x5bd1e995L : column.stringValue(row).hashCode())) * 0xff51afd7ed558ccdL;
                }
                case DECIMAL -> throw new UnsupportedOperationException("prototype: DECIMAL keys");
            }
        }
        for (int row = 0; row < rows; row++) {
            long h = hashes[row];
            h ^= h >>> 33;
            h *= 0xc4ceb9fe1a85ec53L;
            h ^= h >>> 33;
            hashes[row] = h;
        }
    }

    /** Column layout shared by all partitions of one merge. */
    static final class Layout {
        final int keyCount, measureCount, stringKeyCount, stride, flagOffset, hashOffset;
        final int[] keyColumns, keyOffset, measureColumns, measureOffset, measureFlag;
        final ColumnType[] keyTypes, measureTypes;
        final AggregateOperation[] measureOps;

        Layout(ResultFrame schema, List<String> dimensions, Map<String, AggregateOperation> ops) {
            keyCount = dimensions.size();
            keyColumns = new int[keyCount];
            keyTypes = new ColumnType[keyCount];
            keyOffset = new int[keyCount];
            int word = 0, strings = 0;
            for (int k = 0; k < keyCount; k++) {
                keyColumns[k] = schema.columnIndex(dimensions.get(k));
                keyTypes[k] = schema.columnTypeAt(keyColumns[k]);
                keyOffset[k] = keyTypes[k] == ColumnType.STRING ? strings++ : word++;
            }
            stringKeyCount = strings;
            List<Integer> measures = new ArrayList<>();
            for (int c = 0; c < schema.columnNames().size(); c++) if (!dimensions.contains(schema.columnNames().get(c))) measures.add(c);
            measureCount = measures.size();
            measureColumns = new int[measureCount];
            measureTypes = new ColumnType[measureCount];
            measureOffset = new int[measureCount];
            measureFlag = new int[measureCount];
            measureOps = new AggregateOperation[measureCount];
            if (keyCount + measureCount > 64) throw new UnsupportedOperationException("prototype: >64 columns");
            for (int m = 0; m < measureCount; m++) {
                measureColumns[m] = measures.get(m);
                measureTypes[m] = schema.columnTypeAt(measureColumns[m]);
                measureOffset[m] = word++;
                measureFlag[m] = keyCount + m;
                measureOps[m] = ops.get(schema.columnNames().get(measureColumns[m]));
            }
            flagOffset = word++;
            hashOffset = word++;
            stride = word;
        }

        static int partitionOf(long hash, int partitions) {
            return (int) (((hash >>> 40) * partitions) >>> 24);
        }
    }
}
