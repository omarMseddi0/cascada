package com.cascada.cache.application.model.cube;

import com.cascada.cache.domain.model.cube.CachedShapeEntry;
import java.util.BitSet;

public record IndexedShape(CachedShapeEntry entry, BitSet groupByBits, BitSet filterBits) {
}
