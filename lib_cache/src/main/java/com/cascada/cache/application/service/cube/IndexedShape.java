package com.cascada.cache.application.service.cube;

import com.cascada.cache.domain.cube.CachedShapeEntry;
import java.util.BitSet;

record IndexedShape(CachedShapeEntry entry, BitSet groupByBits, BitSet filterBits) {
}
