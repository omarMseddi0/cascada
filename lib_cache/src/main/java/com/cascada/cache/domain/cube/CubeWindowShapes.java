package com.cascada.cache.domain.cube;

import java.util.HashSet;
import java.util.Set;

record CubeWindowShapes(ShapeLatticeIndex index, Set<QueryShape> shapes) {
    CubeWindowShapes() {
        this(new ShapeLatticeIndex(), new HashSet<>());
    }
}
