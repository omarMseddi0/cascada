package com.cascada.cache.application.service.cube;

import com.cascada.cache.domain.cube.QueryShape;
import com.cascada.cache.domain.frame.ResultFrame;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

record CubeWindowShapes(ShapeLatticeIndex index, Set<QueryShape> shapes, Map<QueryShape, ResultFrame> exactAnswers) {
    CubeWindowShapes() {
        this(new ShapeLatticeIndex(), new HashSet<>(), new java.util.HashMap<>());
    }
}
