package com.cascada.cache.application.model.query;

import com.cascada.cache.domain.model.cube.CubeInterval;
import com.cascada.cache.domain.model.cube.ExactCubeFragment;

/** A complete fragment edge or a Spark residual when the fragment is absent. */
public record CubeCoverEdge(CubeInterval interval, ExactCubeFragment fragment) {}
