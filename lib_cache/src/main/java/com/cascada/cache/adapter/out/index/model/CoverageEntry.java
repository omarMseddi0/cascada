package com.cascada.cache.adapter.out.index.model;

import com.cascada.cache.domain.index.BucketCoverageBitmap;

public record CoverageEntry(BucketCoverageBitmap bitmap, long bytes) { }
