package com.cascada.cache.adapter.out.model.index;

import com.cascada.cache.domain.index.BucketCoverageBitmap;

public record CoverageEntry(BucketCoverageBitmap bitmap, long bytes) { }
