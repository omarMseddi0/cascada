package com.cascada.cache.application.service.merge;

import com.cascada.cache.domain.frame.FrameColumnReader;

/**
 * Reusable producer-owned routing storage. Consumers read disjoint row ranges after publication;
 * the producer must await every consumer before changing any field or array.
 */
final class PartitionedFrame {
    FrameColumnReader[] columns;
    long[] hashes = new long[0];
    int[] rows = new int[0];
    final int[] offsets;

    PartitionedFrame(int partitionCount) {
        offsets = new int[partitionCount + 1];
    }
}
