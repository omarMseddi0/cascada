package com.cascada.cache.domain.warming;

import com.cascada.identity.domain.QueryHash;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class WarmingQueueTest {

    private static final QueryHash A = QueryHash.of("00000000000000000000000000000001");
    private static final QueryHash B = QueryHash.of("00000000000000000000000000000002");

    @Test
    void warmingQueueDeduplicatesVotesAndDrainsInInsertionOrder() {
        WarmingQueue queue = new WarmingQueue();
        queue.vote(A);
        queue.vote(B);
        queue.vote(A);

        assertThat(queue.size()).isEqualTo(2);
        assertThat(queue.consumeAll()).containsExactly(A, B);
        assertThat(queue.size()).isZero();
    }
}
