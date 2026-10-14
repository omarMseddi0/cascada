package com.cascada.cache.adapter.out.sketch;

import com.cascada.cache.application.port.out.DistinctSketchAccumulator;
import java.util.Base64;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.Union;

/**
 * One group's HLL union. Sketches arrive Base64-encoded (Spark's {@code base64}, chunked or not).
 * The union is created only when a second sketch arrives, so single-bucket groups hold one sketch.
 */
final class HllSketchUnion implements DistinctSketchAccumulator {

    private static final Base64.Decoder DECODER = Base64.getMimeDecoder();

    private final int logConfigK;
    private HllSketch single;
    private Union union;

    HllSketchUnion(int logConfigK) {
        this.logConfigK = logConfigK;
    }

    @Override
    public void add(String encodedSketch) {
        HllSketch sketch = decode(encodedSketch);
        if (union != null) {
            union.update(sketch);
        } else if (single == null) {
            single = sketch;
        } else {
            union = new Union(logConfigK);
            union.update(single);
            union.update(sketch);
            single = null;
        }
    }

    @Override
    public long estimate() {
        if (union != null) {
            return Math.round(union.getEstimate());
        }
        if (single != null) {
            return Math.round(single.getEstimate());
        }
        return 0L;
    }

    private static HllSketch decode(String encodedSketch) {
        byte[] bytes = DECODER.decode(encodedSketch);
        try {
            return HllSketch.heapify(bytes);
        } catch (LinkageError unsupportedRuntime) {
            // datasketches-memory 3.0.x refuses JDKs newer than 21. Report a merge failure so the
            // engine answers from Spark instead of failing the request with an Error.
            throw new IllegalStateException("HLL sketches cannot be read on this JVM", unsupportedRuntime);
        }
    }
}
