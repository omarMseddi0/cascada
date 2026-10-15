package com.cascada.app.adapter.in.api;

import com.cascada.app.adapter.in.grpc.protocol.*;
import com.cascada.app.exception.ApiFailure;
import com.cascada.app.exception.InvalidRequestException;
import com.cascada.cache.application.port.in.*;
import com.cascada.cache.application.port.in.FlushCacheUseCase;
import com.cascada.cache.application.port.in.MeasureCacheSizeUseCase;
import com.cascada.cache.application.port.in.WarmCacheUseCase;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public final class CacheGrpcService extends CacheServiceGrpc.CacheServiceImplBase {
    private final MeasureCacheSizeUseCase measure;
    private final FlushCacheUseCase flush;
    private final WarmCacheUseCase warm;
    @Override public void measureCache(EmptyRequest request, StreamObserver<CacheSizeResponse> response) {
        respond(response, () -> {
            var report = measure.measureCacheSize();
            return CacheSizeResponse.newBuilder().setTotalBytes(report.totalBytes()).setBucketCount(report.bucketCount())
                    .setTotalMegabytes(report.totalMegabytes()).build();
        });
    }
    @Override public void flushCache(FlushCacheRequest request, StreamObserver<FlushCacheResponse> response) {
        respond(response, () -> {
            if (request.hasPrefix() && request.getPrefix().isBlank()) throw new InvalidRequestException("prefix must not be blank");
            if (request.hasPrefix()) {
                try { com.cascada.cache.domain.admin.CacheScope.forKeyPrefix(request.getPrefix()); }
                catch (IllegalArgumentException invalid) { throw new InvalidRequestException("prefix must select Cascada bucket keys (QC:V4:)"); }
            }
            long purged = request.hasPrefix() ? flush.flushKeyPrefix(request.getPrefix()) : flush.flushEverything();
            return FlushCacheResponse.newBuilder().setBucketsPurged(purged).build();
        });
    }
    @Override public void warmCache(WarmCacheRequest request, StreamObserver<WarmCacheResponse> response) {
        respond(response, () -> {
            if (request.getStart() > request.getEnd()) throw new InvalidRequestException("start must be <= end");
            var report = warm.warmCycle(request.getStart(), request.getEnd(), request.getForceOverwrite());
            return WarmCacheResponse.newBuilder().setPatternsWarmed(report.patternsWarmed())
                    .setBucketsWarmed(report.bucketsWarmed()).setBucketsSkipped(report.bucketsSkipped()).build();
        });
    }
    private static <T> void respond(StreamObserver<T> response, java.util.function.Supplier<T> operation) {
        try { response.onNext(operation.get()); response.onCompleted(); }
        catch (RuntimeException failure) { response.onError(ApiFailure.from(failure).grpcException()); }
    }
}
