package com.cascada.app.adapter.in.api;

import com.cascada.app.adapter.in.grpc.protocol.*;
import com.cascada.app.exception.ApiFailure;
import com.cascada.app.exception.InvalidRequestException;
import com.cascada.fabric.application.port.in.ManageClusterLifecycleUseCase;
import com.cascada.fabric.application.port.in.PrepareClusterSettingsUseCase;
import com.cascada.fabric.domain.ClusterValues;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public final class ClusterGrpcService extends ClusterServiceGrpc.ClusterServiceImplBase {
    private final ManageClusterLifecycleUseCase clusters;
    private final PrepareClusterSettingsUseCase preparation;
    @Override public void changeCluster(ClusterRequest request, StreamObserver<ClusterResponse> response) {
        try {
            ClusterValues values = values(request);
            var result = ClusterResponse.newBuilder();
            switch (request.getOperation()) {
                case DEPLOY -> result.addAllResources(clusters.deploy(values)).setStatus("deployed");
                case STOP -> { clusters.stop(values); result.setStatus("stopped"); }
                case START -> { clusters.start(values); result.setStatus("started"); }
                case RESTART -> { clusters.restart(values); result.setStatus("restarted"); }
                case DELETE -> { clusters.delete(values); result.setStatus("deleted"); }
                default -> throw new InvalidRequestException("A cluster operation is required");
            }
            response.onNext(result.build()); response.onCompleted();
        } catch (RuntimeException failure) { response.onError(ApiFailure.from(failure).grpcException()); }
    }
    private ClusterValues values(ClusterRequest request) {
        try { return preparation.prepareClusterSettings(request.getSettingsMap()); }
        catch (IllegalArgumentException invalid) { throw new InvalidRequestException("Invalid cluster settings"); }
    }
}
