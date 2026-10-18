package com.cascada.app.adapter.in.api;

import com.cascada.app.adapter.in.grpc.protocol.*;
import com.cascada.app.model.api.ApiFailure;
import com.cascada.app.exception.InvalidRequestException;
import com.cascada.fabric.application.port.in.ManageClusterLifecycleUseCase;
import com.cascada.fabric.application.port.in.PrepareClusterSettingsUseCase;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public final class ClusterGrpcService extends ClusterServiceGrpc.ClusterServiceImplBase {
    private final ManageClusterLifecycleUseCase clusters;
    private final PrepareClusterSettingsUseCase preparation;
    @Override public void changeCluster(ClusterRequest request, StreamObserver<ClusterResponse> response) {
        try {
            ClusterChange operation = switch (request.getOperation()) {
                case DEPLOY -> ClusterChange.DEPLOY;
                case STOP -> ClusterChange.STOP;
                case START -> ClusterChange.START;
                case RESTART -> ClusterChange.RESTART;
                case DELETE -> ClusterChange.DELETE;
                default -> throw new InvalidRequestException("A cluster operation is required");
            };
            var result = AdministrationRequests.changeCluster(clusters, preparation, operation, request.getSettingsMap());
            response.onNext(ClusterResponse.newBuilder().setStatus(result.status())
                    .addAllResources(result.resources()).build());
            response.onCompleted();
        } catch (RuntimeException failure) { response.onError(ApiFailure.from(failure).grpcException()); }
    }
}
