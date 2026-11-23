package com.cascada.app.bootstrap;

import com.cascada.app.model.config.EngineSettings;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.fixture.ValkeyTestDatabase;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = ValkeyTestDatabase.ENVIRONMENT_VARIABLE, matches = ".+")
class CascadaRuntimeTest {
    @RegisterExtension
    final ValkeyTestDatabase valkey = new ValkeyTestDatabase();

    private EngineSettings settings() {
        var defaults = EngineSettings.localDefaults();
        return new EngineSettings(defaults.cacheExecution(), valkey.uri(), defaults.mainTableName(),
                defaults.mainTablePath(), defaults.useLocalSpark(), defaults.warmingTopNQueries());
    }

    @Test void startsAndStopsTheApiWithoutCreatingSpark() throws Exception {
        AtomicInteger sparkStarts = new AtomicInteger();
        CascadaRuntime runtime = new CascadaRuntime(settings(), () -> {
            sparkStarts.incrementAndGet(); return sql -> ResultFrame.empty();
        }, 0, 0);
        try (runtime) {
            runtime.start();
            var response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(
                    URI.create("http://localhost:" + runtime.httpPort() + "/health")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(runtime.grpcPort()).isPositive();
            assertThat(sparkStarts).hasValue(0);
        }
        runtime.close();
    }
    @Test void aFailedGrpcBindClosesTheAlreadyBoundHttpServer() throws Exception {
        int httpPort;
        try (ServerSocket port = new ServerSocket(0)) { httpPort = port.getLocalPort(); }
        try (ServerSocket occupied = new ServerSocket(0)) {
            try (CascadaRuntime runtime = new CascadaRuntime(settings(),
                    () -> sql -> ResultFrame.empty(), httpPort, occupied.getLocalPort())) {
                assertThatThrownBy(runtime::start).isInstanceOf(java.io.IOException.class);
            }
        }
        try (ServerSocket rebound = new ServerSocket(httpPort)) { assertThat(rebound.isBound()).isTrue(); }
    }
}
