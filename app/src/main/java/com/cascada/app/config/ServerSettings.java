package com.cascada.app.config;

import java.time.Duration;

public record ServerSettings(int httpPort, int grpcPort, Duration shutdownTimeout) {
    public ServerSettings {
        if (httpPort < 0 || httpPort > 65535 || grpcPort < 0 || grpcPort > 65535
                || (httpPort != 0 && httpPort == grpcPort)
                || shutdownTimeout == null || shutdownTimeout.isZero() || shutdownTimeout.isNegative())
            throw new IllegalArgumentException("Invalid server ports or shutdown timeout");
    }
    public static ServerSettings defaults() { return new ServerSettings(8000, 9090, Duration.ofSeconds(30)); }
}
