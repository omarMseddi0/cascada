package com.cascada.app.model.config;

import java.util.Map;
import java.util.Objects;

/** One startup snapshot of the application configuration and its explicit environment overrides. */
public record DeploymentSettings(EngineSettings engine, ApplicationSettings application, Map<String, String> sparkProperties) {
    public DeploymentSettings {
        Objects.requireNonNull(engine);
        Objects.requireNonNull(application);
        sparkProperties = Map.copyOf(sparkProperties);
    }
}
