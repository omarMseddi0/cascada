package com.cascada.app.adapter.out.configuration;

import com.cascada.app.exception.ConfigurationException;
import com.cascada.app.model.config.DeploymentSettings;
import java.util.Objects;
import java.util.function.Function;

/** Loads the central JSON once; existing environment overrides remain available for deployment compatibility. */
public final class DeploymentSettingsReader {
    public DeploymentSettings read(Function<String, String> environment) {
        Objects.requireNonNull(environment, "environment");
        var configuration = SettingValues.applicationConfiguration(environment);
        try {
            var engine = new EnvironmentSettingsReader(environment).read(configuration);
            var application = new ApplicationSettingsReader().read(environment, configuration);
            var sparkReader = new SparkConfigurationFileReader();
            var spark = configuration.has("spark") ? sparkReader.readSection(configuration.get("spark"))
                    : sparkReader.read(environment.apply("SPARK_CONFIG_PATH"));
            return new DeploymentSettings(engine, application, spark);
        } catch (IllegalArgumentException failure) {
            throw new ConfigurationException("Invalid deployment configuration", failure);
        }
    }
}
