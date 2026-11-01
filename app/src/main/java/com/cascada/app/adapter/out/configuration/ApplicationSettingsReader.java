package com.cascada.app.adapter.out.configuration;

import com.cascada.app.model.config.ApplicationSettings;
import com.cascada.app.model.config.CacheSafetySettings;
import com.cascada.app.model.config.ServerSettings;
import com.cascada.app.model.config.TableSettings;
import com.cascada.app.model.config.WarmingSettings;
import com.cascada.app.exception.ConfigurationException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public final class ApplicationSettingsReader {
    private static final ObjectMapper JSON = SettingValues.JSON;
    public ApplicationSettings read(Function<String, String> environment) {
        return read(environment, SettingValues.applicationConfiguration(environment));
    }

    ApplicationSettings read(Function<String, String> environment, JsonNode configuration) {
        ApplicationSettings defaults = ApplicationSettings.defaults();
        try {
            JsonNode server = configuration.path("server");
            validateSection(server, "server");
            validateSection(configuration.path("warming"), "warming");
            validateSection(configuration.path("cacheSafety"), "cacheSafety");
            ServerSettings ports = new ServerSettings(
                    port(environment, "CASCADA_HTTP_PORT", integer(server, "httpPort", defaults.server().httpPort())),
                    port(environment, "CASCADA_GRPC_PORT", integer(server, "grpcPort", defaults.server().grpcPort())),
                    Duration.parse(server.path("shutdownTimeout").asText("PT30S")));
            JsonNode warming = configuration.path("warming");
            WarmingSettings warm = new WarmingSettings(booleanValue(warming, "enabled", true),
                    Duration.parse(warming.path("interval").asText("PT5M")),
                    Duration.parse(warming.path("lookback").asText("P7D")),
                    integer(warming, "maximumPatterns", defaults.warming().maximumPatterns()),
                    warming.path("validationSql").textValue(),
                    Duration.parse(warming.path("popularityRetention")
                            .asText(WarmingSettings.DEFAULT_POPULARITY_RETENTION.toString())));
            JsonNode cacheSafety = configuration.path("cacheSafety");
            CacheSafetySettings safety = new CacheSafetySettings(
                    columns(cacheSafety, "highCardinalityColumns"),
                    columns(cacheSafety, "liquidClusteredFilterColumns"),
                    optionalLong(cacheSafety, "minimumCacheableTimeRangeSeconds"),
                    integer(cacheSafety, "maximumCachedRows", CacheSafetySettings.DEFAULT_MAXIMUM_CACHED_ROWS));
            List<TableSettings> tables = configuration.has("tables")
                    ? JSON.convertValue(configuration.get("tables"), JSON.getTypeFactory().constructCollectionType(List.class, TableSettings.class))
                    : List.of();
            return new ApplicationSettings(ports, warm, tables, safety);
        } catch (IllegalArgumentException | java.time.DateTimeException failure) {
            throw new ConfigurationException("Invalid application configuration", failure);
        }
    }
    private static void validateSection(JsonNode section, String name) {
        if (!section.isMissingNode() && !section.isObject()) throw new IllegalArgumentException(name + " must be an object");
    }
    private static Set<String> columns(JsonNode section, String name) {
        JsonNode values = section.path(name);
        if (!values.isMissingNode() && !values.isArray()) throw new IllegalArgumentException(name + " must be an array");
        return StreamSupport.stream(values.spliterator(), false).map(JsonNode::asText).collect(Collectors.toSet());
    }
    private static Long optionalLong(JsonNode section, String name) {
        if (!section.has(name)) return null;
        if (!section.path(name).isIntegralNumber() || !section.path(name).canConvertToLong())
            throw new IllegalArgumentException(name + " must be an integer");
        return section.path(name).longValue();
    }
    private static boolean booleanValue(JsonNode section, String name, boolean fallback) {
        if (!section.has(name)) return fallback;
        if (!section.path(name).isBoolean()) throw new IllegalArgumentException(name + " must be Boolean");
        return section.path(name).booleanValue();
    }
    private static int integer(JsonNode section, String name, int fallback) {
        if (!section.has(name)) return fallback;
        if (!section.path(name).isIntegralNumber() || !section.path(name).canConvertToInt())
            throw new IllegalArgumentException(name + " must be an integer");
        return section.path(name).intValue();
    }
    /** Port 0 asks the OS for an ephemeral port (used by tests); anything else is a real TCP port. */
    private static int port(Function<String, String> environment, String name, int fallback) {
        return SettingValues.intValue(name, environment.apply(name), fallback, 0, 65_535);
    }
}
