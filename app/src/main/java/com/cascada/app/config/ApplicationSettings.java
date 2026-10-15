package com.cascada.app.config;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

public record ApplicationSettings(ServerSettings server, WarmingSettings warming, List<TableSettings> tables,
                                  CacheSafetySettings cacheSafety) {
    public ApplicationSettings {
        Objects.requireNonNull(server);
        Objects.requireNonNull(warming);
        Objects.requireNonNull(cacheSafety);
        tables = List.copyOf(tables);
        long names = tables.stream().map(table -> table.logicalName().toLowerCase(Locale.ROOT)).distinct().count();
        if (names != tables.size()) throw new IllegalArgumentException("Table names must be unique");
    }

    public ApplicationSettings(ServerSettings server, WarmingSettings warming, List<TableSettings> tables) {
        this(server, warming, tables, CacheSafetySettings.defaults());
    }

    public static ApplicationSettings defaults() {
        return new ApplicationSettings(ServerSettings.defaults(), WarmingSettings.defaults(), List.of());
    }
}
