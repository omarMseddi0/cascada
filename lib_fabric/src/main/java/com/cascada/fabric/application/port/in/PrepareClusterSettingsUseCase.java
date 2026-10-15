package com.cascada.fabric.application.port.in;

import com.cascada.fabric.domain.ClusterValues;
import java.util.Map;

public interface PrepareClusterSettingsUseCase {
    ClusterValues prepareClusterSettings(Map<String, String> settings);
}
