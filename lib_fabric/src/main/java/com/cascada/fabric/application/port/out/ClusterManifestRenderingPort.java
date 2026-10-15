package com.cascada.fabric.application.port.out;

import com.cascada.fabric.domain.ClusterValues;

import java.util.List;

/** Renders deployment values into apply-ready manifest documents. */
public interface ClusterManifestRenderingPort {

    List<String> render(ClusterValues values);
}
