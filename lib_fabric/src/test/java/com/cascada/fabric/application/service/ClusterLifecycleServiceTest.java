package com.cascada.fabric.application.service;

import com.cascada.fabric.application.port.out.ClusterManifestRenderingPort;
import com.cascada.fabric.application.port.out.ClusterOrchestratorPort;
import com.cascada.fabric.domain.ClusterValues;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterLifecycleServiceTest {

    private static final List<String> MANIFESTS = List.of("service-account", "deployment");

    @Test
    void lifecycleCoordinatesRenderingAndClusterOperationsThroughPorts() {
        List<String> events = new ArrayList<>();
        RecordingRenderer renderer = new RecordingRenderer(events);
        RecordingOrchestrator orchestrator = new RecordingOrchestrator(events);
        ClusterLifecycleService service = new ClusterLifecycleService(renderer, orchestrator);
        ClusterValues values = ClusterValues.defaults();

        assertThat(service.deploy(values)).containsExactly("Deployment/cascada-driver-copy1");
        service.stop(values);
        service.start(values);
        service.restart(values);
        service.delete(values);

        assertThat(events).containsExactly(
                "render", "apply",
                "scale:default:cascada-driver-copy1:0",
                "scale:default:cascada-driver-copy1:1",
                "restart:default:cascada-driver-copy1",
                "render", "delete");
        assertThat(orchestrator.applied).isEqualTo(MANIFESTS);
        assertThat(orchestrator.deleted).isEqualTo(MANIFESTS);
    }

    private static final class RecordingRenderer implements ClusterManifestRenderingPort {

        private final List<String> events;

        private RecordingRenderer(List<String> events) {
            this.events = events;
        }

        @Override
        public List<String> render(ClusterValues values) {
            events.add("render");
            return MANIFESTS;
        }
    }

    private static final class RecordingOrchestrator implements ClusterOrchestratorPort {

        private final List<String> events;
        private List<String> applied;
        private List<String> deleted;

        private RecordingOrchestrator(List<String> events) {
            this.events = events;
        }

        @Override
        public List<String> apply(List<String> manifestDocuments) {
            events.add("apply");
            applied = manifestDocuments;
            return List.of("Deployment/cascada-driver-copy1");
        }

        @Override
        public void delete(List<String> manifestDocuments) {
            events.add("delete");
            deleted = manifestDocuments;
        }

        @Override
        public void scaleWorkload(String namespace, String workloadName, int replicas) {
            events.add("scale:" + namespace + ":" + workloadName + ":" + replicas);
        }

        @Override
        public void restartWorkload(String namespace, String workloadName) {
            events.add("restart:" + namespace + ":" + workloadName);
        }
    }
}
