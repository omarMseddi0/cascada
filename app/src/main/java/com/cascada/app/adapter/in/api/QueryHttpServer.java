package com.cascada.app.adapter.in.api;

import com.cascada.app.concurrent.RequestExecutor;
import com.cascada.app.config.ServerSettings;
import com.cascada.app.exception.ApiFailure;
import com.cascada.app.exception.InvalidRequestException;
import com.cascada.cache.application.execution.QueryCancellation;
import com.cascada.cache.application.execution.QueryExecutionScope;
import com.cascada.cache.application.port.in.*;
import com.cascada.cache.application.port.in.ExecuteLogicalQueryUseCase;
import com.cascada.cache.application.port.in.FlushCacheUseCase;
import com.cascada.cache.application.port.in.MeasureCacheSizeUseCase;
import com.cascada.cache.application.port.in.WarmCacheUseCase;
import com.cascada.fabric.application.port.in.ManageClusterLifecycleUseCase;
import com.cascada.fabric.application.port.in.PrepareClusterSettingsUseCase;
import com.cascada.fabric.domain.ClusterValues;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.*;

/** Transport binding only; query, cache and cluster operations use their existing inbound ports. */
public final class QueryHttpServer implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final HttpServer server;
    private final InetSocketAddress address;
    private final RequestExecutor requests;
    private final ExecuteLogicalQueryUseCase queries;
    private final java.util.Set<QueryCancellation> activeQueries = ConcurrentHashMap.newKeySet();
    private final ServerSettings settings;
    private boolean started;

    public QueryHttpServer(int port, ExecuteLogicalQueryUseCase queries) throws IOException {
        this(new ServerSettings(port, port == 9090 ? 9091 : 9090, java.time.Duration.ofSeconds(30)), queries);
    }
    public QueryHttpServer(ServerSettings settings, ExecuteLogicalQueryUseCase queries) throws IOException {
        this.settings = settings;
        this.queries = queries;
        address = new InetSocketAddress(settings.httpPort());
        server = HttpServer.create();
        requests = new RequestExecutor(settings);
        // Dispatch only admission and a tiny health response; request bodies and operations run on workers.
        server.setExecutor(Runnable::run);
        server.createContext("/health", exchange -> {
            try (exchange) {
                if (!exchange.getRequestURI().getPath().equals("/health")) exchange.sendResponseHeaders(404, -1);
                else json(exchange, Map.of("status", "up"));
            }
        });
        route("/query", "POST", this::query);
    }

    public void bindCacheAdministration(MeasureCacheSizeUseCase measure, FlushCacheUseCase flush, WarmCacheUseCase warm) {
        route("/cache/size", "GET", exchange -> {
            var report = measure.measureCacheSize();
            json(exchange, Map.of("totalBytes", report.totalBytes(), "totalMegabytes", report.totalMegabytes(), "bucketCount", report.bucketCount()));
        });
        route("/cache/flush", "POST", exchange -> {
            JsonNode request = readRequest(exchange);
            if (request.has("prefix")) {
                try { com.cascada.cache.domain.admin.CacheScope.forKeyPrefix(requiredText(request, "prefix")); }
                catch (IllegalArgumentException invalid) { throw new InvalidRequestException("prefix must select Cascada bucket keys (QC:V4:)"); }
            }
            long purged = request.has("prefix") ? flush.flushKeyPrefix(requiredText(request, "prefix")) : flush.flushEverything();
            json(exchange, Map.of("bucketsPurged", purged));
        });
        route("/cache/warm", "POST", exchange -> {
            JsonNode request = readRequest(exchange);
            long start = requiredTimestamp(request, "start");
            long end = requiredTimestamp(request, "end");
            if (start > end) throw new InvalidRequestException("start must be <= end");
            json(exchange, warm.warmCycle(start, end, request.path("forceOverwrite").asBoolean(false)));
        });
    }

    public void bindClusterLifecycle(ManageClusterLifecycleUseCase clusters, PrepareClusterSettingsUseCase preparation) {
        route("/cluster/deploy", "POST", exchange -> json(exchange, Map.of("resources", clusters.deploy(clusterValues(exchange, preparation)))));
        route("/cluster/stop", "POST", exchange -> { clusters.stop(clusterValues(exchange, preparation)); json(exchange, Map.of("status", "stopped")); });
        route("/cluster/start", "POST", exchange -> { clusters.start(clusterValues(exchange, preparation)); json(exchange, Map.of("status", "started")); });
        route("/cluster/restart", "POST", exchange -> { clusters.restart(clusterValues(exchange, preparation)); json(exchange, Map.of("status", "restarted")); });
        route("/cluster/delete", "POST", exchange -> { clusters.delete(clusterValues(exchange, preparation)); json(exchange, Map.of("status", "deleted")); });
    }

    private void route(String path, String method, HttpHandler operation) {
        server.createContext(path, exchange -> {
            if (!exchange.getRequestURI().getPath().equals(path)) {
                try (exchange) { exchange.sendResponseHeaders(404, -1); }
                return;
            }
            if (!exchange.getRequestMethod().equals(method)) {
                try (exchange) { exchange.getResponseHeaders().set("Allow", method); exchange.sendResponseHeaders(405, -1); }
                return;
            }
            try { requests.submit(() -> serve(exchange, operation)); }
            catch (RejectedExecutionException busy) {
                try (exchange) { error(exchange, ApiFailure.from(busy)); }
            }
        });
    }

    private void serve(HttpExchange exchange, HttpHandler operation) {
        QueryCancellation cancellation = new QueryCancellation();
        activeQueries.add(cancellation);
        try (exchange; var scope = new QueryExecutionScope(cancellation)) {
            try { operation.handle(exchange); }
            catch (Throwable failure) {
                ApiFailure error = ApiFailure.from(failure);
                if (error.httpStatus() == 500) java.util.logging.Logger.getLogger(getClass().getName())
                        .log(java.util.logging.Level.WARNING, "API operation failed", failure);
                if (exchange.getResponseCode() == -1) error(exchange, error);
                if (failure instanceof Error fatal) throw fatal;
            }
        } catch (IOException disconnected) {
            cancellation.cancel();
        } finally { activeQueries.remove(cancellation); }
    }

    public void start() throws IOException { server.bind(address, 64); server.start(); started = true; }
    public int port() { return server.getAddress().getPort(); }

    private void query(HttpExchange exchange) throws IOException {
        JsonNode request = readRequest(exchange);
        String sql = requiredText(request, "sql");
        String format = request.path("format").asText("json");
        if (!format.equals("json") && !format.equals("parquet")) throw new InvalidRequestException("format must be json or parquet");
        var result = queries.query(sql);
        exchange.getResponseHeaders().set("X-Cascada-Cache", Boolean.toString(result.servedThroughCache()));
        exchange.getResponseHeaders().set("X-Cascada-Row-Count", Integer.toString(result.frame().rowCount()));
        if (format.equals("json")) {
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, 0);
            QueryResponseEncoder.json(result.frame(), exchange.getResponseBody());
        } else {
            try (var parquet = new ParquetResult(QueryResponseEncoder.parquet(result.frame()))) {
                exchange.getResponseHeaders().set("Content-Type", "application/vnd.apache.parquet");
                exchange.getResponseHeaders().set("Content-Disposition", "attachment; filename=result.parquet");
                exchange.sendResponseHeaders(200, Files.size(parquet.path()));
                Files.copy(parquet.path(), exchange.getResponseBody());
            }
        }
    }

    private static JsonNode readRequest(HttpExchange exchange) throws IOException {
        JsonNode request = JSON.readTree(exchange.getRequestBody());
        if (request == null || !request.isObject()) throw new InvalidRequestException("Request must be a JSON object");
        return request;
    }
    private static String requiredText(JsonNode request, String name) {
        if (!request.path(name).isTextual() || request.path(name).asText().isBlank()) throw new InvalidRequestException(name + " is required");
        return request.path(name).asText();
    }
    private static long requiredTimestamp(JsonNode request, String name) {
        if (!request.path(name).isIntegralNumber() || !request.path(name).canConvertToLong()) throw new InvalidRequestException(name + " must be an epoch-second integer");
        return request.path(name).longValue();
    }
    private static ClusterValues clusterValues(HttpExchange exchange, PrepareClusterSettingsUseCase preparation) throws IOException {
        JsonNode request = readRequest(exchange);
        Map<String, String> values = new java.util.LinkedHashMap<>();
        request.fields().forEachRemaining(entry -> {
            if (!entry.getValue().isTextual()) throw new InvalidRequestException("Cluster settings must be strings");
            values.put(entry.getKey(), entry.getValue().textValue());
        });
        try { return preparation.prepareClusterSettings(values); }
        catch (IllegalArgumentException invalid) { throw new InvalidRequestException("Invalid cluster settings"); }
    }
    private static void json(HttpExchange exchange, Object response) throws IOException {
        byte[] body = JSON.writeValueAsBytes(response);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, body.length);
        exchange.getResponseBody().write(body);
    }
    private static void error(HttpExchange exchange, ApiFailure failure) throws IOException {
        byte[] body = JSON.writeValueAsBytes(Map.of("code", failure.code(), "error", failure.message()));
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(failure.httpStatus(), body.length);
        exchange.getResponseBody().write(body);
    }
    @Override public void close() {
        activeQueries.forEach(QueryCancellation::cancel);
        if (started) server.stop(1);
        requests.close();
    }
}
