package com.cascada.app.adapter.in.api.model;

import java.util.List;

/** A cluster lifecycle request's result: its status and, for a deploy, the applied resources. */
public record ClusterChangeResult(String status, List<String> resources) { }
