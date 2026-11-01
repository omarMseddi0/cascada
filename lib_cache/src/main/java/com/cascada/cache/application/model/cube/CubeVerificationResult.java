package com.cascada.cache.application.model.cube;

public record CubeVerificationResult(boolean isConsistent, String reason) {
    public static CubeVerificationResult consistent() { return new CubeVerificationResult(true, ""); }
    public static CubeVerificationResult inconsistent(String reason) { return new CubeVerificationResult(false, reason); }
}
