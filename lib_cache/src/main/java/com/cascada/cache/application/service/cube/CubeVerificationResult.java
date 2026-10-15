package com.cascada.cache.application.service.cube;

public record CubeVerificationResult(boolean isConsistent, String reason) {
    static CubeVerificationResult consistent() { return new CubeVerificationResult(true, ""); }
    static CubeVerificationResult inconsistent(String reason) { return new CubeVerificationResult(false, reason); }
}
