# Single source of the JVM module flags for scripts: the root pom's `agrona.jvm.args` property.
# Dot-source this file and call Get-CascadaJvmOptions; the Dockerfile and the Kubernetes
# deployment template carry the same list and are checked against the pom by JvmOptionsConsistencyTest.
function Get-CascadaJvmOptions {
    $pom = Join-Path (Split-Path -Parent $PSScriptRoot) 'pom.xml'
    [xml]$project = Get-Content -LiteralPath $pom -Raw
    $value = $project.project.properties.'agrona.jvm.args'
    if ([string]::IsNullOrWhiteSpace($value)) { throw "agrona.jvm.args is missing from $pom" }
    return @($value.Trim() -split '\s+')
}
