param([Parameter(Mandatory)][string]$JarPath, [Parameter(Mandatory)][string]$ManifestPath)
$ErrorActionPreference = 'Stop'
$manifest = Get-Content -LiteralPath $ManifestPath -Raw | ConvertFrom-Json
if ($manifest.sparkVersion -ne '4.1.1' -or $manifest.scalaBinaryVersion -ne '2.13' -or $manifest.deltaVersion -ne '4.1.0') {
    throw 'Candidate manifest must target Spark 4.1.1 / Scala 2.13 / Delta 4.1.0.'
}
if (!$manifest.sha512 -or (Get-FileHash -LiteralPath $JarPath -Algorithm SHA512).Hash -ne $manifest.sha512) {
    throw 'Delta-enabled Gluten bundle checksum does not match its build manifest.'
}
Add-Type -AssemblyName System.IO.Compression.FileSystem
$archive = [IO.Compression.ZipFile]::OpenRead((Resolve-Path -LiteralPath $JarPath).Path)
try {
    foreach ($entry in @('org/apache/gluten/execution/DeltaScanTransformer.class',
        'org/apache/gluten/extension/OffloadDeltaScan.class',
        'org/apache/gluten/extension/DeltaPostTransformRules$.class',
        'org/apache/gluten/component/VeloxDeltaComponent.class',
        'META-INF/gluten-components/org.apache.gluten.component.VeloxDeltaComponent',
        'gluten-build-info.properties')) {
        if (!$archive.GetEntry($entry)) { throw "Missing $entry : rebuild the complete Velox bundle with -Pdelta." }
    }
    if (!($archive.Entries | Where-Object FullName -Match '(^|/)libgluten\.so$')) { throw 'Native libgluten.so is absent.' }
    $reader = [IO.StreamReader]::new($archive.GetEntry('gluten-build-info.properties').Open())
    try { $metadata = $reader.ReadToEnd() } finally { $reader.Dispose() }
    foreach ($pattern in @('(?m)^backend_type\s*=\s*velox\s*$', '(?m)^spark_version\s*=\s*4\.1\.1\s*$', '(?m)^scala_version\s*=\s*2\.13\.')) {
        if ($metadata -notmatch $pattern) { throw "Bundle build metadata failed compatibility check: $pattern" }
    }
} finally { $archive.Dispose() }
