param(
    [long]$Rows = 1000000,
    [int]$HeapMb = 1536,
    [switch]$UnitTests,
    [string]$Filter = '',
    [string]$TestMethod = '',
    [string]$TestClass = 'LocalSparkConsistencyIntegrationTest',
    [switch]$Clean,
    [string]$JavaHome = ''
)
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
if (!$JavaHome) {
    $installedJavaHome = Join-Path $env:USERPROFILE '.local/share/cascada/jdk-21'
    $JavaHome = if (Test-Path (Join-Path $installedJavaHome 'bin/java.exe')) { $installedJavaHome } else { $env:JAVA_HOME }
}
$javaExe = Join-Path $JavaHome 'bin/java.exe'
$mavenHome = Split-Path -Parent (Split-Path -Parent (Get-Command mvn).Source)
$launcher = (Get-ChildItem -LiteralPath (Join-Path $mavenHome 'boot') -Filter 'plexus-classworlds-*.jar' | Select-Object -First 1).FullName
$jvmArgs = @("-Xmx${HeapMb}m", '-XX:ActiveProcessorCount=2', '--enable-native-access=ALL-UNNAMED',
    '--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED', '--add-opens=java.base/java.util.zip=ALL-UNNAMED',
    '--add-opens=java.base/java.nio=ALL-UNNAMED', '--add-opens=java.base/java.lang=ALL-UNNAMED',
    '--add-opens=java.base/java.io=ALL-UNNAMED', '--add-opens=java.base/java.util=ALL-UNNAMED',
    '--add-opens=java.base/sun.nio.ch=ALL-UNNAMED', '--add-opens=java.base/sun.util.calendar=ALL-UNNAMED')
# Run Maven in this JVM: the Windows cmd AutoRun configuration breaks Surefire's fork wrapper here.
$mavenArgs = @('-B', '-Plocal-spark,validation', '-pl', 'app', '-am', 'test', '-DforkCount=0',
    '-Dsurefire.failIfNoSpecifiedTests=false', '-Dmaven.compiler.fork=true', '-Dmaven.compiler.maxmem=512m')
if ($Clean) {
    foreach ($module in @('lib_identity','lib_cache','lib_spark_config','lib_fabric','lib_sql','lib_spark','app')) {
        $resolvedTarget = [IO.Path]::GetFullPath((Join-Path $repoRoot "$module/target"))
        if (!$resolvedTarget.StartsWith($repoRoot + [IO.Path]::DirectorySeparatorChar)) { throw 'Unexpected Maven clean target' }
    }
    $mavenArgs = @('-B', '-Plocal-spark,validation', '-pl', 'app', '-am', 'clean', 'test', '-DforkCount=0',
        '-Dsurefire.failIfNoSpecifiedTests=false', '-Dmaven.compiler.fork=true', '-Dmaven.compiler.maxmem=512m')
}
if (!$UnitTests) {
    $selection = $TestClass
    if ($TestMethod) { $selection += "#$TestMethod" }
    $mavenArgs += @('-Dcascada.spark.integration=true', "-Dcascada.consistency.rows=$Rows",
        "-Dcascada.consistency.filter=$Filter", "-Dtest=$selection")
}
Push-Location $repoRoot
$previousJavaHome = $env:JAVA_HOME
try {
    $env:JAVA_HOME = $JavaHome
    & $javaExe @jvmArgs -classpath $launcher "-Dclassworlds.conf=$mavenHome/bin/m2.conf" "-Dmaven.home=$mavenHome" "-Dmaven.multiModuleProjectDirectory=$repoRoot" org.codehaus.plexus.classworlds.launcher.Launcher @mavenArgs
    if ($LASTEXITCODE -ne 0) { throw "Consistency verification failed: exit $LASTEXITCODE" }
} finally { $env:JAVA_HOME = $previousJavaHome; Pop-Location }
