param([int]$Jobs = 2, [string]$BuildImage = 'apache/gluten:vcpkg-centos-9', [switch]$CleanupOnFailure, [string]$ResumeBuildId='')
$ErrorActionPreference = 'Stop'
if ($Jobs -lt 1 -or $Jobs -gt 2) { throw 'This local build is limited to one or two compilation jobs.' }
if($ResumeBuildId -and $ResumeBuildId -notmatch '^[0-9a-f]{8}$'){throw 'Resume requires the exact owned build id.'}
$buildId = if($ResumeBuildId){$ResumeBuildId}else{[guid]::NewGuid().ToString('N').Substring(0,8)}
$container = "cascada-gluten-build-$buildId"
$volume = "cascada-gluten-build-$buildId"
$output = Join-Path $env:USERPROFILE ".local/share/cascada/gluten-1.7.0-spark4.1-delta-$buildId"
if(!$ResumeBuildId){New-Item -ItemType Directory -Path $output | Out-Null}
function Assert-BuildExit([string]$Step) { if ($LASTEXITCODE -ne 0) { throw "$Step failed (exit $LASTEXITCODE); see $output" } }
$volumeCreated = $false
$containerCreated = $false
$buildSucceeded = $false
try {
    if($ResumeBuildId){
        $containerOwner=& docker inspect --format '{{index .Config.Labels "cascada.gluten-build"}}' $container
        Assert-BuildExit 'Resume container ownership'
        $volumeOwner=& docker volume inspect --format '{{index .Labels "cascada.gluten-build"}}' $volume
        Assert-BuildExit 'Resume volume ownership'
        if($containerOwner.Trim() -ne $buildId -or $volumeOwner.Trim() -ne $buildId){throw 'Unowned build resources'}
        $running=& docker inspect --format '{{.State.Running}}' $container
        Assert-BuildExit 'Resume container state'
        if($running.Trim() -ne 'true'){& docker start $container | Out-Null; Assert-BuildExit 'Restart owned build container'}
        $digest=(Get-Content -LiteralPath (Join-Path $output 'build-request.json') -Raw | ConvertFrom-Json).imageDigest
        $containerCreated=$true;$volumeCreated=$true
    }else{
    & docker pull $BuildImage *> (Join-Path $output 'image-pull.log'); Assert-BuildExit 'Toolchain image pull'
    $digest = (& docker image inspect --format '{{index .RepoDigests 0}}' $BuildImage).Trim(); Assert-BuildExit 'Image digest'
    [ordered]@{buildId=$buildId;imageDigest=$digest;jobs=$Jobs;memoryLimitBytes=13958643712;
        sourceRevision='52f7cc9d3d2536cead21e95a6dc28f36923b7dd6';startedUtc=[DateTime]::UtcNow.ToString('o')} |
        ConvertTo-Json | Set-Content -LiteralPath (Join-Path $output 'build-request.json') -Encoding utf8
    & docker volume create --label "cascada.gluten-build=$buildId" $volume | Out-Null
    Assert-BuildExit 'Build volume'; $volumeCreated = $true
    & docker run -d --name $container --label "cascada.gluten-build=$buildId" --cpus $Jobs --memory 13g --memory-swap 13g `
        --mount "type=volume,source=$volume,target=/workspace" --mount "type=bind,source=$output,target=/output" `
        -e "NUM_THREADS=$Jobs" -e "VCPKG_MAX_CONCURRENCY=$Jobs" -e 'PROMPT_ALWAYS_RESPOND=y' $digest sleep infinity | Out-Null
    Assert-BuildExit 'Build container'; $containerCreated = $true
    }
    $script = @'
set -euo pipefail
source /opt/rh/gcc-toolset-12/enable
java -version
gcc --version
cmake --version
if [ ! -d /workspace/gluten/.git ]; then git clone --depth 1 --branch v1.7.0 https://github.com/apache/gluten.git /workspace/gluten; fi
cd /workspace/gluten
test "$(git rev-parse HEAD)" = 52f7cc9d3d2536cead21e95a6dc28f36923b7dd6
export MAVEN_OPTS='-Xmx2g -XX:ActiveProcessorCount=2'
# Root native builds explicitly use two jobs; serialize nested ExternalProject
# builds so an Arrow child Ninja cannot independently spawn every host CPU.
export CMAKE_BUILD_PARALLEL_LEVEL=1
native_args=(--spark_version=4.1 --num_threads="$NUM_THREADS" --enable_vcpkg=ON --run_setup_script=OFF --build_arrow=OFF --enable_s3=OFF --enable_gcs=OFF --enable_hdfs=OFF --enable_abfs=OFF --build_tests=OFF --build_benchmarks=OFF)
if [ ! -d ep/build-velox/build/velox_ep/.git ]; then
  ./dev/builddeps-veloxbe.sh "${native_args[@]}" get_velox 2>&1 | tee /output/dependencies-build.log
fi
# The pinned Velox Arrow-18 external project defaults to shared compression deps,
# while Gluten's vcpkg triplet supplies static archives. Keep one static Arrow build.
python3 - <<'PY'
from pathlib import Path
p=Path('ep/build-velox/build/velox_ep/CMake/resolve_dependency_modules/arrow/CMakeLists.txt')
s=p.read_text()
needle='    -DARROW_DEPENDENCY_SOURCE=AUTO\n'
if needle not in s: raise RuntimeError('Pinned Arrow CMake argument anchor not found')
if '-DARROW_ZSTD_USE_SHARED=OFF' not in s:
    s=s.replace(needle, needle+'    -DARROW_BUILD_SHARED=OFF\n    -DARROW_DEPENDENCY_USE_SHARED=OFF\n    -DARROW_ZSTD_USE_SHARED=OFF\n',1)
if '-DARROW_FILESYSTEM=ON' not in s:
    s=s.replace(needle, needle+'    -DARROW_FILESYSTEM=ON\n',1)
# Arrow-18's testing utility expects Boost's legacy process API. The pinned
# Velox patch removes Boost::process from its link interface (and its macros),
# so explicitly select the still-provided v1 compatibility headers.
s=s.replace('    -DARROW_CXXFLAGS=-Wno-documentation\n',
            '    "-DARROW_CXXFLAGS=-Wno-documentation -DBOOST_PROCESS_HAVE_V1"\n')
p.write_text(s)
# Velox's Arrow testing patch links filesystem/system for both library types,
# so static Arrow testing must discover the compiled Boost libraries too.
q=Path('ep/build-velox/build/velox_ep/CMake/resolve_dependency_modules/arrow/arrow-testing-boost.patch')
q.write_text(q.read_text().replace('OR (ARROW_TESTING AND ARROW_BUILD_SHARED))','OR ARROW_TESTING)'))
r=Path('ep/build-velox/build/velox_ep/_build/release/CMake/resolve_dependency_modules/arrow/arrow_ep/src/arrow_ep/cpp/cmake_modules/ThirdpartyToolchain.cmake')
if r.exists(): r.write_text(r.read_text().replace('OR (ARROW_TESTING AND ARROW_BUILD_SHARED))','OR ARROW_TESTING)'))
core=Path('cpp/core/CMakeLists.txt')
c=core.read_text()
c=c.replace('find_arrow_lib(${ARROW_BUNDLED_DEPS})',
            'find_package(Arrow CONFIG REQUIRED PATHS "${ARROW_INSTALL_DIR}/lib/cmake/Arrow" NO_DEFAULT_PATH)')
c=c.replace('target_link_libraries(gluten PUBLIC Arrow::arrow\n                                    Arrow::arrow_bundled_dependencies)',
            'target_link_libraries(gluten PUBLIC Arrow::arrow_static)')
core.write_text(c)
backend=Path('cpp/velox/CMakeLists.txt')
b=backend.read_text()
anchor='target_link_libraries(velox PUBLIC facebook::velox)'
if 'find_package(xxHash CONFIG REQUIRED)' not in b:
    if anchor not in b: raise RuntimeError('Velox linkage anchor missing')
    b=b.replace(anchor,anchor+'\nfind_package(xxHash CONFIG REQUIRED)\ntarget_link_libraries(velox PRIVATE xxHash::xxhash)',1)
backend.write_text(b)
PY
git -C ep/build-velox/build/velox_ep diff > /output/velox-build.patch
git diff > /output/gluten-build.patch
./dev/builddeps-veloxbe.sh "${native_args[@]}" build_velox build_gluten_cpp 2>&1 | tee /output/native-build.log
ldd -r cpp/build/releases/libvelox.so 2>&1 | tee /output/native-linkage.log
if grep -Eq 'undefined symbol:|not found' /output/native-linkage.log; then exit 1; fi
./build/mvn clean install -Pbackends-velox -Pdelta -Pspark-4.1 -Pjava-17 -Pscala-2.13 -Dmaven.compiler.release=17 -DskipTests 2>&1 | tee /output/maven-build.log
mapfile -t bundles < <(find package/target -maxdepth 1 -name 'gluten-velox-bundle-spark4.1*.jar' ! -name '*sources*' ! -name '*javadoc*')
test "${#bundles[@]}" = 1
cp "${bundles[0]}" /output/gluten.jar
git -C ep/build-velox/build/velox_ep rev-parse HEAD > /output/velox-revision.txt
cp LICENSE NOTICE /output/
'@
    [IO.File]::WriteAllText((Join-Path $output 'build.sh'), $script.Replace("`r`n", "`n"), [Text.UTF8Encoding]::new($false))
    Write-Host "Building native libraries, then explicit -Pdelta. Logs: $output"
    & docker exec $container bash /output/build.sh *> (Join-Path $output 'build-console.log')
    $buildExit=$LASTEXITCODE
    if($buildExit -ne 0){
        & docker exec $container bash -c 'find /workspace/gluten -type f \( -name "*-err.log" -o -name "*CMakeError.log" -o -name "*stdout-*.log" \) -print -exec tail -n 100 {} \;' *> (Join-Path $output 'failure-diagnostics.log')
        $global:LASTEXITCODE=$buildExit
    }
    Assert-BuildExit 'Native-first Delta bundle build'
    [ordered]@{sparkVersion='4.1.1';deltaVersion='4.1.0';scalaBinaryVersion='2.13';glutenVersion='1.7.0';
        glutenRevision='52f7cc9d3d2536cead21e95a6dc28f36923b7dd6';veloxRevision=(Get-Content (Join-Path $output 'velox-revision.txt')).Trim();
        imageDigest=$digest;profiles=@('backends-velox','delta','spark-4.1','java-17','scala-2.13');jobs=$Jobs;
        nativeArrowVersion='18.0.0';veloxBuildPatchSha512=(Get-FileHash (Join-Path $output 'velox-build.patch') -Algorithm SHA512).Hash;
        glutenBuildPatchSha512=(Get-FileHash (Join-Path $output 'gluten-build.patch') -Algorithm SHA512).Hash;
        nativeFlags=@('enable_vcpkg=ON','build_arrow=OFF (Velox builds its Arrow-18 dependency)',
            'CMAKE_BUILD_PARALLEL_LEVEL=1 (nested projects; root native jobs=2)',
            'ARROW_BUILD_SHARED=OFF','ARROW_DEPENDENCY_USE_SHARED=OFF','ARROW_ZSTD_USE_SHARED=OFF',
            'enable_s3=OFF','enable_gcs=OFF','enable_hdfs=OFF','enable_abfs=OFF','build_tests=OFF');
        sha512=(Get-FileHash (Join-Path $output 'gluten.jar') -Algorithm SHA512).Hash;createdUtc=[DateTime]::UtcNow.ToString('o')} |
        ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $output 'manifest.json') -Encoding utf8
    & (Join-Path $PSScriptRoot 'Assert-GlutenDeltaBundle.ps1') -JarPath (Join-Path $output 'gluten.jar') -ManifestPath (Join-Path $output 'manifest.json')
    $buildSucceeded = $true
    [ordered]@{status='Succeeded';buildId=$buildId;finishedUtc=[DateTime]::UtcNow.ToString('o')} |
        ConvertTo-Json | Set-Content -LiteralPath (Join-Path $output 'build-outcome.json') -Encoding utf8
    Write-Host "DELTA_BUNDLE_READY $output"
} catch {
    [ordered]@{status='Failed';buildId=$buildId;error=$_.Exception.Message;finishedUtc=[DateTime]::UtcNow.ToString('o')} |
        ConvertTo-Json | Set-Content -LiteralPath (Join-Path $output 'build-outcome.json') -Encoding utf8
    throw
} finally {
    if ($containerCreated) {
        & docker exec $container bash -c 'cat /sys/fs/cgroup/memory.events; cat /sys/fs/cgroup/memory.peak' *> (Join-Path $output 'build-memory.log')
        & docker inspect --format '{{json .State}}' $container | Set-Content (Join-Path $output 'container-state.json')
        $owner = & docker inspect --format '{{index .Config.Labels "cascada.gluten-build"}}' $container
        if (($buildSucceeded -or $CleanupOnFailure) -and $owner.Trim() -eq $buildId) { & docker rm -f $container | Out-Null }
    }
    if ($volumeCreated) {
        $owner = & docker volume inspect --format '{{index .Labels "cascada.gluten-build"}}' $volume
        if (($buildSucceeded -or $CleanupOnFailure) -and $owner.Trim() -eq $buildId) { & docker volume rm $volume | Out-Null }
    }
    if (!$buildSucceeded -and !$CleanupOnFailure) {
        Write-Host "FAILED_BUILD_CACHE_RETAINED container=$container volume=$volume output=$output"
    }
}
