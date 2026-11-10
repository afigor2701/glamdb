#!/usr/bin/env bash
set -euo pipefail

if [[ $# != 2 || ! $1 =~ ^(normal|asan|ubsan|msan|tsan|lsan)$ || ! $2 =~ ^(runtime|deps|build|test)$ ]]; then
  echo "Usage: $0 {normal|asan|ubsan|msan|tsan|lsan} {runtime|deps|build|test}" >&2
  exit 2
fi
profile=$1
stage=$2
root=$(cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"
build="$root/build/ci-$profile"
prefix="$root/.deps/ci-$profile/install"
logs="$root/artifacts/native-$profile"
mkdir -p "$logs"
exec > >(tee "$logs/$stage.log") 2>&1
trap 'result=$?; if (( result != 0 )); then echo "FAILED: $profile/$stage (exit $result); see the command output above."; fi' EXIT
export GLAMDB_CI_PROFILE="$profile" GLAMDB_CI_PHASE="$stage" GLAMDB_CI_PREFIX="$prefix"
export CC=clang-18 CXX=clang++-18 CXXFLAGS=
export ASAN_SYMBOLIZER_PATH=/usr/bin/llvm-symbolizer-18
export MSAN_SYMBOLIZER_PATH=/usr/bin/llvm-symbolizer-18
export ASAN_OPTIONS=detect_leaks=1:halt_on_error=1
export UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1
export MSAN_OPTIONS=print_stats=1:halt_on_error=1:exit_code=86
export TSAN_OPTIONS=halt_on_error=1:second_deadlock_stack=1:exitcode=87
export LSAN_OPTIONS=exitcode=88:report_objects=1
if [[ $profile != normal ]]; then
  # Alternative allocators in ordinary Arrow builds can bypass sanitizer hooks.
  export ARROW_DEFAULT_MEMORY_POOL=system
fi
if [[ $profile == msan || $profile == tsan ]]; then
  export GLAMDB_CI_RUNTIME_PREFIX="$root/.deps/ci-$profile/runtime"
fi
configure=(-G Ninja "-DCMAKE_TOOLCHAIN_FILE=$root/cmake/ci/clang.cmake"
  "-DCMAKE_PREFIX_PATH=$prefix" -DCMAKE_BUILD_TYPE=Debug)

case "$stage" in
  runtime)
    if [[ $profile != msan && $profile != tsan ]]; then
      exit 0
    fi
    llvm_sanitizer=MemoryWithOrigins
    if [[ $profile == tsan ]]; then llvm_sanitizer=Thread; fi
    # Build the standard library before using the CI toolchain that requires it.
    cmake -S .deps/llvm-project/runtimes -B "$build-runtime" -G Ninja \
      -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang-18 -DCMAKE_CXX_COMPILER=clang++-18 \
      "-DCMAKE_INSTALL_PREFIX=$GLAMDB_CI_RUNTIME_PREFIX" -DCMAKE_INSTALL_LIBDIR=lib \
      '-DLLVM_ENABLE_RUNTIMES=libcxx;libcxxabi' "-DLLVM_USE_SANITIZER=$llvm_sanitizer" \
      -DLLVM_INCLUDE_TESTS=OFF -DLIBCXX_INCLUDE_TESTS=OFF -DLIBCXXABI_INCLUDE_TESTS=OFF \
      -DLIBCXX_INCLUDE_BENCHMARKS=OFF \
      -DLIBCXXABI_USE_LLVM_UNWINDER=OFF
    cmake --build "$build-runtime" --parallel 2
    cmake --install "$build-runtime"
    ;;
  deps)
    if [[ $profile == msan || $profile == tsan ]]; then
      # Reuse Arrow's pinned zlib version/checksum for Iceberg's system-zlib lookup.
      version_file=third_party/arrow/cpp/thirdparty/versions.txt
      zlib_version=$(sed -n 's/^ARROW_ZLIB_BUILD_VERSION=//p' "$version_file")
      zlib_sha=$(sed -n 's/^ARROW_ZLIB_BUILD_SHA256_CHECKSUM=//p' "$version_file")
      source_dir="$root/.deps/ci-$profile/zlib-source"
      mkdir -p "$source_dir"
      curl --fail --silent --show-error --location --retry 3 \
        "https://zlib.net/fossils/zlib-$zlib_version.tar.gz" -o "$source_dir/zlib.tar.gz"
      printf '%s  %s\n' "$zlib_sha" "$source_dir/zlib.tar.gz" | sha256sum --check --strict
      tar -xzf "$source_dir/zlib.tar.gz" -C "$source_dir" --strip-components=1
      cmake -S "$source_dir" -B "$build-zlib" "${configure[@]}" \
        "-DCMAKE_INSTALL_PREFIX=$prefix" -DCMAKE_INSTALL_LIBDIR=lib
      cmake --build "$build-zlib" --parallel 2
      cmake --install "$build-zlib"
      # Avro also resolves zstd independently of Arrow's bundled copy.
      zstd_version=$(sed -n 's/^ARROW_ZSTD_BUILD_VERSION=//p' "$version_file")
      zstd_sha=$(sed -n 's/^ARROW_ZSTD_BUILD_SHA256_CHECKSUM=//p' "$version_file")
      source_dir="$root/.deps/ci-$profile/zstd-source"
      mkdir -p "$source_dir"
      curl --fail --silent --show-error --location --retry 3 \
        "https://github.com/facebook/zstd/releases/download/v$zstd_version/zstd-$zstd_version.tar.gz" \
        -o "$source_dir/zstd.tar.gz"
      printf '%s  %s\n' "$zstd_sha" "$source_dir/zstd.tar.gz" | sha256sum --check --strict
      tar -xzf "$source_dir/zstd.tar.gz" -C "$source_dir" --strip-components=1
      cmake -S "$source_dir/build/cmake" -B "$build-zstd" "${configure[@]}" \
        "-DCMAKE_INSTALL_PREFIX=$prefix" -DCMAKE_INSTALL_LIBDIR=lib \
        -DZSTD_BUILD_PROGRAMS=OFF -DZSTD_BUILD_TESTS=OFF -DZSTD_BUILD_SHARED=OFF
      cmake --build "$build-zstd" --parallel 2
      cmake --install "$build-zstd"
    fi
    cmake --preset deps-local -B "$build-deps" "${configure[@]}" \
      -DCMAKE_BUILD_TYPE=Release "-DGLAMDB_DEPENDENCY_PREFIX=$prefix" \
      "-DCMAKE_INSTALL_PREFIX=$prefix" -DGLAMDB_DEPENDENCY_JOBS=2
    cmake --build "$build-deps" --parallel 2
    ;;
  build)
    cmake --preset local -B "$build" "${configure[@]}" \
      -DGLAMDB_BUILD_STORAGE=ON -DGLAMDB_BUILD_IO_PROBE=ON -DGLAMDB_IO_ONLY=OFF -DBUILD_TESTING=ON \
      -DCMAKE_GTEST_DISCOVER_TESTS_DISCOVERY_MODE=PRE_TEST
    cmake --build "$build" --parallel 2
    if [[ $profile == normal ]]; then
      # The engine without storage adapters: public factories install no
      # backend; coordinator tests use the fake storage double.
      cmake --preset local -B "$build-nostorage" "${configure[@]}" \
        -DGLAMDB_BUILD_STORAGE=OFF -DGLAMDB_IO_ONLY=OFF -DBUILD_TESTING=ON \
        -DCMAKE_GTEST_DISCOVER_TESTS_DISCOVERY_MODE=PRE_TEST
      cmake --build "$build-nostorage" --parallel 2
    fi
    ;;
  test)
    result=0
    run_tests() {
      local directory=$1 name=$2
      shift 2
      ctest --test-dir "$directory" --output-on-failure --no-tests=error \
        --parallel 2 --output-log "$logs/$name-ctest.log" \
        --output-junit "$logs/$name-junit.xml" "$@" || {
          result=1
          # CTest's console output can be truncated; retain the complete diagnostic.
          if [[ -f $directory/Testing/Temporary/LastTest.log ]]; then
            cat "$directory/Testing/Temporary/LastTest.log"
          fi
        }
    }
    run_tests "$build" project
    if [[ $profile == normal ]]; then
      # Named contours: (1) public API over the real storage backend,
      # (2) the engine built without storage, (3) in-memory planner/cursor
      # tests with a read-only working and temporary directory.
      run_tests "$build" vertical -L vertical
      run_tests "$build-nostorage" nostorage
      readonly_dir=$(mktemp -d)
      chmod 0555 "$readonly_dir"
      (cd "$readonly_dir" && export TMPDIR="$readonly_dir" && run_tests "$build" mock -L mock \
        && exit "$result") || result=1
      chmod 0755 "$readonly_dir" && rmdir "$readonly_dir"
    fi
    # Discover standalone CMake test projects; no CI list of individual tests.
    for project in probes/*/CMakeLists.txt; do
      [[ -f $project ]] || continue
      source_dir=${project%/CMakeLists.txt}
      name=${source_dir##*/}
      # The I/O probe is a root-owned target, not a standalone CMake project.
      if [[ $name == io ]]; then continue; fi
      # This regression probe intentionally compares mixed ASan annotations;
      # its baseline must remain the ordinary, uninstrumented dependency prefix.
      if [[ $name == toolchain && $profile != normal ]]; then continue; fi
      if cmake -S "$source_dir" -B "$build-probe-$name" "${configure[@]}" \
          && cmake --build "$build-probe-$name" --parallel 2; then
        run_tests "$build-probe-$name" "$name"
      else
        result=1
      fi
    done
    exit "$result"
    ;;
esac
