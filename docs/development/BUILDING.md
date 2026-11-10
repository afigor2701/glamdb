# Building

## Requirements

This page covers C++. For the implemented Python SDK/mock and browser UI, use
[WORKBENCH.md](WORKBENCH.md). Those packages do not link the C++ core or
require an Arrow/Iceberg source build.

- CMake 3.28 or newer, Ninja, and Python 3.9 or newer.
- Linux and a C++23 compiler **and standard library** with `std::expected`:
  GCC 14+ or Clang 18+ with a matching STL. Clang with an older system
  libstdc++ may lack this feature. Use the same C++ ABI for all dependencies.
- LLVM 18 `clang-format` and `clang-tidy` for quality targets.

## Build and install

First install the [pinned dependencies](../../third_party/README.md) with the
same compiler and C++ standard library used for the engine:

```sh
git submodule update --init --recursive
cmake --preset deps-local
cmake --build --preset deps-local --parallel 2
cmake --preset local -DCMAKE_PREFIX_PATH="$PWD/.deps/install"
cmake --build --preset local
ctest --preset local --no-tests=error
./build/local/apps/glamdb/glamdb --help
```

The default full build provides the catalog/coordinator and public C++ facade.
The CLI creates or inspects an empty store with an explicit identity; it does
not serve HTTP. The real file module is available separately, but connecting it
through `core/engine/storage_backend.h` remains integration work. Public factories
do not yet write user-data files, and the scanner is pending.

CLI11 provides argument parsing and help from its pinned submodule. Run
`glamdb --help` or `glamdb create-store --help` / `glamdb inspect-store --help`
for usage. Both commands require `--root ABSOLUTE_PATH` and `--store-id UUID`;
options may appear in either order and also accept `--name=value` syntax.
No arguments prints help successfully. Invalid command lines exit with code 2
before opening a store; engine errors exit with code 1. Successful commands
retain the store-info JSON output.

Use `release` or `asan-ubsan` in place of `local` for the other build profiles.
For a build without tests or GoogleTest, use a separate directory:

```sh
cmake -S . -B build/no-tests -G Ninja -DBUILD_TESTING=OFF -DCMAKE_PREFIX_PATH="$PWD/.deps/install"
cmake --build build/no-tests
cmake --install build/local
./build/install/local/bin/glamdb --help
```

The install contains the executable, `storage/*.h`, the `glamdb::core` static
library and `GlamDBConfig.cmake`. Consumers use
`find_package(GlamDB CONFIG REQUIRED)` with both GlamDB and dependency prefixes
in `CMAKE_PREFIX_PATH`. `glamdb.installed_consumer` installs to a disposable
prefix and builds/runs a public-header consumer without source-tree headers or
HTTP/Python dependencies. It exercises create/open and missing-collection
results. Scan/predicate acceptance awaits the scanner. The consumer test is
omitted from sanitizer builds because instrumented static libraries require
matching consumer flags.

### Independent local I/O build

`GLAMDB_IO_ONLY=ON` builds `glamdb_io` and its tests without Arrow, Iceberg,
SQLite, the engine, or CLI. It does not install the engine package:

```sh
git submodule update --init third_party/googletest
cmake --preset local -B build/io -DGLAMDB_IO_ONLY=ON -DGLAMDB_BUILD_STORAGE=OFF
cmake --build build/io
ctest --test-dir build/io --output-on-failure --no-tests=error
```

The I/O target contains filesystem access, directory/URI checks, attempts and
receipts, SHA-256, and durable file copying. GoogleTest comes from the pinned
submodule; missing sources produce a configure error with the initialization
command, without downloading during configuration. `BUILD_TESTING=OFF` does
not require GoogleTest. Tooling tests check runner failure propagation, source
isolation, optional tests, static libraries, and the I/O-only install boundary.

## Linux tests with Docker

With a running Docker daemon and the GoogleTest submodule initialized:

```sh
./scripts/test-in-docker.sh
./scripts/test-in-docker.sh asan-ubsan
```

The runner also accepts `release` and selects `GLAMDB_IO_ONLY=ON`. It builds the image from
`docker/dev/Dockerfile` with no filesystem build context, mounts the source
checkout read-only at `/src`, and builds in `/build` inside a disposable
container. Each run configures, builds, requires a nonempty `unit` suite,
runs all CTest suites, then runs formatting, tidy, and negative controls.
Failures stop the command and propagate its exit status. Build outputs are
removed with the container, so Linux and macOS CMake caches cannot collide.
The test container runs without network access; the initial image build needs
network access to download packages.

One macOS setup uses [Colima](https://colima.run/docs/installation/) and a
separate Docker client configuration. Run this once from the repository root:

```sh
brew install colima docker docker-buildx
colima start glamdb --cpus 4 --memory 4 --disk 20 --activate=false --ssh-config=false
export DOCKER_CONFIG="$PWD/artifacts/docker-client"
mkdir -p "$DOCKER_CONFIG/cli-plugins"
ln -s "$(brew --prefix)/lib/docker/cli-plugins/docker-buildx" "$DOCKER_CONFIG/cli-plugins/docker-buildx"
docker context create glamdb --docker "host=unix://$HOME/.colima/glamdb/docker.sock"
docker context use glamdb
./scripts/test-in-docker.sh
./scripts/test-in-docker.sh asan-ubsan
```

In a new shell, export `DOCKER_CONFIG` again before invoking the runner. This
keeps credential helpers and the active context in the default Docker client
configuration unchanged, including settings left by a previous Docker Desktop
installation. The isolated configuration is for this public development image;
it does not inherit registry credentials.
`colima stop glamdb` releases the VM's CPU/RAM when finished. A separate Docker
context avoids changing the active context for other projects.

The image pins the Ubuntu 24.04 multi-architecture base digest and uses Clang
and LLVM quality tools 18. It runs on the daemon's native architecture, including
ARM64 on Apple Silicon. Distribution packages are not snapshot-pinned: this is
a development environment, not Linux x86-64/ext4 power-loss qualification.
Toolchain versions and executed results belong in the validation
record for each change.

### Storage adapters

Initialize and build the pinned Linux dependencies once, then use the storage runner:

```sh
git submodule update --init third_party/arrow third_party/iceberg-cpp third_party/googletest
./scripts/build-deps-in-docker.sh
./scripts/test-storage-in-docker.sh
./scripts/test-storage-in-docker.sh asan-ubsan
./scripts/test-storage-in-docker.sh release
```

`test-storage-in-docker.sh` reuses `glamdb-deps:clang18` and the installed prefix
from the `glamdb-deps-clang18` volume. It does not rebuild dependencies. Set
`GLAMDB_DEPS_VOLUME` to select another compatible prefix. The prefix and checkout
are mounted read-only; each offline run uses a fresh `/build` and at most two
compile jobs. All unit and CTest suites and the three quality targets run with
`GLAMDB_IO_ONLY=OFF` and `GLAMDB_BUILD_STORAGE=ON`, including catalog/coordinator
and storage components and their tests.
The [storage module handoff](STORAGE_MODULE.md) maps components to tests and
defines the responsibilities of the catalog/coordinator and scan callers.

With a compatible installed prefix, the equivalent local configuration is:

```sh
cmake --preset local -DGLAMDB_IO_ONLY=OFF -DGLAMDB_BUILD_STORAGE=ON -DCMAKE_PREFIX_PATH=/path/to/prefix
cmake --build --preset local
ctest --preset local --no-tests=error
```

Use the prefix's compiler and C++ standard library. Linux dependency binaries
cannot be linked into a macOS build. The Docker runner handles this separation.

This runner uses Clang 18 with libc++ and statically links the C++ runtime using
`-static-libstdc++`. This avoids the independently reproduced Ubuntu libc++abi
exception mismatch under ASan without suppressing sanitizer checks. See
[dependency validation](DEPENDENCY_VALIDATION.md) for the build pins, installed
API probe, runtime issue, and development environment limits. The regular
`test-in-docker.sh` selects the independent I/O-only build.

Only the storage `asan-ubsan` profile also defines `_LIBCPP_HAS_NO_ASAN`. The
installed dependency prefix is not instrumented; mixing its libc++ container
templates with instrumented callers produces a reproducible container-overflow
report in the upstream Avro manifest writer. This is the
[documented mixed instrumentation problem](https://github.com/google/sanitizers/wiki/AddressSanitizerContainerOverflow).
The flag disables libc++ container bounds annotations. ASan allocation bounds,
use-after-free checks and UBSan remain enabled for GlamDB; the ordinary IO-only
ASan runner retains its full instrumentation. No runtime suppression is set.

The standalone probe uses only installed upstream APIs. One test requires the
original container-overflow report, while another checks the same program with
the compatibility flag. Run it against the development prefix:

```sh
docker run --rm --network none --volume "$PWD:/src:ro" \
    --mount "type=volume,src=${GLAMDB_DEPS_VOLUME:-glamdb-deps-clang18},dst=/prefix,readonly,volume-subpath=install" \
    glamdb-deps:clang18 sh -eu -c '
        cmake -S /src/probes/toolchain -B /build -G Ninja -DCMAKE_PREFIX_PATH=/prefix
        cmake --build /build --parallel 2
        ctest --test-dir /build --output-on-failure --no-tests=error -L toolchain-repro
        clang-tidy-18 /src/probes/toolchain/libcxx_asan.cpp -p /build --config-file=/src/.clang-tidy
    '
```

Its compilation database is separate from the normal build, so root tidy checks
exclude `probes/toolchain`; root formatting still checks it. This probe captures
the Clang/libc++ 18 development compatibility issue, not a general toolchain
qualification test.

## Test CI

`Native tests` runs six independent jobs: normal, ASan, UBSan, MSan, TSan,
and standalone LSan. A failing profile does not cancel the others. Each enables
storage and `BUILD_TESTING`, builds all native test targets, and runs CTest without
label/name filters. Installed-consumer tests remain included because the CI
toolchain propagates instrumentation to consumer builds too.

The normal job additionally runs three named contours from `scripts/ci-native.sh`:
`vertical` (`ctest -L vertical`, the public API over the real storage backend),
`nostorage` (a separate `GLAMDB_BUILD_STORAGE=OFF`, `GLAMDB_IO_ONLY=OFF` build and
its full CTest run with the fake coordinator backend) and `mock` (`ctest -L mock`,
in-memory planner/cursor tests run from a read-only working and temporary
directory). The read-only directory catches accidental writes there; it is not a
syscall-level no-I/O proof.

CMake recursively discovers `.cpp` files inside the unit, I/O, storage, mock,
vertical and fault suites, with `CONFIGURE_DEPENDS`; GoogleTest discovers their test cases. Add new
cases or test files there without editing CI. Tooling Python files are discovered
by unittest. Native integration drivers remain registered in CMake because their
binary arguments differ. New standalone CMake projects under `probes/` are also
discovered. The mixed-annotation compatibility probe runs only in the normal job:
it deliberately compares ASan callers against an uninstrumented dependency prefix.

`Workbench checks` has independent Python, Node, and Playwright jobs. Pytest searches
the repository, excluding vendor/generated trees, UI, and native tooling; native
`*_test.py` drivers run through CTest instead. Node discovers `*.test.mjs` files;
Playwright discovers browser specs under `ui/tests/e2e`. New files following these
conventions need no CI list entry. `bash scripts/check-workbench.sh` runs all three
suites locally even if an earlier suite fails; an optional `python`, `node`, or
`browser` argument selects one suite.

Both workflows run on PRs, pushes to `main`, and manual dispatch. They skip changes
limited to `docs/`, Markdown, issue templates, CODEOWNERS, and LICENSE. These are
exclusions rather than a source-directory allowlist, so new code directories still
trigger tests. Manual dispatch bypasses these filters. Do not require a skipped
workflow as an unconditional branch-protection check.

### Instrumentation and reproduction

All profiles use Clang 18. ASan, UBSan, and LSan instrument the project and test
binaries with an ordinary dependency prefix. ASan uses the same documented
`_LIBCPP_HAS_NO_ASAN` compatibility flag as the existing storage Docker runner;
heap bounds, use-after-free and leak detection remain enabled. UBSan aborts on
the first report. LSan has its own job in addition to ASan's leak detection.
Sanitizer jobs select Arrow's system memory pool so its ordinary mimalloc build
cannot bypass sanitizer allocation hooks.

MSan and TSan use a separate dependency prefix and rebuild libc++/libc++abi from
LLVM 18.1.8, pinned in the workflow. Instrumentation propagates to Arrow, Iceberg,
SQLite, GoogleTest, and transitive source dependencies. Iceberg/Avro's zlib and
zstd lookups use separately instrumented builds with Arrow's existing versions
and checksums. Compiled FetchContent dependencies cannot substitute system packages.
MSan also disables Arrow's mimalloc and jemalloc at build time. Selecting the
system pool at runtime alone does not stop a linked allocator's pre-main
initialization; the pinned mimalloc reports an uninitialized THP buffer there.
Arrow itself remains instrumented, with no MSan suppressions.
MSan tracks origins. These profiles dynamically link the instrumented C++ runtime;
the others use the existing static C++ runtime workaround. Standard-library paths
are explicit, and no sanitizer error is ignored or retried. Hosted MSan/TSan jobs
set `vm.mmap_rnd_bits=28` for LLVM 18's shadow-memory layout.

On a Linux machine with the packages from the workflow installed, initialize the
submodules, then run (replace `normal` with the desired profile):

```sh
bash scripts/ci-native.sh normal deps
bash scripts/ci-native.sh normal build
bash scripts/ci-native.sh normal test
```

For MSan/TSan, first check out the workflow's pinned LLVM source into
`.deps/llvm-project` and run `bash scripts/ci-native.sh msan runtime` (or `tsan`).
Builds and dependency prefixes are isolated by profile. The toolchain is CI-only;
existing developer presets and Docker commands are unchanged.

### Failure logs

Open the failed job and its failed step in GitHub Actions. Compiler output,
sanitizer stack traces (with LLVM symbolization), and failing test output are
streamed to the step log. CTest failures also print the complete `LastTest.log`
so CTest's console-size limit cannot hide diagnostics. Download the workflow's
log archive from GitHub to retain the output. Shell pipelines preserve failing
exit codes; no job uses `continue-on-error`.

Local copies and JUnit reports are written under `artifacts/native-<profile>/`
and `artifacts/`. Browser runs retain traces/screenshots locally under
`artifacts/e2e/`. These workspace files are not uploaded as Actions artifacts;
the repository currently permits only checkout and setup-python Actions.

## Quality

Follow the [C++ style guide](CPP_STYLE_GUIDE.md). `.clang-format` implements
its mechanical rules; naming, ownership, and error handling also need review.

The [C++ quality workflow](../../.github/workflows/cpp-quality.yml) runs independent
`clang-format` and `clang-tidy` jobs. Formatting runs on pull requests, pushes to
`main`, and manual dispatch. Tidy runs only on pushes to `main` and manual dispatch;
it is skipped on pull requests. Formatting runs without a build. Tidy prepares the pinned
dependencies with Clang/libc++ 18, then configures `local` with
`GLAMDB_BUILD_STORAGE=ON`, `GLAMDB_BUILD_IO_PROBE=ON` and `BUILD_TESTING=ON` to check
all enabled project components and tests. It also runs `quality-negative-check`.
Module scanning is disabled for this analysis-only configuration so compile
commands do not reference `.modmap` files that require a project build.

Tidy checks all translation units using two concurrent processes, preserving each
file's diagnostics and reporting its elapsed time. Set `GLAMDB_TIDY_JOBS=1` to
serialize or use `python3 scripts/quality.py tidy-check --jobs N`. The default of
two limits memory use on hosted runners; increase it only when memory permits.
A failed translation unit still fails the job, after the remaining files finish.

CI caches only `.deps/install`, using an exact key for dependency pins, build
recipes, OS/architecture, compiler versions/targets, installed package versions,
build flags and the installation path. Submodule pins are verified even on a hit.
A miss, partial match or restore failure rebuilds from clean dependency build
and installation directories. A completed installation is saved before analysis,
so a later code diagnostic does not discard it. Cache service failures do not
skip the build or analysis. The first run of a new key still builds dependencies;
PR caches remain scoped to that PR, while the default branch can supply shared
caches. Run the cache-decision tests with:

```sh
python3 tests/tooling/test_ci_dependencies.py
```

```sh
cmake --build --preset local --target format-check tidy-check quality-negative-check
cmake --build --preset local --target format
```

Tools must report LLVM major version 18. Versioned binaries such as
`clang-tidy-18` take precedence over unversioned binaries. Override their paths
with `GLAMDB_CLANG_FORMAT` and `GLAMDB_CLANG_TIDY`.

Checks cover project sources and headers, excluding generated and third-party
files. C++ formatting uses a 120-column limit. `tidy-check` uses
`compile_commands.json` and rejects missing translation
units. `quality-negative-check` verifies that a formatting violation and a null
dereference in a project header both fail their respective checks.
Run `tidy-check` from a configuration with `BUILD_TESTING=ON` so test translation
units have compile commands too.

## Dependencies

See [the dependency guide](../../third_party/README.md). The separate
`deps-local` build installs dependencies into `.deps/install`. GoogleTest is built
once from its pinned submodule by the project test configuration; it is not
built or installed by the dependency superbuild, and GoogleMock is disabled.

The initial durability qualification targets Linux x86-64 on a dedicated local
ext4 volume. The development module and its tests do not qualify that environment.
The [qualification preparation guide](QUALIFICATION.md) includes a read-only
environment collector and the evidence required before running those campaigns.

## Validation scope

| Check | What it establishes |
| --- | --- |
| Default build and `glamdb.smoke` | The engine and store-management CLI build and the entry point exits successfully. |
| I/O-only `unit` suite | Local I/O, receipts, directory/URI checks, SHA-256, copying, and injected failure behavior. |
| Catalog/coordinator unit and VFS fault suites | Canonical input, SQLite transactions, bindings/outcomes and coordinator states with a storage test double. |
| `glamdb.process_recovery` | Separate-process reopen, interrupted operations, hot-journal recovery and exclusive ownership. |
| `glamdb.real_sqlcatalog` | Real Iceberg create/property-update ordering, outer rollback, exact base and fresh-process load with test FileIO. |
| `glamdb.installed_consumer` | Installed CMake package and public facade work without private headers. |
| `unit` suite, storage enabled | Adds FileIO, table profiles, real file writing/staging, publication/closure/recovery, and capacity tests. |
| `tooling` suite | Docker runner profiles, isolation and failure propagation; optional GoogleTest, static core and install contents. |
| Formatting, tidy, and negative controls | Configured C++ checks execute and reject deliberate violations. |
| Installed dependency probe | Pinned dependencies build/install and their public APIs work from a relocated installed prefix. |
| Storage pipeline test | Real Parquet/Avro/JSON append and fresh-process reachable-file recovery with a catalog test double. |
| Future combined integration | Production backend wiring, real public append/read, and scanner contracts. |
| Future environment qualification | Durability and supported limits for the specifically tested platform and storage configuration. |

Fault injection and a storage pipeline are implemented in the component suites.
The combined production backend, scanner, load measurements, and environment/
power-loss qualification remain deferred. See [COORDINATOR.md](COORDINATOR.md)
and [the module handoff](STORAGE_MODULE.md) for their separate validation scope.
Independent fixture, receipt/model and network-client/browser suites run
separately, as recorded in [WORKBENCH_VALIDATION.md](WORKBENCH_VALIDATION.md).
They do not qualify production file/scanner integration or physical durability.
The [roadmap](../ROADMAP.md) defines remaining evidence. Describe checks as
designed, run with PASS/FAIL, or NOT RUN; a configured target alone is not a pass.
Do not use proposed test labels or executable names as if they exist already.

Use `local`, `release`, and `asan-ubsan` with the configure/build/test commands
above. Lifetime changes require sanitizer
coverage; reopen and recovery require a fresh process. The eventual qualification
also needs calibrated fault modeling and dedicated-volume power cuts, which
ordinary CTest or a macOS build cannot establish on its own.

Record the toolchain and dependency pins, command, exit code, environment,
fixture/seed, and measured results for reproducibility. Store raw generated
output under ignored `artifacts/` and retain a reviewable summary or stable
artifact reference with the change. Keep commands and this status description
current as targets and suites are added.

See [E0 validation](E0_VALIDATION.md) for the executed scaffold checks and scope.
