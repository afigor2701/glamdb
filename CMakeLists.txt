cmake_minimum_required(VERSION 3.28)

project(GlamDB VERSION 0.1.0 LANGUAGES C CXX)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_BINARY_DIR)
    message(FATAL_ERROR "Use an out-of-source build: cmake --preset local")
endif()

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
include(CTest)

option(GLAMDB_BUILD_DEPENDENCIES "Build and install pinned dependencies" OFF)
if(GLAMDB_BUILD_DEPENDENCIES)
    add_subdirectory(cmake/dependencies)
    return()
endif()

include(GNUInstallDirs)
option(GLAMDB_IO_ONLY "Build independent IO and telemetry without the engine and CLI" OFF)
# The engine needs the storage adapters for collections, appends and scans.
if(GLAMDB_IO_ONLY)
    set(glamdb_storage_default OFF)
else()
    set(glamdb_storage_default ON)
endif()
option(GLAMDB_BUILD_STORAGE "Build the Arrow/Iceberg storage adapters" ${glamdb_storage_default})
option(GLAMDB_BUILD_IO_PROBE "Build the storage I/O fault probe" OFF)
option(GLAMDB_BUILD_ADMIN "Build the offline store administration utility" OFF)
if(GLAMDB_BUILD_IO_PROBE AND NOT GLAMDB_BUILD_STORAGE)
    message(FATAL_ERROR "GLAMDB_BUILD_IO_PROBE requires GLAMDB_BUILD_STORAGE=ON")
endif()
option(GLAMDB_BUILD_METRICS "Build native telemetry in IO-only mode" OFF)
option(GLAMDB_BUILD_LOGGING "Build Quill logging in IO-only mode" OFF)
include(cmake/CompilerOptions.cmake)
include(cmake/Sanitizers.cmake)
include(cmake/Quality.cmake)
if(DEFINED GLAMDB_ENABLE_CACHE_TEST_HOOKS)
    option(GLAMDB_ENABLE_CACHE_TEST_HOOKS
        "Enable TSnapshotMetadataCache allocator fault-injection hooks" OFF)
    if(GLAMDB_ENABLE_CACHE_TEST_HOOKS)
        target_compile_definitions(glamdb_build_options INTERFACE GLAMDB_ENABLE_CACHE_TEST_HOOKS=1)
    endif()
elseif(BUILD_TESTING)
    target_compile_definitions(glamdb_build_options INTERFACE
        "$<$<NOT:$<CONFIG:Release>>:GLAMDB_ENABLE_CACHE_TEST_HOOKS=1>")
endif()

add_subdirectory(core)
if(NOT GLAMDB_IO_ONLY)
    add_subdirectory(apps)
endif()
if(BUILD_TESTING)
    add_subdirectory(tests)
endif()
if(GLAMDB_BUILD_IO_PROBE)
    add_subdirectory(probes/io)
endif()

add_custom_target(dependency-check
    COMMAND "${CMAKE_COMMAND}" -P
            "${PROJECT_SOURCE_DIR}/cmake/dependencies/VerifySubmodules.cmake"
    VERBATIM
)
