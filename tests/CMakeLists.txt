if(NOT EXISTS "${PROJECT_SOURCE_DIR}/third_party/googletest/CMakeLists.txt")
    message(FATAL_ERROR
        "Unit tests require the pinned GoogleTest submodule. "
        "Run: git submodule update --init third_party/googletest "
        "(or configure with -DBUILD_TESTING=OFF)."
    )
endif()

set(BUILD_GMOCK OFF)
set(INSTALL_GTEST OFF)
add_subdirectory("${PROJECT_SOURCE_DIR}/third_party/googletest"
                 "${PROJECT_BINARY_DIR}/third_party/googletest" EXCLUDE_FROM_ALL SYSTEM)
include(GoogleTest)
add_subdirectory(unit)

# The compiler-configuration tests take the active compiler through the environment.
add_test(NAME glamdb.tooling
    COMMAND "${CMAKE_COMMAND}" -E env
        "CC=${CMAKE_C_COMPILER}" "CXX=${CMAKE_CXX_COMPILER}"
        "GLAMDB_TEST_CMAKE=${CMAKE_COMMAND}" "GLAMDB_TEST_GENERATOR=${CMAKE_GENERATOR}"
        "GLAMDB_TEST_TOOLCHAIN=${CMAKE_TOOLCHAIN_FILE}" "GLAMDB_TEST_PREFIX=${CMAKE_PREFIX_PATH}"
        "GLAMDB_TEST_C_FLAGS=${CMAKE_C_FLAGS}" "GLAMDB_TEST_CXX_FLAGS=${CMAKE_CXX_FLAGS}"
        "GLAMDB_TEST_EXE_LINKER_FLAGS=${CMAKE_EXE_LINKER_FLAGS}"
        "${Python3_EXECUTABLE}" -m unittest discover
        -s "${CMAKE_CURRENT_SOURCE_DIR}/tooling" -p "test_*.py" -v
)
set_tests_properties(glamdb.tooling PROPERTIES LABELS "tooling" TIMEOUT 240)

if(NOT GLAMDB_IO_ONLY)
    add_test(NAME glamdb.smoke COMMAND glamdb_cli)
    set_tests_properties(glamdb.smoke PROPERTIES LABELS "smoke" TIMEOUT 10)

    file(GLOB_RECURSE unit_test_sources CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/unit/*.cpp")
    list(FILTER unit_test_sources EXCLUDE REGEX "/unit/(io|storage)/|/unit/(core_test|metadata_cache_oom_test)\\.cpp$")
    add_executable(glamdb_unit ${unit_test_sources} support/fake_storage.cpp)
    target_link_libraries(glamdb_unit PRIVATE glamdb::core GTest::gtest_main glamdb_build_options
        SQLite::SQLite3 iceberg::iceberg_sql_catalog_static Arrow::arrow_static glamdb_json)
    target_include_directories(glamdb_unit PRIVATE "${PROJECT_SOURCE_DIR}")
    target_compile_definitions(glamdb_unit PRIVATE "GLAMDB_TEST_STORAGE=$<BOOL:${GLAMDB_BUILD_STORAGE}>")
    gtest_discover_tests(glamdb_unit TEST_PREFIX glamdb.unit. PROPERTIES LABELS unit TIMEOUT 30)

    if(GLAMDB_BUILD_STORAGE)
        add_executable(glamdb_metadata_cache_oom unit/metadata_cache_oom_test.cpp)
        target_link_libraries(glamdb_metadata_cache_oom PRIVATE glamdb::core glamdb_storage GTest::gtest_main
            glamdb_build_options Arrow::arrow_static)
        target_include_directories(glamdb_metadata_cache_oom PRIVATE "${PROJECT_SOURCE_DIR}")
        target_compile_definitions(glamdb_metadata_cache_oom PRIVATE "GLAMDB_TEST_STORAGE=1")
        gtest_discover_tests(glamdb_metadata_cache_oom TEST_PREFIX glamdb.unit. PROPERTIES LABELS unit TIMEOUT 30)
    endif()

    if(GLAMDB_BUILD_STORAGE)
        # Planner, cursor and storage error classification over an in-memory
        # FileIO: real encodings in RAM, no filesystem access (label mock).
        file(GLOB_RECURSE mock_test_sources CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/mock/*.cpp")
        add_executable(glamdb_mock ${mock_test_sources})
        target_include_directories(glamdb_mock PRIVATE "${PROJECT_SOURCE_DIR}")
        target_link_libraries(glamdb_mock PRIVATE glamdb::core glamdb_storage glamdb_build_options GTest::gtest_main)
        gtest_discover_tests(glamdb_mock TEST_PREFIX glamdb.mock. PROPERTIES LABELS "mock;scan" TIMEOUT 30)

        # Public API with the real storage backend: Parquet, Iceberg metadata and SQLite.
        file(GLOB_RECURSE vertical_test_sources CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/vertical/*.cpp")
        add_executable(glamdb_vertical ${vertical_test_sources} support/fault_fs.cpp support/journal_fault_vfs.cpp)
        target_include_directories(glamdb_vertical PRIVATE "${PROJECT_SOURCE_DIR}")
        target_link_libraries(glamdb_vertical PRIVATE glamdb::core glamdb_storage glamdb_build_options
            GTest::gtest_main SQLite::SQLite3)
        # Release builds skip the staged-root round-trip parse by default; the
        # public-API storage tests always verify it.
        gtest_discover_tests(glamdb_vertical TEST_PREFIX glamdb.vertical.
            PROPERTIES LABELS "vertical;scan" TIMEOUT 60 ENVIRONMENT "GLAMDB_VERIFY_ROUNDTRIP=1")

        add_executable(glamdb_scan_index_bench "${PROJECT_SOURCE_DIR}/benchmarks/scan_index.cpp" support/fault_fs.cpp)
        target_include_directories(glamdb_scan_index_bench PRIVATE "${PROJECT_SOURCE_DIR}")
        target_link_libraries(glamdb_scan_index_bench PRIVATE glamdb::core glamdb_storage glamdb_build_options)

        add_executable(glamdb_capacity_bench "${PROJECT_SOURCE_DIR}/benchmarks/capacity.cpp")
        set_target_properties(glamdb_capacity_bench PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/benchmarks")
        target_include_directories(glamdb_capacity_bench PRIVATE "${PROJECT_SOURCE_DIR}")
        target_include_directories(glamdb_capacity_bench SYSTEM PRIVATE "${PROJECT_SOURCE_DIR}/third_party/json/single_include")
        target_link_libraries(glamdb_capacity_bench PRIVATE glamdb::core glamdb_storage glamdb_build_options)
        add_test(NAME glamdb.capacity_campaign COMMAND "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/integration/capacity_test.py" "$<TARGET_FILE:glamdb_capacity_bench>")
        set_tests_properties(glamdb.capacity_campaign PROPERTIES LABELS "integration;capacity" TIMEOUT 180)

        add_executable(glamdb_parquet_settings_bench "${PROJECT_SOURCE_DIR}/benchmarks/parquet_settings.cpp")
        target_include_directories(glamdb_parquet_settings_bench PRIVATE "${PROJECT_SOURCE_DIR}")
        target_link_libraries(glamdb_parquet_settings_bench PRIVATE glamdb::core glamdb_build_options Arrow::arrow_static)

        add_executable(glamdb_scan_data_reads_bench
            "${PROJECT_SOURCE_DIR}/benchmarks/scan_data_reads.cpp" support/fault_fs.cpp)
        target_include_directories(glamdb_scan_data_reads_bench PRIVATE "${PROJECT_SOURCE_DIR}")
        target_link_libraries(glamdb_scan_data_reads_bench PRIVATE glamdb::core glamdb_storage glamdb_build_options)
    endif()

    file(GLOB_RECURSE fault_test_sources CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/fault/*.cpp")
    add_executable(glamdb_fault ${fault_test_sources} support/fake_storage.cpp support/journal_fault_vfs.cpp)
    target_include_directories(glamdb_fault PRIVATE "${PROJECT_SOURCE_DIR}")
    target_link_libraries(glamdb_fault PRIVATE glamdb::core glamdb_build_options GTest::gtest_main
        SQLite::SQLite3 iceberg::iceberg_sql_catalog_static Arrow::arrow_static)
    gtest_discover_tests(glamdb_fault TEST_PREFIX glamdb.fault. PROPERTIES LABELS fault TIMEOUT 30)

    if(TARGET glamdb_admin)
        add_executable(glamdb_admin_result_test admin/result_test.cpp)
        target_link_libraries(glamdb_admin_result_test PRIVATE glamdb_admin_commands glamdb_build_options GTest::gtest_main)
        target_compile_definitions(glamdb_admin_result_test PRIVATE
            GLAMDB_OUTCOME_FIXTURE="${PROJECT_SOURCE_DIR}/fixtures/http/outcomes-v0.json")
        gtest_discover_tests(glamdb_admin_result_test TEST_PREFIX glamdb.admin_result. PROPERTIES LABELS unit TIMEOUT 30)
        add_test(NAME glamdb.admin COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/integration/cli_test.py" "$<TARGET_FILE:glamdb_admin>")
        set_tests_properties(glamdb.admin PROPERTIES LABELS integration TIMEOUT 30)
    endif()
    add_executable(glamdb_recovery_driver integration/recovery_driver.cpp support/fake_storage.cpp)
    target_include_directories(glamdb_recovery_driver PRIVATE "${PROJECT_SOURCE_DIR}")
    target_link_libraries(glamdb_recovery_driver PRIVATE glamdb_json glamdb_build_options SQLite::SQLite3
        iceberg::iceberg_sql_catalog_static Arrow::arrow_static)
    add_test(NAME glamdb.process_recovery COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/integration/recovery_test.py" "$<TARGET_FILE:glamdb_recovery_driver>")
    set_tests_properties(glamdb.process_recovery PROPERTIES LABELS integration TIMEOUT 90)
    add_executable(glamdb_sqlcatalog_driver integration/sqlcatalog_driver.cpp)
    target_include_directories(glamdb_sqlcatalog_driver PRIVATE "${PROJECT_SOURCE_DIR}")
    target_link_libraries(glamdb_sqlcatalog_driver PRIVATE glamdb::core glamdb_build_options
        iceberg::iceberg_sql_catalog_static iceberg::iceberg_bundle_static SQLite::SQLite3)
    add_test(NAME glamdb.real_sqlcatalog COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/integration/sqlcatalog_test.py" "$<TARGET_FILE:glamdb_sqlcatalog_driver>")
    set_tests_properties(glamdb.real_sqlcatalog PROPERTIES LABELS integration TIMEOUT 90)
    if(GLAMDB_BUILD_STORAGE)
        add_executable(glamdb_storage_catalog_driver integration/storage_catalog_driver.cpp)
        target_include_directories(glamdb_storage_catalog_driver PRIVATE "${PROJECT_SOURCE_DIR}")
        target_link_libraries(glamdb_storage_catalog_driver PRIVATE glamdb::core glamdb_storage glamdb_build_options
            iceberg::iceberg_sql_catalog_static SQLite::SQLite3)
        add_test(NAME glamdb.durable_storage_catalog COMMAND "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/integration/sqlcatalog_test.py" "$<TARGET_FILE:glamdb_storage_catalog_driver>")
        set_tests_properties(glamdb.durable_storage_catalog PROPERTIES LABELS integration TIMEOUT 90)
        add_executable(glamdb_scan_driver integration/scan_driver.cpp)
        target_include_directories(glamdb_scan_driver PRIVATE "${PROJECT_SOURCE_DIR}")
        target_link_libraries(glamdb_scan_driver PRIVATE glamdb::core glamdb_storage glamdb_build_options)
        add_test(NAME glamdb.scan_process COMMAND "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/integration/scan_process_test.py" "$<TARGET_FILE:glamdb_scan_driver>")
        set_tests_properties(glamdb.scan_process PROPERTIES LABELS "integration;scan" TIMEOUT 120)
    endif()
    add_executable(glamdb_public_consumer_compile installed_consumer/main.cpp)
    target_link_libraries(glamdb_public_consumer_compile PRIVATE glamdb::core glamdb_build_options Arrow::arrow_static)
    if(NOT GLAMDB_ENABLE_ASAN AND NOT GLAMDB_ENABLE_UBSAN)
        add_test(NAME glamdb.installed_consumer COMMAND "${CMAKE_COMMAND}"
            "-DENGINE_BINARY_DIR=${PROJECT_BINARY_DIR}"
            "-DTEST_BINARY_DIR=${PROJECT_BINARY_DIR}/installed-consumer"
            "-DTEST_SOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/installed_consumer"
            "-DDEPENDENCY_PREFIX=${CMAKE_PREFIX_PATH}"
            "-DTEST_GENERATOR=${CMAKE_GENERATOR}"
            "-DTEST_CXX_FLAGS=${CMAKE_CXX_FLAGS}"
            "-DTEST_LINKER_FLAGS=${CMAKE_EXE_LINKER_FLAGS}"
            "-DTEST_TOOLCHAIN=${CMAKE_TOOLCHAIN_FILE}"
            "-DTEST_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
            "-DTEST_STORAGE_ENABLED=${GLAMDB_BUILD_STORAGE}"
            -P "${CMAKE_CURRENT_SOURCE_DIR}/installed_consumer/run.cmake")
        set_tests_properties(glamdb.installed_consumer PROPERTIES LABELS installed TIMEOUT 120)
    endif()
endif()

if(TARGET glamdb_server)
    add_executable(glamdb_observability_probe logging/operation_probe.cpp)
    target_include_directories(glamdb_observability_probe PRIVATE "${PROJECT_SOURCE_DIR}" "${PROJECT_SOURCE_DIR}/public")
    target_link_libraries(glamdb_observability_probe PRIVATE glamdb_metrics glamdb_logging glamdb_build_options)
    add_test(NAME glamdb.server_observability COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/server/observability_test.py" "$<TARGET_FILE:glamdb_server>"
        --probe "$<TARGET_FILE:glamdb_observability_probe>")
    set_tests_properties(glamdb.server_observability PROPERTIES LABELS "integration;http;telemetry" TIMEOUT 120)
    add_test(NAME glamdb.native_metrics COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/server/metrics_test.py" "$<TARGET_FILE:glamdb_server>")
    set_tests_properties(glamdb.native_metrics PROPERTIES LABELS "integration;http;metrics" TIMEOUT 120)
    set(GLAMDB_TEST_CLI_EXECUTABLE "" CACHE FILEPATH "Optional external CLI for server integration tests")
    set(glamdb_test_cli "$<TARGET_FILE:glamdb_cli>")
    if(GLAMDB_TEST_CLI_EXECUTABLE)
        set(glamdb_test_cli "${GLAMDB_TEST_CLI_EXECUTABLE}")
    endif()
    if(TARGET glamdb_admin)
        add_test(NAME glamdb.cli_data COMMAND "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/client/cli_data_test.py"
            "${glamdb_test_cli}" "$<TARGET_FILE:glamdb_admin>" "$<TARGET_FILE:glamdb_server>")
        set_tests_properties(glamdb.cli_data PROPERTIES LABELS "integration;http;cli" TIMEOUT 180)
        if(GLAMDB_ENABLE_MSAN)
            # System OpenSSL is not instrumented. Other network scenarios use
            # the ordinary standalone CLI selected above; admin stays native.
            set_property(TEST glamdb.cli_data APPEND PROPERTY ENVIRONMENT "GLAMDB_TEST_ADMIN_HTTP=0")
        endif()
    endif()
    add_test(NAME glamdb.native_metadata COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/server/native_metadata_test.py"
        "$<TARGET_FILE:glamdb_server>" "${glamdb_test_cli}" "${PROJECT_SOURCE_DIR}/ui")
    set_tests_properties(glamdb.native_metadata PROPERTIES LABELS "integration;http;metadata" TIMEOUT 180)
    add_test(NAME glamdb.native_http COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/server/native_http_test.py"
        "$<TARGET_FILE:glamdb_server>" "${glamdb_test_cli}" "${PROJECT_SOURCE_DIR}/ui")
    add_test(NAME glamdb.http_review COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/server/http_review_test.py"
        "$<TARGET_FILE:glamdb_server>" "${glamdb_test_cli}" "${PROJECT_SOURCE_DIR}/ui")
    set_tests_properties(glamdb.http_review PROPERTIES LABELS "integration;http" TIMEOUT 120)
    add_test(NAME glamdb.http_security COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/server/http_security_test.py"
        "$<TARGET_FILE:glamdb_server>" "${glamdb_test_cli}" "${PROJECT_SOURCE_DIR}/ui")
    set_tests_properties(glamdb.http_security PROPERTIES LABELS "integration;http" TIMEOUT 120)
    add_test(NAME glamdb.database_limits COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/server/database_limits_test.py" "$<TARGET_FILE:glamdb_server>")
    set_tests_properties(glamdb.database_limits PROPERTIES LABELS "integration;http" TIMEOUT 120)
    set_tests_properties(glamdb.native_http PROPERTIES LABELS "integration;http" TIMEOUT 120)
    add_test(NAME glamdb.cli_network COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/client/cli_network_test.py"
        "${glamdb_test_cli}" "$<TARGET_FILE:glamdb_server>")
    set_tests_properties(glamdb.cli_network PROPERTIES LABELS "integration;http;cli" TIMEOUT 90)
    add_test(NAME glamdb.cli_workload_bridge COMMAND "${Python3_EXECUTABLE}"
        "${PROJECT_SOURCE_DIR}/tools/workload/tests/bridge_test.py" "${glamdb_test_cli}")
    set_tests_properties(glamdb.cli_workload_bridge PROPERTIES LABELS "integration;cli" TIMEOUT 30)
    add_test(NAME glamdb.scan_http COMMAND "${CMAKE_COMMAND}" -E env
        "PYTHONPATH=${PROJECT_SOURCE_DIR}/sdk/python/src"
        "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/server/scan_http_test.py"
        "$<TARGET_FILE:glamdb_server>" "${glamdb_test_cli}" "${PROJECT_SOURCE_DIR}/ui"
        "$<TARGET_FILE:glamdb_resource_profile_driver>")
    set_tests_properties(glamdb.scan_http PROPERTIES LABELS "integration;http;scan" TIMEOUT 420)
    add_test(NAME glamdb.scan_policy COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/server/scan_policy_test.py"
        "$<TARGET_FILE:glamdb_server>" "${glamdb_test_cli}" "${PROJECT_SOURCE_DIR}/ui")
    set_tests_properties(glamdb.scan_policy PROPERTIES LABELS "integration;http;scan;resources" TIMEOUT 240)
endif()
if(TARGET glamdb_transport)
    find_package(OpenSSL REQUIRED COMPONENTS Crypto)
    add_executable(glamdb_transport_test
        server/transport_test.cpp
        server/outcome_conformance_test.cpp
        server/arrow_ipc_test.cpp
        server/json_codec_test.cpp
    )
    target_link_libraries(glamdb_transport_test PRIVATE
        glamdb_transport
        glamdb_build_options
        GTest::gtest_main
        OpenSSL::Crypto
    )
    gtest_discover_tests(glamdb_transport_test TEST_PREFIX glamdb.transport.
        PROPERTIES LABELS "unit;http" TIMEOUT 120)
    # Manual codec micro-benchmark; not a CTest case.
    add_executable(glamdb_json_codec_bench server/json_codec_bench.cpp)
    target_link_libraries(glamdb_json_codec_bench PRIVATE glamdb_transport glamdb_build_options)
endif()

if(TARGET glamdb_routes)
    add_executable(glamdb_native_fault_driver server/native_fault_driver.cpp support/journal_fault_vfs.cpp)
    target_link_libraries(glamdb_native_fault_driver PRIVATE glamdb_server_runtime glamdb_build_options
        SQLite::SQLite3 iceberg::iceberg_sql_catalog_static)
    add_test(NAME glamdb.native_http_fault COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/server/native_fault_test.py" "$<TARGET_FILE:glamdb_native_fault_driver>")
    set_tests_properties(glamdb.native_http_fault PROPERTIES LABELS "integration;http;fault" TIMEOUT 180)
    add_executable(glamdb_routes_test server/admission_test.cpp server/routes_test.cpp server/metadata_pages_test.cpp server/scan_sessions_test.cpp server/databases_test.cpp server/database_limits_test.cpp support/fake_storage.cpp support/journal_fault_vfs.cpp)
    target_link_libraries(glamdb_routes_test PRIVATE glamdb_routes glamdb_build_options GTest::gtest_main
        SQLite::SQLite3 iceberg::iceberg_sql_catalog_static)
    gtest_discover_tests(glamdb_routes_test TEST_PREFIX glamdb.routes.
        PROPERTIES LABELS "unit;http;fault" TIMEOUT 120)
endif()
if(TARGET glamdb_routes)
    add_executable(glamdb_http_lifecycle_driver server/lifecycle_driver.cpp support/fake_storage.cpp)
    target_link_libraries(glamdb_http_lifecycle_driver PRIVATE glamdb_server_runtime glamdb_build_options
        SQLite::SQLite3 iceberg::iceberg_sql_catalog_static)
    add_test(NAME glamdb.http_lifecycle COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/server/lifecycle_test.py" "$<TARGET_FILE:glamdb_http_lifecycle_driver>")
    set_tests_properties(glamdb.http_lifecycle PROPERTIES LABELS "integration;http;fault" TIMEOUT 30)
endif()

if(TARGET glamdb_routes)
    add_executable(glamdb_resource_profile_driver server/resource_profile_driver.cpp)
    target_link_libraries(glamdb_resource_profile_driver PRIVATE glamdb_server_runtime glamdb_build_options)
    add_test(NAME glamdb.resource_profile COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/server/resource_profile_test.py" "$<TARGET_FILE:glamdb_resource_profile_driver>")
    set_tests_properties(glamdb.resource_profile PROPERTIES LABELS "integration;http;resources" TIMEOUT 240)
endif()
if(TARGET glamdb_logging)
add_executable(glamdb_logging_probe logging/logging_probe.cpp)
target_include_directories(glamdb_logging_probe PRIVATE "${PROJECT_SOURCE_DIR}")
target_link_libraries(glamdb_logging_probe PRIVATE glamdb_logging glamdb_build_options)
add_test(NAME glamdb.logging
    COMMAND "${CMAKE_COMMAND}" -E env "GLAMDB_LOG_PROBE=$<TARGET_FILE:glamdb_logging_probe>"
            "${Python3_EXECUTABLE}" -m unittest discover -s "${PROJECT_SOURCE_DIR}/tests/logging" -v)
set_tests_properties(glamdb.logging PROPERTIES LABELS "unit;logging" TIMEOUT 60)
endif()

if(TARGET glamdb_server)
    set(glamdb_logging_admin "-")
    if(TARGET glamdb_admin)
        set(glamdb_logging_admin "$<TARGET_FILE:glamdb_admin>")
    endif()
    add_test(NAME glamdb.server_logging COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/server/logging_test.py"
        "$<TARGET_FILE:glamdb_server>" "${glamdb_logging_admin}")
    set_tests_properties(glamdb.server_logging PROPERTIES LABELS "integration;http;logging" TIMEOUT 90)
endif()
