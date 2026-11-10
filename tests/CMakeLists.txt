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
        "${Python3_EXECUTABLE}" -m unittest discover
        -s "${CMAKE_CURRENT_SOURCE_DIR}/tooling" -p "test_*.py" -v
)
set_tests_properties(glamdb.tooling PROPERTIES LABELS "tooling" TIMEOUT 240)

if(NOT GLAMDB_IO_ONLY)
    add_test(NAME glamdb.smoke COMMAND glamdb)
    set_tests_properties(glamdb.smoke PROPERTIES LABELS "smoke" TIMEOUT 10)

    file(GLOB_RECURSE unit_test_sources CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/unit/*.cpp")
    list(FILTER unit_test_sources EXCLUDE REGEX "/unit/(io|storage)/|/unit/core_test\\.cpp$")
    add_executable(glamdb_unit ${unit_test_sources} support/fake_storage.cpp)
    target_link_libraries(glamdb_unit PRIVATE glamdb::core GTest::gtest_main glamdb_build_options
        SQLite::SQLite3 iceberg::iceberg_sql_catalog_static Arrow::arrow_static glamdb_json)
    target_include_directories(glamdb_unit PRIVATE "${PROJECT_SOURCE_DIR}")
    target_compile_definitions(glamdb_unit PRIVATE "GLAMDB_TEST_STORAGE=$<BOOL:${GLAMDB_BUILD_STORAGE}>")
    gtest_discover_tests(glamdb_unit TEST_PREFIX glamdb.unit. PROPERTIES LABELS unit TIMEOUT 30)

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
        add_executable(glamdb_vertical ${vertical_test_sources} support/fault_fs.cpp)
        target_include_directories(glamdb_vertical PRIVATE "${PROJECT_SOURCE_DIR}")
        target_link_libraries(glamdb_vertical PRIVATE glamdb::core glamdb_storage glamdb_build_options
            GTest::gtest_main)
        gtest_discover_tests(glamdb_vertical TEST_PREFIX glamdb.vertical.
            PROPERTIES LABELS "vertical;scan" TIMEOUT 60)
    endif()

    file(GLOB_RECURSE fault_test_sources CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/fault/*.cpp")
    add_executable(glamdb_fault ${fault_test_sources} support/fake_storage.cpp support/journal_fault_vfs.cpp)
    target_include_directories(glamdb_fault PRIVATE "${PROJECT_SOURCE_DIR}")
    target_link_libraries(glamdb_fault PRIVATE glamdb::core glamdb_build_options GTest::gtest_main
        SQLite::SQLite3 iceberg::iceberg_sql_catalog_static Arrow::arrow_static)
    gtest_discover_tests(glamdb_fault TEST_PREFIX glamdb.fault. PROPERTIES LABELS fault TIMEOUT 30)

    add_test(NAME glamdb.cli COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/integration/cli_test.py" "$<TARGET_FILE:glamdb>")
    set_tests_properties(glamdb.cli PROPERTIES LABELS integration TIMEOUT 30)
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
            -P "${CMAKE_CURRENT_SOURCE_DIR}/installed_consumer/run.cmake")
        set_tests_properties(glamdb.installed_consumer PROPERTIES LABELS installed TIMEOUT 120)
    endif()
endif()
