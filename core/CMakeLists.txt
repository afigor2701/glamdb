if(NOT GLAMDB_IO_ONLY OR GLAMDB_BUILD_LOGGING)
    add_subdirectory(telemetry)
endif()

add_library(glamdb_io STATIC
    io/local_fs.cpp
    io/sha256.cpp
    io/store_uri.cpp
    io/store_dirs.cpp
    io/io_attempt.cpp
    io/durable_file.cpp
    io/durable_copy.cpp
    io/write_latch.cpp
)
target_link_libraries(glamdb_io PRIVATE "$<BUILD_INTERFACE:glamdb_build_options>")
target_include_directories(glamdb_io PRIVATE "${PROJECT_SOURCE_DIR}")

find_package(Threads REQUIRED)
if(NOT GLAMDB_IO_ONLY OR GLAMDB_BUILD_METRICS)
    find_package(prometheus-cpp 1.3.0 EXACT CONFIG REQUIRED GLOBAL)
    add_library(glamdb_metrics STATIC telemetry/metrics.cpp)
    target_link_libraries(glamdb_metrics PRIVATE "$<BUILD_INTERFACE:glamdb_build_options>"
        Threads::Threads prometheus-cpp::core)
    target_include_directories(glamdb_metrics PRIVATE "${PROJECT_SOURCE_DIR}" "${PROJECT_SOURCE_DIR}/public")
    if(TARGET glamdb_logging)
        target_link_libraries(glamdb_metrics PRIVATE glamdb_logging)
        target_compile_definitions(glamdb_metrics PRIVATE GLAMDB_METRICS_WITH_LOGGING=1)
    endif()
endif()

if(NOT GLAMDB_IO_ONLY)
    find_package(SQLite3 CONFIG REQUIRED GLOBAL)
    find_package(iceberg CONFIG REQUIRED GLOBAL)
    find_package(Arrow 25 CONFIG REQUIRED GLOBAL)
    add_library(glamdb_core STATIC resources/resources.cpp resources/arrow_pool.cpp resources/metadata_result.cpp api/canonical.cpp api/codec.cpp api/arrow_input.cpp
        api/write_settings.cpp
        catalog/sqlite.cpp catalog/durable_sqlite_catalog.cpp
        engine/storage_mapping.cpp engine/store_lease.cpp engine/engine.cpp engine/metadata_pages.cpp
        scan/predicate.cpp)
    add_library(glamdb::core ALIAS glamdb_core)
    target_link_libraries(glamdb_core PRIVATE "$<BUILD_INTERFACE:glamdb_build_options>" SQLite::SQLite3
        iceberg::iceberg_sql_catalog_static Arrow::arrow_static glamdb_logging glamdb_metrics)
    target_include_directories(glamdb_core
        PUBLIC "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/public>"
               "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
        PRIVATE "${PROJECT_SOURCE_DIR}")
    target_compile_features(glamdb_core PUBLIC cxx_std_23)
    set_target_properties(glamdb_core PROPERTIES EXPORT_NAME core)
    if(GLAMDB_BUILD_STORAGE)
        # The production storage backend and scanner. Without them the public
        # factories install no backend and report DEPENDENCY_UNAVAILABLE.
        target_sources(glamdb_core PRIVATE backend/inert_catalog.cpp backend/local_storage_backend.cpp
            backend/storage_errors.cpp
            scan/parquet_input.cpp scan/pruning.cpp scan/metadata_cache.cpp scan/scan_planner.cpp scan/scan_cursor.cpp
            scan/metadata.cpp scan/file_metadata.cpp)
        target_compile_definitions(glamdb_core PRIVATE GLAMDB_HAS_STORAGE=1)
        target_link_libraries(glamdb_core PRIVATE glamdb_storage)
        set_target_properties(glamdb_io PROPERTIES EXPORT_NAME io)
        install(TARGETS glamdb_io EXPORT GlamDBTargets ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}")
    endif()
    install(TARGETS glamdb_core glamdb_metrics EXPORT GlamDBTargets ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}")
    if(TARGET glamdb_logging)
        set_target_properties(glamdb_logging PROPERTIES EXPORT_NAME logging)
        install(TARGETS glamdb_logging EXPORT GlamDBTargets ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}")
    endif()
    install(DIRECTORY "${PROJECT_SOURCE_DIR}/public/storage" DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
        FILES_MATCHING PATTERN "*.h")
    install(EXPORT GlamDBTargets NAMESPACE glamdb:: DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/GlamDB")
    include(CMakePackageConfigHelpers)
    configure_package_config_file("${PROJECT_SOURCE_DIR}/cmake/GlamDBConfig.cmake.in"
        "${PROJECT_BINARY_DIR}/GlamDBConfig.cmake"
        INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/GlamDB")
    write_basic_package_version_file("${PROJECT_BINARY_DIR}/GlamDBConfigVersion.cmake"
        VERSION "${PROJECT_VERSION}" COMPATIBILITY SameMajorVersion)
    install(FILES "${PROJECT_BINARY_DIR}/GlamDBConfig.cmake" "${PROJECT_BINARY_DIR}/GlamDBConfigVersion.cmake"
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/GlamDB")
endif()

if(GLAMDB_BUILD_STORAGE)
    add_subdirectory(storage)
endif()
