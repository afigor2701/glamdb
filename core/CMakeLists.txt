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

if(NOT GLAMDB_IO_ONLY)
    find_package(SQLite3 CONFIG REQUIRED GLOBAL)
    find_package(iceberg CONFIG REQUIRED GLOBAL)
    find_package(Arrow 25 CONFIG REQUIRED GLOBAL)
    add_library(glamdb_core STATIC api/canonical.cpp api/codec.cpp api/arrow_input.cpp
        catalog/sqlite.cpp catalog/durable_sqlite_catalog.cpp
        engine/storage_mapping.cpp engine/store_lease.cpp engine/engine.cpp
        scan/predicate.cpp)
    add_library(glamdb::core ALIAS glamdb_core)
    target_link_libraries(glamdb_core PRIVATE "$<BUILD_INTERFACE:glamdb_build_options>" SQLite::SQLite3
        iceberg::iceberg_sql_catalog_static Arrow::arrow_static)
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
            scan/scan_planner.cpp scan/scan_cursor.cpp)
        target_compile_definitions(glamdb_core PRIVATE GLAMDB_HAS_STORAGE=1)
        target_link_libraries(glamdb_core PRIVATE glamdb_storage)
        set_target_properties(glamdb_io PROPERTIES EXPORT_NAME io)
        install(TARGETS glamdb_io EXPORT GlamDBTargets ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}")
    endif()
    install(TARGETS glamdb_core EXPORT GlamDBTargets ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}")
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
