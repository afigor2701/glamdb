cmake_minimum_required(VERSION 3.28)
project(GlamDBConsumer LANGUAGES CXX)
find_package(GlamDB 0.1 CONFIG REQUIRED)
# The consumer builds its own Arrow input batches.
find_package(Arrow 25 CONFIG REQUIRED)
add_executable(consumer main.cpp)
target_link_libraries(consumer PRIVATE glamdb::core Arrow::arrow_static)
