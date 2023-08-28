cmake_minimum_required(VERSION 3.20)
project(kv)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_C_STANDARD 99)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
add_compile_options(-Wall -fsanitize=address -fno-omit-frame-pointer)
add_link_options(-fsanitize=address -fno-omit-frame-pointer)
include(cmake/add_external.cmake)

add_library(kv db.cpp batch.cpp errors.cpp external/log.cpp)

add_executable(kv_test main.cpp)
target_link_libraries(kv_test kv)

add_subdirectory(tests)
