cmake_minimum_required(VERSION 3.20)
project(kv)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
add_compile_options(-Wall -fsanitize=address -fno-omit-frame-pointer)
add_link_options(-fsanitize=address -fno-omit-frame-pointer)
include(cmake/add_external.cmake)

add_executable(kv main.cpp)

add_subdirectory(tests)
