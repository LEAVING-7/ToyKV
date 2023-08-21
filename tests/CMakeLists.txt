enable_testing()

add_external(googletest; https://githubfast.com/google/googletest.git; v1.13.0)

add_executable(encoding_test encoding_test.cpp)
target_link_libraries(encoding_test gtest_main)

add_executable(segment_test segment_test.cpp)
target_link_libraries(segment_test gtest_main)

add_executable(wal_test wal_test.cpp)
target_link_libraries(wal_test gtest_main)

include(GoogleTest)
gtest_discover_tests(encoding_test)
gtest_discover_tests(segment_test)
gtest_discover_tests(wal_test)