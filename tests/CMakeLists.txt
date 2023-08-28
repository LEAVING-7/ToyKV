enable_testing()

add_external(googletest; https://githubfast.com/google/googletest.git; v1.13.0)

add_executable(encoding_test encoding_test.cpp)
target_link_libraries(encoding_test gtest_main)

add_executable(segment_test segment_test.cpp)
target_link_libraries(segment_test gtest_main kv)

add_executable(wal_test wal_test.cpp)
target_link_libraries(wal_test gtest_main kv)

add_executable(snowflake_test snowflake_test.cpp)
target_link_libraries(snowflake_test gtest_main)

add_executable(db_test db_test.cpp)
target_link_libraries(db_test gtest_main kv)

add_executable(batch_test batch_test.cpp)
target_link_libraries(batch_test gtest_main kv)

include(GoogleTest)
gtest_discover_tests(encoding_test)
gtest_discover_tests(segment_test)
gtest_discover_tests(wal_test)
gtest_discover_tests(snowflake_test)
gtest_discover_tests(db_test)
gtest_discover_tests(batch_test)