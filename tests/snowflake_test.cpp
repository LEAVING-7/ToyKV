#include "../snowflake.hpp"
#include <gtest/gtest.h>

TEST(Snowflake, DuplicateID)
{
  auto node = snowflake::Node(1);

  snowflake::ID id1, id2;
  for (int i = 0; i < 1'000'000; i++) {
    id2 = node.gen();
    ASSERT_NE(id1.id, id2.id);
    ASSERT_EQ(id2.raw.node, 1);
    id1 = id2;
  }
}
