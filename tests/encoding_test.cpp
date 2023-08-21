#include "../encoding.hpp"
#include "../preclude.hpp"
#include <gtest/gtest.h>
#include <array>
#include <cstddef>
#include <span>

#include <string_view>
using namespace std::literals;
TEST(Encoding, Put)
{
  auto arr = std::array{1, 2, 3};
  enc::put(arr, 2333);
  ASSERT_EQ(arr[0], 2333);
 
 
  auto vec = std::vector{1, 2, 3};
  enc::put(vec, 0x2333ull);
  ASSERT_EQ(vec[0], 0x2333);
  ASSERT_EQ(vec[1], 0x0);
  enc::put(&vec[1], 7777);
  enc::put(&vec[0], 9999);
  ASSERT_EQ(vec[0], 9999);
  ASSERT_EQ(vec[1], 7777);
  enc::put(vec.data(), 1234);
  ASSERT_EQ(vec[0], 1234);
  ASSERT_EQ(vec[1], 7777);

  auto str = std::string("hello");
  enc::put(str, 'A');
  auto span = std::span(str);
  ASSERT_EQ(str[0], 'A');
  enc::put(str, "worl");
  ASSERT_EQ(str.size(), 5);
  ASSERT_EQ(str, "worl\0"sv);
}

TEST(Encoding, Get)
{
  auto arr = std::array{9999999999};
  auto v = 0l;
  enc::get(arr, v);
  ASSERT_EQ(v, 9999999999);

  std::uint8_t buf[8] = {1, 1, 1, 1, 1, 1, 1, 1};
  auto v2 = 0ull;
  enc::get(buf, v2);
  ASSERT_EQ(v2, 0x0101010101010101);

  v2 = 0;
  enc::get((std::uint8_t*)(buf), v2);
  ASSERT_EQ(v2, 0x0101010101010101);

  auto str = std::string("hello");
  auto buf2 = std::array<std::uint8_t, 5>{};
  enc::get(str, buf2);
  ASSERT_EQ(buf2[0], 'h');
  ASSERT_EQ(buf2[1], 'e');
  ASSERT_EQ(buf2[2], 'l');
  ASSERT_EQ(buf2[3], 'l');
  ASSERT_EQ(buf2[4], 'o');

  std::uint32_t v3 = 0;
  enc::get(std::array<std::uint8_t, 4>{0x23, 0x11, 0x32, 0x88}, v3);
  ASSERT_EQ(v3, 0x88321123);
}

TEST(Encoding, PutGet)
{
  auto arr = std::array<std::uint8_t, 10>{0, 1, 2, 3, 4, 5, 6, 7, 8};
  auto const v = 0x1234567890ull;
  enc::put(arr, v);
  auto pull = *reinterpret_cast<std::uint64_t*>(arr.data());
  ASSERT_EQ(pull, 0x1234567890);
  auto temp = 0ull;
  enc::get(arr, temp);
  ASSERT_EQ(temp, v);
  ASSERT_EQ(arr[8], 0x8);
  enc::put<std::endian::big>(arr, v);
  pull = *reinterpret_cast<std::uint64_t*>(arr.data());
  ASSERT_EQ(pull, 0x9078563412000000);
  ASSERT_EQ(arr[8], 0x8);
}
