#pragma once
#include <algorithm>
#include <cstddef>
#include <cstring>
#include <format>
#include <memory>
#include <random>
#include <string>
#include <utility>

auto constexpr chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
static auto len = std::strlen(chars);
static auto rng = std::mt19937{std::random_device{}()};

inline auto genTestKey(int i) -> std::pair<std::shared_ptr<std::byte[]>, size_t>
{
  auto key = std::format("db-test-key-{:09}", i);
  auto keyPtr = std::make_shared<std::byte[]>(key.size());
  std::memcpy(keyPtr.get(), key.data(), key.size());
  return {keyPtr, key.size()};
}

inline auto randomValue(int n) -> std::pair<std::shared_ptr<std::byte[]>, size_t>
{
  auto dist = std::uniform_int_distribution<size_t>(0, len - 1);
  char lsen[] = "db-test-value-";
  auto cap = 15 + n;
  auto result = std::make_shared<std::byte[]>(cap);
  std::copy_n((std::byte*)lsen, 15, result.get());
  std::generate_n(result.get() + 15, n, [&]() { return (std::byte)chars[dist(rng)]; });

  return {result, cap};
}