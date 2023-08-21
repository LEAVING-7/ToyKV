#include "cache.hpp"
#include "encoding.hpp"
#include "segment.hpp"
#include "wal.hpp"
#include <cassert>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <format>
#include <iostream>
#include <random>
#include <string_view>
#include <vector>
using namespace std::literals;

namespace fs = std::filesystem;

template <typename T>
auto print_span(std::span<T> arr) -> void
{
  for (auto i = 0; i < arr.size(); ++i) {
    std::cout << arr[i] << " ";
  }
  std::cout << "\n";
}

template <typename T>
auto parition(std::span<T> arr) -> int
{
  auto pivot = arr.back();
  auto i = 0;

  for (auto j = 0; j < arr.size(); ++j) {
    if (arr[j] < pivot) {
      std::swap(arr[i], arr[j]);
      ++i;
      // print_span(arr);
    }
  }
  std::swap(arr[i], arr.back());
  // std::cout << "final: ";
  // print_span(arr);
  return i;
}

template <typename T>
auto quick_sort(std::span<T> arr) -> void
{
  if (arr.size() <= 1) {
    return;
  }
  auto parition_index = parition(arr);
  quick_sort(arr.subspan(0, parition_index));
  quick_sort(arr.subspan(parition_index + 1));
}

template <typename T>
auto random_vector() -> std::vector<T>
{
  static std::random_device rd;
  static std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, 1000);

  auto n = dis(gen);
  std::vector<T> v(n);
  for (auto i = 0; i < n; ++i) {
    v[i] = i;
  }
  std::shuffle(v.begin(), v.end(), gen);
  return v;
}

template <typename T>
auto validate(std::vector<T> const& vec) -> bool
{
  for (auto i = 0; i < vec.size(); i++) {
    if (vec[i] != i) {
      return false;
    }
  }
  return true;
}
auto main() -> int
{
  for (int i = 0; i < 1000; i++) {
    auto vec = random_vector<int>();
    quick_sort(std::span(vec));
    assert(validate(vec));
  }
  auto vec = std::vector{5, 4, 1, 9, 7, 8, 5, 2};
  auto partition = parition(std::span(vec));
  std::cout << partition;
}

extern "C" {

}