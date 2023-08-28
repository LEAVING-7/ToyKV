#include "cache.hpp"
#include "db.hpp"
#include "encoding.hpp"
#include "record.hpp"
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

auto constexpr kKeyFormat = "abc-{:09}";
auto constexpr kValueFormat = "value-{:09}";

auto getBytesString(Bytes const& bytes) -> std::string_view
{
  return std::string_view{reinterpret_cast<char const*>(bytes.data()), bytes.capacity()};
}
auto main() -> int
{
  auto wal = Wal::create(WalOption{
      .dirPath = fs::temp_directory_path() / "wal-temp",
      .segmentSize = 1024,
      .segmentFileExt = ".SEG",
      .blockCache = 512,
      .syncWrite = false,
      .bytesPerSync = 0,
  });
  if (!wal) {
    throw std::system_error(wal.error());
  }
  // auto buffer = std::array<std::byte, 512>();
  // auto pos = wal->get()->write(buffer);
  // if (!pos) {
  //   throw std::system_error(pos.error());
  // }
  // auto pos2 = wal->get()->write(buffer);
  // if (!pos2) {
  //   throw std::system_error(pos.error());
  // }
  // auto pos3 = wal->get()->write(buffer);
  // if (!pos3) {
  //   throw std::system_error(pos.error());
  // }
  auto reader = wal.value()->reader();
  while (true) {
    auto pos = ChunkPosition();
    auto res = reader.next(pos);
    if (!res) {
      throw std::system_error(res.error());
    }
    assert(res.value().span().size() == 512);
  }
  return 0;
}