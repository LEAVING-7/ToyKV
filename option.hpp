#pragma once

#include <filesystem>

constexpr std::size_t B = 1;
constexpr std::size_t KiB = 1024 * B;
constexpr std::size_t MiB = 1024 * KiB;
constexpr std::size_t GiB = 1024 * MiB;

inline auto tempDBDir() -> std::filesystem::path
{
  auto path = std::filesystem::temp_directory_path() / "db-temp";
  std::filesystem::create_directory(path);
  return path;
}
struct WalOption {
  std::filesystem::path dirPath = std::filesystem::temp_directory_path();
  std::int64_t segmentSize = 1 * GiB;
  std::string segmentFileExt = ".SEG";
  std::uint32_t blockCache = 32 * KiB * 10;
  bool syncWrite = false;
  std::uint32_t bytesPerSync = 0;
};

struct DbOption {
  std::filesystem::path dirPath = tempDBDir();
  std::int64_t segmentSize = 1 * GiB;
  std::uint32_t blockCache = 32 * KiB * 10;
  bool syncWrite = false;
  std::uint32_t bytesPerSync = 0;
  // watch queue
};

inline auto checkDbOption(DbOption const& option) -> void
{
  if (option.dirPath.empty()) {
    throw std::invalid_argument("dirPath is empty");
  }
  if (option.segmentSize < 0) {
    throw std::invalid_argument("segmentSize is negative");
  }
}
