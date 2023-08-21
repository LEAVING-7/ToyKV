#pragma once

#include <filesystem>

constexpr std::size_t B = 1;
constexpr std::size_t KiB = 1024 * B;
constexpr std::size_t MiB = 1024 * KiB;
constexpr std::size_t GiB = 1024 * MiB;

struct Option {
  std::filesystem::path dirPath = std::filesystem::temp_directory_path();
  std::int64_t segmentSize = 1 * GiB;
  std::string segmentFileExt = ".SEG";
  std::uint32_t blockCache = 32 * KiB * 10;
  bool sync = false;
  std::uint32_t bytesPerSync = 0;
};
