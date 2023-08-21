#include <gtest/gtest.h>

#include "../wal.hpp"
#include <filesystem>
#include <string_view>

using namespace std::literals;
namespace fs = std::filesystem;

auto operator==(std::span<std::byte const> const lhs, std::span<std::byte const> const rhs) -> bool
{
  if (lhs.size() != rhs.size()) {
    return false;
  }
  if (lhs.data() == rhs.data()) {
    return true;
  }
  return std::memcmp(lhs.data(), rhs.data(), lhs.size()) == 0;
}

auto eq(std::span<std::byte const> const lhs, std::span<std::byte const> const rhs) -> bool { return lhs == rhs; }

auto destroyWAL(WAL& wal)
{
  wal.close();
  fs::remove_all(wal.option().dirPath);
}

TEST(WAL, Write)
{
  auto dir = fs::temp_directory_path() / "wal-test-write1";
  fs::create_directories(dir);

  auto ops = Option{
      .dirPath = dir.string(),
      .segmentSize = 3l * 1024 * 1024,
      .segmentFileExt = ".SEG",
      .blockCache = 3 * 1024 * 10,
  };
  auto wal = WAL(ops);

  auto data = std::vector{std::as_bytes(std::span("hello1"sv)), std::as_bytes(std::span("hello2"sv)),
                          std::as_bytes(std::span("hello3"sv))};

  auto pos1 = wal.write(data[0]);
  ASSERT_TRUE(pos1.has_value());
  auto pos2 = wal.write(data[1]);
  ASSERT_TRUE(pos2.has_value());
  auto pos3 = wal.write(data[2]);
  ASSERT_TRUE(pos3.has_value());

  auto var1 = wal.read(pos1.value());
  ASSERT_TRUE(var1.has_value());
  ASSERT_TRUE(eq(var1.value().span(), data[0]));

  auto var2 = wal.read(pos2.value());
  ASSERT_TRUE(var2.has_value());
  ASSERT_TRUE(eq(var2.value().span(), data[1]));

  auto var3 = wal.read(pos3.value());
  ASSERT_TRUE(var3.has_value());
  ASSERT_TRUE(eq(var3.value().span(), data[2]));

  destroyWAL(wal);
}

auto testWriteAndIterate(WAL& wal, std::int32_t size, std::int32_t n)
{
  auto valueSize = n * 3;
  auto const data = std::vector<std::byte>(valueSize, std::byte{0x23});
  auto positions = std::vector<ChunkPosition>{};

  for (auto i = 0; i < size; ++i) {
    auto pos = wal.write(std::as_bytes(std::span(data)));
    ASSERT_TRUE(pos.has_value());
    positions.push_back(pos.value());
  }

  auto reader = wal.reader();
  auto index = 0;
  for (;;) {
    auto pos = ChunkPosition();
    auto data = reader.next(pos);
    if (!data && data.error().code() == WALErrc::EndOfSegments) {
      break;
    }

    ASSERT_TRUE(data.has_value());
    ASSERT_TRUE(eq(data.value().span(), std::as_bytes(data->span())));

    ASSERT_EQ(pos.mSegmentID, positions[index].mSegmentID);
    ASSERT_EQ(pos.mChunkOffset, positions[index].mChunkOffset);
    ASSERT_EQ(pos.mBlockNumber, positions[index].mBlockNumber);

    index += 1;
  }
  ASSERT_EQ(index, size);
}

TEST(WAL, WriteLarge)
{
  auto dir = fs::temp_directory_path() / "wal-test-write2";
  fs::create_directories(dir);

  auto ops = Option{
      .dirPath = dir.string(),
      .segmentSize = 3l * 1024 * 1024,
      .segmentFileExt = ".SEG",
      .blockCache = 3 * 1024 * 10,
  };

  auto wal = WAL(ops);

  testWriteAndIterate(wal, 100000, 512);

  destroyWAL(wal);
}

TEST(WAL, WriteLarge2)
{
  auto dir = fs::temp_directory_path() / "wal-test-write3";
  fs::create_directories(dir);

  auto ops = Option{
      .dirPath = dir.string(),
      .segmentSize = 3l * 1024 * 1024,
      .segmentFileExt = ".SEG",
      .blockCache = 3 * 1024 * 10,
  };

  auto wal = WAL(ops);

  testWriteAndIterate(wal, 2000, 32 * 1024 * 3 + 10);

  destroyWAL(wal);
}

TEST(WAL, UseNewActiveSegment)
{
  auto dir = fs::temp_directory_path() / "wal-test-new-active-segment";
  fs::create_directories(dir);

  auto ops = Option{
      .dirPath = dir.string(),
      .segmentSize = 3l * 1024 * 1024,
      .segmentFileExt = ".SEG",
  };

  auto wal = WAL(ops);

  testWriteAndIterate(wal, 2000, 512);
  auto err = wal.useNewAciveSegment();
  ASSERT_TRUE(err.ok());

  auto data = std::vector(300, std::byte{0x23});
  for (int i = 0; i < 100; i++) {
    auto pos = wal.write(std::as_bytes(std::span(data)));
    ASSERT_TRUE(pos.has_value());
  }

  destroyWAL(wal);
}

TEST(WAL, IsEmpty)
{
  auto dir = fs::temp_directory_path() / "wal-test-is-empty";
  fs::create_directories(dir);

  auto ops = Option{
      .dirPath = dir.string(),
      .segmentSize = 3l * 1024 * 1024,
      .segmentFileExt = ".SEG",
  };

  auto wal = WAL(ops);

  ASSERT_TRUE(wal.empty());
  testWriteAndIterate(wal, 2000, 512);
  ASSERT_FALSE(wal.empty());

  destroyWAL(wal);
}

TEST(WAL, Reader)
{
  auto dir = fs::temp_directory_path() / "wal-test-wal-reader";
  fs::create_directories(dir);

  auto ops = Option{
      .dirPath = dir.string(),
      .segmentSize = 3l * 1024 * 1024,
      .segmentFileExt = ".SEG",
      .blockCache = 3 * 1024 * 10,
  };

  auto wal = WAL(ops);

  auto const size = 100000;

  auto const data = std::vector<std::byte>(512 * 3, std::byte{0x23});

  for (int i = 0; i < size; i++) {
    auto pos = wal.write(std::as_bytes(std::span(data)));
    ASSERT_TRUE(pos.has_value());
  }

  auto validate = [](WAL& wal, std::int32_t size) {
    auto i = 0;
    auto reader = wal.reader();
    for (;;) {
      auto pos = ChunkPosition();
      auto data = reader.next(pos);
      if (!data) {
        if (data.error().code() == WALErrc::EndOfSegments) {
          break;
        } else {
          throw std::runtime_error("unexpected error");
        }
      }
      ASSERT_EQ(pos.mSegmentID, reader.currentSegmentID());
      i += 1;
    }
    ASSERT_EQ(i, size);
  };

  validate(wal, size);

  auto err = wal.close();
  ASSERT_TRUE(err.ok());

  auto wal2 = WAL(ops);
  validate(wal2, size);

  destroyWAL(wal);
  destroyWAL(wal2);
}

TEST(WAL, Delete)
{
  auto dir = fs::temp_directory_path() / "wal-test-delete";
  fs::create_directories(dir);

  auto ops = Option{
      .dirPath = dir.string(),
      .segmentSize = 3l * 1024 * 1024,
      .segmentFileExt = ".SEG",
      .blockCache = 3 * 1024 * 10,
  };

  auto wal = WAL(ops);
  testWriteAndIterate(wal, 2000, 512);
  ASSERT_FALSE(wal.empty());

  auto err = wal.removeFiles();
  ASSERT_TRUE(err.ok());

  err = wal.setup(ops);
  assert(err.ok());
  ASSERT_TRUE(wal.empty());
}