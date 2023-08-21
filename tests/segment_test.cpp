#include <gtest/gtest.h>
#include <random>

#include "../segment.hpp"

namespace fs = std::filesystem;
inline auto operator==(std::span<std::byte const> const lhs, std::span<std::byte const> const rhs) -> bool
{
  if (lhs.size() != rhs.size()) {
    return false;
  }
  if (lhs.data() == rhs.data()) {
    return true;
  }
  return std::memcmp(lhs.data(), rhs.data(), lhs.size()) == 0;
}

TEST(Segment, WriteFull)
{
  auto dir = fs::temp_directory_path() / "seg-test-full1";
  fs::create_directories(dir);
  auto seg = Segment(dir.string(), ".SIG", 1, nullptr);

  auto data = std::vector<std::byte>(100);
  // generate random data
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, 255);

  for (int i = 0; i < data.size(); i++) {
    data[i] = std::byte(dis(gen));
  }

  auto data1 = data;
  auto pos1 = seg.write(data1);
  ASSERT_TRUE(pos1.has_value());

  for (int i = 0; i < data.size(); i++) {
    data[i] = std::byte(dis(gen));
  }
  auto data2 = data;
  auto pos2 = seg.write(data2);
  ASSERT_TRUE(pos2.has_value());

  auto v1 = seg.read(pos1->mBlockNumber, pos1->mChunkOffset);
  ASSERT_TRUE(v1.has_value());
  ASSERT_TRUE(v1->span() == data1);

  auto v2 = seg.read(pos2->mBlockNumber, pos2->mChunkOffset);
  ASSERT_TRUE(v2.has_value());
  ASSERT_TRUE(v2->span() == data2);

  std::fill(data.begin(), data.end(), std::byte(0x23));

  for (int i = 0; i < 1000'000; i++) {
    auto pos = seg.write(data);
    ASSERT_TRUE(pos.has_value());
    auto v = seg.read(pos->mBlockNumber, pos->mChunkOffset);
    ASSERT_TRUE(v.has_value());
    ASSERT_TRUE(v->span() == data);
  }
  seg.remove();
}

TEST(Segment, WriteFull2)
{
  auto dir = fs::temp_directory_path() / "seg-test-full2";
  fs::create_directories(dir);
  auto seg = Segment(dir.string(), ".SIG", 1, nullptr);

  auto const data = std::vector<std::byte>(BLOCK_SIZE - CHUNK_HEADER_SIZE, std::byte(0x23));
  auto pos1 = seg.write(data);
  ASSERT_EQ(pos1->mBlockNumber, 0);
  ASSERT_EQ(pos1->mChunkOffset, 0);
  auto val1 = seg.read(pos1->mBlockNumber, pos1->mChunkOffset);
  ASSERT_TRUE(val1.has_value());
  ASSERT_TRUE(val1->span() == data);

  auto pos2 = seg.write(data);
  ASSERT_EQ(pos2->mBlockNumber, 1);
  ASSERT_EQ(pos2->mChunkOffset, 0);
  auto val2 = seg.read(pos2->mBlockNumber, pos2->mChunkOffset);
  ASSERT_TRUE(val2->span() == data);
  seg.remove();
}

TEST(Segment, WritePadding)
{
  auto dir = fs::temp_directory_path() / "seg-test-padding";
  fs::create_directories(dir);
  auto seg = Segment(dir.string(), ".SIG", 1, nullptr);

  auto const data = std::vector<std::byte>(BLOCK_SIZE - CHUNK_HEADER_SIZE - 3, std::byte(0x23));

  auto pos1 = seg.write(data);
  ASSERT_TRUE(pos1);

  auto pos2 = seg.write(data);
  ASSERT_EQ(pos2->mBlockNumber, 1);
  ASSERT_EQ(pos2->mChunkOffset, 0);

  auto var2 = seg.read(pos1->mBlockNumber, pos1->mChunkOffset);
  ASSERT_TRUE(var2->span() == data);
  seg.remove();
}

TEST(Segment, WriteNotFull)
{
  auto dir = fs::temp_directory_path() / "seg-test-not-full";
  fs::create_directories(dir);
  auto seg = Segment(dir.string(), ".SIG", 1, nullptr);

  auto const data = std::vector<std::byte>(BLOCK_SIZE + 100, std::byte(0x23));
  auto pos1 = seg.write(data);
  ASSERT_TRUE(pos1.has_value());
  auto var1 = seg.read(pos1->mBlockNumber, pos1->mChunkOffset);
  ASSERT_TRUE(var1.has_value());
  ASSERT_TRUE(var1->span() == data);

  auto pos2 = seg.write(data);
  ASSERT_TRUE(pos2.has_value());
  auto var2 = seg.read(pos2->mBlockNumber, pos2->mChunkOffset);
  ASSERT_TRUE(var2.has_value());
  ASSERT_TRUE(var2->span() == data);

  auto pos3 = seg.write(data);
  ASSERT_TRUE(pos3.has_value());
  auto var3 = seg.read(pos3->mBlockNumber, pos3->mChunkOffset);
  ASSERT_TRUE(var3.has_value());
  ASSERT_TRUE(var3->span() == data);

  auto const data2 = std::vector<std::byte>(BLOCK_SIZE * 3 + 100, std::byte(0x23));
  auto pos4 = seg.write(data2);
  ASSERT_TRUE(pos4.has_value());
  auto var4 = seg.read(pos4->mBlockNumber, pos4->mChunkOffset);
  ASSERT_TRUE(var4.has_value());
  ASSERT_TRUE(var4->span() == data2);
  seg.remove();
}

TEST(Segment, ReaderFull)
{
  auto dir = fs::temp_directory_path() / "seg-test-reader-full";
  fs::create_directories(dir);
  auto seg = Segment(dir.string(), ".SIG", 1, nullptr);

  auto const data = std::vector<std::byte>(BLOCK_SIZE + 100, std::byte(0x23));
  auto pos1 = seg.write(data);
  ASSERT_TRUE(pos1.has_value());
  auto pos2 = seg.write(data);
  ASSERT_TRUE(pos2.has_value());

  auto reader = seg.reader();
  auto rpos = ChunkPosition();
  auto var1 = reader.next(rpos);
  ASSERT_TRUE(var1.has_value());
  ASSERT_TRUE(var1->span() == data);
  ASSERT_EQ(rpos, *pos1);

  auto var2 = reader.next(rpos);
  ASSERT_TRUE(var2.has_value());
  ASSERT_TRUE(var2->span() == data);
  ASSERT_EQ(rpos, *pos2);

  auto var3 = reader.next(rpos);
  ASSERT_FALSE(var3.has_value());
  ASSERT_TRUE(var3.error() == SegmentErrc::EndOfSegment);

  seg.remove();
}

TEST(Segment, ManyChunksFull)
{
  auto dir = fs::temp_directory_path() / "seg-test-reader-ManyChunks_FULL";
  fs::create_directories(dir);
  auto cache = std::make_shared<Cache<std::uint64_t, Bytes>>(5, 2);
  auto seg = Segment(dir.string(), ".SIG", 1, cache);

  auto const data = std::vector<std::byte>(128, std::byte(0x23));
  auto positions = std::vector<ChunkPosition>();

  for (auto i = 1; i <= 1000000; i++) {
    auto pos = seg.write(data);
    ASSERT_TRUE(pos.has_value());
    positions.push_back(*pos);
  }

  auto reader = seg.reader();
  auto i = 0ull;
  for (;;) {
    auto rpos = ChunkPosition();
    auto var = reader.next(rpos);
    if (!var.has_value()) {
      ASSERT_TRUE(var.error() == SegmentErrc::EndOfSegment);
      break;
    }
    ASSERT_TRUE(var->span() == data);
    
    ASSERT_EQ(rpos.mBlockNumber, positions[i].mBlockNumber);
    ASSERT_EQ(rpos.mChunkOffset, positions[i].mChunkOffset);
    ASSERT_EQ(rpos.mSegmentID, positions[i].mSegmentID);
    i++;
  }
  seg.remove();
}