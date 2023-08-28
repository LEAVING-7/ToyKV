#pragma once
#include "cache.hpp"
#include "crc32.hpp"
#include "encoding.hpp"
#include "errors.hpp"
#include "file.hpp"
#include "option.hpp"
#include "preclude.hpp"

#include <fcntl.h>
#include <unistd.h>

using SegmentID = std::uint32_t;

enum class ChunkType : std::uint8_t { Full, First, Middle, Last };

struct ChunkPosition {
  SegmentID mSegmentID;
  std::uint32_t mBlockNumber;
  std::int64_t mChunkOffset;
  std::uint32_t mChunkSize;

  auto operator==(ChunkPosition const& rhs) const -> bool
  {
    return mSegmentID == rhs.mSegmentID && mBlockNumber == rhs.mBlockNumber && mChunkOffset == rhs.mChunkOffset;
  }
};

struct ChunkHeader {
  std::uint32_t mCrc;
  std::uint16_t mLength;
  ChunkType mType;
};

inline auto getChecksum(ChunkHeader const& header, std::span<std::byte const> data) -> std::uint32_t
{
  auto headerPtr = (std::byte const*)(&header) + 4;
  auto headerCrc = crc32(headerPtr, 3);
  return crc32(data, headerCrc);
}

constexpr std::size_t kChunkHeaderSize = 7;
constexpr std::size_t kBlockSize = 32 * KiB;
constexpr int kSegmentFilePerm = 0644;

class Bytes {
public:
  Bytes() = default;
  Bytes(std::size_t cap) : mData(std::make_shared<std::byte[]>(cap)), mCap(cap) {}
  Bytes(std::size_t cap, std::shared_ptr<std::byte[]>&& data) : mData(std::move(data)), mCap(cap) {}
  Bytes(Bytes&&) = default;
  Bytes(Bytes const&) = default;
  Bytes& operator=(Bytes&&) = default;
  Bytes& operator=(Bytes const&) = default;
  ~Bytes() = default;

  auto data() -> std::byte* { return mData.get(); }
  [[nodiscard]] auto data() const -> std::byte const* { return mData.get(); }
  [[nodiscard]] auto capacity() const -> std::size_t { return mCap; }
  explicit operator std::span<std::byte>() { return span(); }
  auto span() -> std::span<std::byte> { return {data(), capacity()}; }
  [[nodiscard]] auto span() const -> std::span<std::byte const> { return {data(), capacity()}; }
  [[nodiscard]] auto clone() const -> Bytes { return *this; }
  auto resize(std::size_t cap) -> void
  {
    if (cap > capacity()) {
      auto newData = std::make_shared<std::byte[]>(cap);
      std::copy(data(), data() + capacity(), newData.get());
      mData = newData;
      mCap = cap;
    }
  }
  static auto from(std::span<std::byte const> data) -> Bytes
  {
    auto ret = Bytes(data.size());
    std::copy(data.begin(), data.end(), ret.data());
    return ret;
  }
  static auto from(std::string_view str) -> Bytes { return from({(std::byte const*)str.data(), str.size()}); }

  auto operator==(Bytes const& rhs) const -> bool
  {
    if (capacity() != rhs.capacity()) {
      return false;
    }
    return std::memcmp(data(), rhs.data(), capacity()) == 0;
  }

private:
  std::shared_ptr<std::byte[]> mData = nullptr;
  std::size_t mCap = 0;
};

struct BytesHash {
  auto operator()(Bytes const& bytes) const -> std::size_t
  {
    return std::hash<std::string_view>()(std::string_view((char*)bytes.data(), bytes.capacity()));
  }
};

class Buffer : public Bytes {
public:
  Buffer() = default;
  Buffer(std::size_t cap) : Bytes(cap) {}
  ~Buffer() = default;

  auto clear() -> void { mSize = 0; }
  [[nodiscard]] auto clone() const -> Buffer { return *this; }
  [[nodiscard]] auto size() const -> std::size_t { return mSize; }
  auto extendCapacity(std::size_t size) -> void
  {
    if (mSize + size > capacity()) {
      resize(mSize + size);
    }
  }
  auto extentSize(std::size_t size) -> void
  {
    if (mSize + size > capacity()) {
      throw std::runtime_error("buffer overflow");
    }
    mSize += size;
  }
  auto append(std::span<std::byte const> data) -> void
  {
    if (size() + data.size() > capacity()) {
      resize(size() + data.size());
    }
    std::copy(data.begin(), data.end(), Bytes::data() + size());
    mSize += data.size();
  }

private:
  std::size_t mSize = 0;
};

inline auto segmentFileName(std::string_view dirPath, std::string_view extName, SegmentID id) -> std::string
{
  using std::filesystem::path;
  return path(dirPath) / std::format("{:09}{}", id, extName);
}

class SegmentReader;

class Segment {
public:
  Segment(std::string_view dirPath, std::string_view extName, SegmentID id,
          std::shared_ptr<Cache<std::uint64_t, Bytes>> cache)
      : mId(id), mCache(std::move(cache))
  {
    mFilePath = segmentFileName(dirPath, extName, id);

    auto file = File::open(mFilePath, "a+b");
    if (!file) {
      throw std::system_error(make_error_code(file.error()));
    }
    std::errc r = file->seek(0, File::End);
    if (r != std::errc(0)) {
      throw std::system_error(make_error_code(r));
    }
    auto offset = file->tell();
    if (!offset) {
      throw std::system_error(make_error_code(offset.error()));
    }
    mFile = std::move(file).value();
    log_debug("segment file %s size: %ld\n", mFilePath.c_str(), *offset);
    mCurrentBlockNumber = *offset / kBlockSize;
    mCurrentBlockSize = *offset % kBlockSize;
  }
  ~Segment()
  {
    if (!isClosed()) {
      mFile.close();
    }
  }

  auto sync() -> std::error_code
  {
    if (isClosed()) {
      return SegmentErr::SegmentClosed;
    }
    if (auto r = mFile.sync(); r != std::errc(0)) {
      return make_error_code(r);
    }
    return SegmentErr::Ok;
  }
  [[nodiscard]] auto id() const -> SegmentID { return mId; }
  [[nodiscard]] auto size() const -> std::size_t { return mCurrentBlockNumber * kBlockSize + mCurrentBlockSize; }
  auto remove() -> bool
  {
    if (!isClosed()) {
      if (!mFile.close()) {
        return false;
      }
    }
    if (std::filesystem::exists(mFilePath)) {
      std::filesystem::remove(mFilePath);
    }
    return true;
  }
  [[nodiscard]] auto isClosed() const -> bool { return mFile.isClosed(); }
  auto close() -> bool
  {
    if (!isClosed()) {
      return mFile.close();
    }
    return true;
  }
  auto write(std::span<std::byte const> data) -> ext::expected<ChunkPosition, std::error_code>
  {
    if (isClosed()) {
      return ext::make_unexpected(SegmentErr::SegmentClosed);
    }
    if (mCurrentBlockSize + kChunkHeaderSize >= kBlockSize) {
      if (mCurrentBlockSize < kBlockSize) {
        auto padding = kBlockSize - mCurrentBlockSize;
        if (auto r = mFile.truncate(size() + padding); r != std::errc(0)) {
          return ext::make_unexpected(make_error_code(r));
        }
      }
      mCurrentBlockNumber++;
      mCurrentBlockSize = 0;
    }

    auto position = ChunkPosition{mId, mCurrentBlockNumber, mCurrentBlockSize, static_cast<std::uint32_t>(data.size())};
    auto dataSize = std::uint32_t(data.size());
    if (mCurrentBlockSize + dataSize + kChunkHeaderSize <= kBlockSize) {
      writeImpl(data, ChunkType::Full);
      position.mChunkSize = dataSize + kChunkHeaderSize;
      return {position};
    }

    std::int64_t leftSize = dataSize;
    auto blockCount = std::uint32_t(0);
    while (leftSize > 0) {
      auto chunkSize = kBlockSize - mCurrentBlockSize - kChunkHeaderSize;
      if (chunkSize > leftSize) {
        chunkSize = leftSize;
      }
      auto chunk = Bytes(chunkSize);
      auto end = dataSize - leftSize + chunkSize;
      if (end > dataSize) {
        end = dataSize;
      }
      enc::put(chunk.span(), data.subspan(dataSize - leftSize, chunkSize));
      if (leftSize == dataSize) {
        writeImpl(chunk.span(), ChunkType::First);
      } else if (leftSize == chunkSize) {
        writeImpl(chunk.span(), ChunkType::Last);
      } else {
        writeImpl(chunk.span(), ChunkType::Middle);
      }
      leftSize -= chunkSize;
      blockCount++;
    }
    position.mChunkSize = blockCount * kChunkHeaderSize + dataSize;
    return {position};
  }

  auto read(std::uint32_t blockNumber, std::int64_t chunkOffset) -> ext::expected<Bytes, std::error_code>
  {
    auto position = ChunkPosition{mId, blockNumber, chunkOffset, 0};
    return readImpl(position);
  }
  auto reader() -> SegmentReader;

private:
  auto writeImpl(std::span<std::byte const> data, ChunkType type) -> std::error_code
  {
    std::uint32_t const dataSize = data.size();

    auto header = ChunkHeader{};
    header.mLength = dataSize;
    header.mType = type;
    header.mCrc = getChecksum(header, data);

    if (auto r = mFile.write(&header, kChunkHeaderSize); !r) {
      assert(r);
    }

    if (auto r = mFile.write(data); !r) {
      assert(r);
    }

    if (mCurrentBlockSize > kBlockSize) {
      throw std::runtime_error("block size overflow");
    }

    mCurrentBlockSize += kChunkHeaderSize + dataSize;
    if (mCurrentBlockSize == kBlockSize) {
      mCurrentBlockNumber++;
      mCurrentBlockSize = 0;
    }
    return SegmentErr::Ok;
  }

  // if success, set position point to the next chunk
  auto readImpl(ChunkPosition& position) -> ext::expected<Bytes, std::error_code>
  {
    if (isClosed()) {
      return ext::make_unexpected(SegmentErr::SegmentClosed);
    }
    auto blockNumber = position.mBlockNumber;
    auto chunkOffset = position.mChunkOffset;

    auto segSize = size();
    auto nextChunk = ChunkPosition{mId};
    auto result = Buffer();
    for (;;) {
      std::int64_t size = kBlockSize;
      std::int64_t offset = blockNumber * kBlockSize;
      Bytes* cachedBlockPtr = nullptr;
      if (kBlockSize + offset > segSize) {
        size = segSize - offset;
      }
      if (chunkOffset >= size) {
        return ext::make_unexpected(SegmentErr::EndOfSegment);
      }
      if (mCache) {
        cachedBlockPtr = mCache->get(cacheKey(blockNumber));
      }
      auto cacheBlock = Bytes();
      if (cachedBlockPtr != nullptr) {
        cacheBlock = cachedBlockPtr->clone();
      } else {
        if (auto r = mFile.seek(offset, File::Set); r != std::errc(0)) {
          return ext::make_unexpected(make_error_code(r));
        }
        cacheBlock = Bytes(size);

        auto r = mFile.read(cacheBlock.span());
        assert(r);
        assert(mFile.eof() == false);

        if (mCache != nullptr && size == kBlockSize && cachedBlockPtr == nullptr) {
          mCache->put(cacheKey(blockNumber), cacheBlock.clone());
        }
      }
      auto header = ChunkHeader();
      enc::get(cacheBlock.span().subspan(chunkOffset, kChunkHeaderSize),
               std::span((std::byte*)&header, kChunkHeaderSize));
      auto start = chunkOffset + kChunkHeaderSize;
      auto length = header.mLength;
      result.extendCapacity(length);
      result.append(cacheBlock.span().subspan(start, length));
      auto checksumEnd = chunkOffset + kChunkHeaderSize + length;
      auto checksum = getChecksum(header, cacheBlock.span().subspan(chunkOffset + kChunkHeaderSize, length));
      auto savedChecksum = header.mCrc;
      if (checksum != savedChecksum) {
        return ext::make_unexpected(SegmentErr::InvalidCheckSum);
      }

      auto chunkType = header.mType;
      if (chunkType == ChunkType::Full || chunkType == ChunkType::Last) {
        nextChunk.mBlockNumber = blockNumber;
        nextChunk.mChunkOffset = checksumEnd;

        if (checksumEnd + kChunkHeaderSize >= kBlockSize) {
          nextChunk.mBlockNumber++;
          nextChunk.mChunkOffset = 0;
        }
        break;
      }
      blockNumber++;
      chunkOffset = 0;
    }
    position = nextChunk;
    return {result};
  }

  auto cacheKey(std::uint32_t blockNumber) -> std::uint64_t
  {
    return std::uint64_t(mId) << 32 | std::uint64_t(blockNumber);
  }

  SegmentID mId;
  File mFile;
  std::string mFilePath;
  std::uint32_t mCurrentBlockNumber;
  std::uint32_t mCurrentBlockSize;
  std::shared_ptr<Cache<std::uint64_t, Bytes>> mCache;

  friend class SegmentReader;
};

class SegmentReader {
public:
  SegmentReader(Segment* segment, std::uint32_t blockNumber, std::int64_t mChunkOffset)
      : mSegment(segment), mBlockNumber(blockNumber), mChunkOffset(mChunkOffset)
  {
  }
  SegmentReader(SegmentReader const&) = default;
  auto id() const -> SegmentID { return mSegment->mId; }
  auto next(ChunkPosition& position) -> ext::expected<Bytes, std::error_code>
  {
    if (mSegment->isClosed()) {
      return ext::make_unexpected(SegmentErr::SegmentClosed);
    }
    position = ChunkPosition{mSegment->mId, mBlockNumber, mChunkOffset};

    auto result = mSegment->readImpl(position);
    if (!result) {
      return ext::make_unexpected(result.error());
    }

    auto chunkSize =
        position.mBlockNumber * kBlockSize + position.mChunkOffset - (mBlockNumber * kBlockSize + mChunkOffset);
    std::swap(mBlockNumber, position.mBlockNumber);
    std::swap(mChunkOffset, position.mChunkOffset);
    position.mChunkSize = chunkSize; // setup chunk size
    return {std::move(result).value()};
  }
  [[nodiscard]] auto blockNumber() const -> std::uint32_t { return mBlockNumber; }
  [[nodiscard]] auto chunkOffset() const -> std::int64_t { return mChunkOffset; }

private:
  Segment* mSegment;
  std::uint32_t mBlockNumber;
  std::int64_t mChunkOffset;
};

inline auto Segment::reader() -> SegmentReader { return SegmentReader(this, 0, 0); }
