#pragma once
#include "cache.hpp"
#include "crc32.hpp"
#include "encoding.hpp"
#include "errors.hpp"
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

  auto operator==(ChunkPosition const& rhs) const -> bool = default;
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

constexpr std::size_t CHUNK_HEADER_SIZE = 7;
constexpr std::size_t BLOCK_SIZE = 32 * KiB;
constexpr int SEGMENT_FILE_PERM = 0644;

class Bytes {
public:
  Bytes() = default;
  Bytes(std::size_t cap) : mData(std::make_shared<std::byte[]>(cap)), mCap(cap) {}
  Bytes(Bytes&&) = default;
  Bytes& operator=(Bytes&&) = default;
  Bytes(Bytes const&) = default;
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
    auto fd = ::open(mFilePath.c_str(), O_RDWR | O_CREAT | O_APPEND, SEGMENT_FILE_PERM);
    if (fd == -1) {
      throw std::runtime_error(std::format("failed to open segment file: {}", ::strerror(errno)));
    }
    mFd = fd;
    auto offset = lseek(mFd, 0, SEEK_END);
    if (offset == -1) {
      throw std::runtime_error(std::format("failed to seek segment file: {}", ::strerror(errno)));
    }

    mCurrentBlockNumber = offset / BLOCK_SIZE;
    mCurrentBlockSize = offset % BLOCK_SIZE;
  }
  ~Segment()
  {
    if (!isClosed()) {
      ::close(mFd);
      mFd = -1;
    }
  }

  auto sync() -> SegmentErrc
  {
    if (isClosed()) {
      return SegmentErrc::SegmentClosed;
    }
    auto r = ::fsync(mFd);
    if (r == -1) {
      return SegmentErrc::lastSysErrc();
    }
    return SegmentErrc::Ok;
  }
  [[nodiscard]] auto id() const -> SegmentID { return mId; }
  [[nodiscard]] auto size() const -> std::size_t { return mCurrentBlockNumber * BLOCK_SIZE + mCurrentBlockSize; }
  auto remove() -> SegmentErrc
  {
    if (!isClosed()) {
      auto r = ::close(mFd);
      if (r == -1) {
        return SegmentErrc::lastSysErrc();
      }
      mFd = -1;
    }
    if (std::filesystem::exists(mFilePath)) {
      std::filesystem::remove(mFilePath);
    }
    return SegmentErrc::Ok;
  }
  [[nodiscard]] auto isClosed() const -> bool { return mFd == -1; }
  auto close() -> SegmentErrc
  {
    if (!isClosed()) {
      auto r = ::close(mFd);
      if (r == -1) {
        return SegmentErrc::lastSysErrc();
      }
      mFd = -1;
      return SegmentErrc::Ok;
    } else {
      return SegmentErrc::SegmentClosed;
    }
  }
  auto write(std::span<std::byte const> data) -> ext::expected<ChunkPosition, SegmentErrc>
  {
    if (isClosed()) {
      return ext::make_unexpected(SegmentErrc::SegmentClosed);
    }
    if (mCurrentBlockSize + CHUNK_HEADER_SIZE >= BLOCK_SIZE) {
      if (mCurrentBlockSize < BLOCK_SIZE) {
        auto padding = BLOCK_SIZE - mCurrentBlockSize;
        auto writeSize = ::ftruncate64(mFd, size() + padding);
        if (writeSize == -1) {
          return ext::make_unexpected(SegmentErrc::lastSysErrc());
        }
      }
      mCurrentBlockNumber++;
      mCurrentBlockSize = 0;
    }
    auto position = ChunkPosition{mId, mCurrentBlockNumber, mCurrentBlockSize, static_cast<std::uint32_t>(data.size())};
    auto dataSize = std::uint32_t(data.size());
    if (mCurrentBlockSize + dataSize + CHUNK_HEADER_SIZE <= BLOCK_SIZE) {
      writeImpl(data, ChunkType::Full);
      position.mChunkSize = dataSize + CHUNK_HEADER_SIZE;
      return {position};
    }

    std::int64_t leftSize = dataSize;
    auto blockCount = std::uint32_t(0);
    while (leftSize > 0) {
      auto chunkSize = BLOCK_SIZE - mCurrentBlockSize - CHUNK_HEADER_SIZE;
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
    position.mChunkSize = blockCount * CHUNK_HEADER_SIZE + dataSize;
    return {position};
  }

  auto read(std::uint32_t blockNumber, std::int64_t chunkOffset) -> ext::expected<Bytes, SegmentErrc>
  {
    auto position = ChunkPosition{mId, blockNumber, chunkOffset, 0};
    return readImpl(position);
  }

  auto reader() -> SegmentReader;

private:
  auto writeImpl(std::span<std::byte const> data, ChunkType type) -> SegmentErrc
  {
    std::uint32_t const dataSize = data.size();

    auto header = ChunkHeader{};
    header.mLength = dataSize;
    header.mType = type;
    header.mCrc = getChecksum(header, data);

    auto writeSize = ::write(mFd, &header, CHUNK_HEADER_SIZE);
    if (writeSize == -1) {
      return SegmentErrc::lastSysErrc();
    }

    writeSize = ::write(mFd, data.data(), data.size());
    if (writeSize == -1) {
      return SegmentErrc::lastSysErrc();
    }

    if (mCurrentBlockSize > BLOCK_SIZE) {
      throw std::runtime_error("block size overflow");
    }

    mCurrentBlockSize += CHUNK_HEADER_SIZE + dataSize;
    if (mCurrentBlockSize == BLOCK_SIZE) {
      mCurrentBlockNumber++;
      mCurrentBlockSize = 0;
    }
    return SegmentErrc::Ok;
  }
  // if success, set position point to the next chunk
  auto readImpl(ChunkPosition& position) -> ext::expected<Bytes, SegmentErrc>
  {
    if (isClosed()) {
      return ext::make_unexpected(SegmentErrc::SegmentClosed);
    }
    auto blockNumber = position.mBlockNumber;
    auto chunkOffset = position.mChunkOffset;

    auto segSize = size();
    auto nextChunk = ChunkPosition{mId};
    auto result = Buffer();
    for (;;) {
      std::int64_t size = BLOCK_SIZE;
      std::int64_t offset = blockNumber * BLOCK_SIZE;
      Bytes* cachedBlockPtr = nullptr;
      if (BLOCK_SIZE + offset > segSize) {
        size = segSize - offset;
      }
      if (chunkOffset >= size) {
        return ext::make_unexpected(SegmentErrc::EndOfSegment);
      }
      if (mCache) {
        cachedBlockPtr = mCache->get(cacheKey(blockNumber));
      }
      auto cacheBlock = Bytes();
      if (cachedBlockPtr != nullptr) {
        cacheBlock = cachedBlockPtr->clone();
      } else {
        if (auto r = ::lseek64(mFd, offset, SEEK_SET); r == -1) {
          return ext::make_unexpected(SegmentErrc::lastSysErrc());
        }
        cacheBlock = Bytes(size);
        auto readSize = ::read(mFd, cacheBlock.data(), cacheBlock.capacity());
        if (readSize == -1) {
          return ext::make_unexpected(SegmentErrc::lastSysErrc());
        }
        if (mCache != nullptr && size == BLOCK_SIZE && cachedBlockPtr == nullptr) {
          mCache->put(cacheKey(blockNumber), cacheBlock.clone());
        }
      }
      auto header = ChunkHeader();
      enc::get(cacheBlock.span().subspan(chunkOffset, CHUNK_HEADER_SIZE),
               std::span((std::byte*)&header, CHUNK_HEADER_SIZE));
      auto start = chunkOffset + CHUNK_HEADER_SIZE;
      auto length = header.mLength;
      result.extendCapacity(length);
      result.append(cacheBlock.span().subspan(start, length));
      auto checksumEnd = chunkOffset + CHUNK_HEADER_SIZE + length;
      auto checksum = getChecksum(header, cacheBlock.span().subspan(chunkOffset + CHUNK_HEADER_SIZE, length));
      auto savedChecksum = header.mCrc;
      if (checksum != savedChecksum) {
        return ext::make_unexpected(SegmentErrc::InvalidChecksum);
      }

      auto chunkType = header.mType;
      if (chunkType == ChunkType::Full || chunkType == ChunkType::Last) {
        nextChunk.mBlockNumber = blockNumber;
        nextChunk.mChunkOffset = checksumEnd;

        if (checksumEnd + CHUNK_HEADER_SIZE >= BLOCK_SIZE) {
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
  int mFd;
  std::string mFilePath;
  std::uint32_t mCurrentBlockNumber;
  std::uint32_t mCurrentBlockSize;
  std::shared_ptr<Cache<std::uint64_t, Bytes>> mCache;

  friend class SegmentReader;
};

class SegmentReader {
public:
  SegmentReader(Segment& segment, std::uint32_t blockNumber, std::int64_t mChunkOffset)
      : mSegment(&segment), mBlockNumber(blockNumber), mChunkOffset(mChunkOffset)
  {
  }
  SegmentReader(SegmentReader const&) = default;
  [[nodiscard]] auto id() const -> SegmentID { return mSegment->mId; }
  auto next(ChunkPosition& position) -> ext::expected<Bytes, SegmentErrc>
  {
    if (mSegment->isClosed()) {
      return ext::make_unexpected(SegmentErrc::SegmentClosed);
    }
    position = ChunkPosition{mSegment->mId, mBlockNumber, mChunkOffset};

    auto result = mSegment->readImpl(position);
    if (!result) {
      return ext::make_unexpected(result.error());
    }

    auto chunkSize =
        position.mBlockNumber * BLOCK_SIZE + position.mChunkOffset - (mBlockNumber * BLOCK_SIZE + mChunkOffset);
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

inline auto Segment::reader() -> SegmentReader { return SegmentReader(*this, 0, 0); }
