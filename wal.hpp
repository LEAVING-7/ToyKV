#pragma once

#include "segment.hpp"

#include <algorithm>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <vector>

constexpr std::size_t kInitSegmentFileID = 1;

class WALReader;

class Wal {
public:
  Wal(std::shared_ptr<Segment> activeSegment, std::map<SegmentID, std::shared_ptr<Segment>> olderSegments,
      WalOption const& option, std::shared_ptr<Cache<std::uint64_t, Bytes>> blockCache,
      std::uint32_t bytesWrite) noexcept
      : mActiveSegment(std::move(activeSegment)), mOlderSegments(std::move(olderSegments)), mOption(option),
        mBlockCache(std::move(blockCache)), mBytesWrite(bytesWrite)
  {
  }
  ~Wal() { close(); }
  static auto create(WalOption const& option) -> ext::expected<std::unique_ptr<Wal>, std::error_code>
  {
    if (!option.segmentFileExt.starts_with(".")) {
      return ext::make_unexpected(WalErr::InvalidOption);
    }
    if (option.blockCache > option.segmentSize) {
      return ext::make_unexpected(WalErr::InvalidOption);
    }
    namespace fs = std::filesystem;
    std::error_code ec;
    
    fs::create_directories(option.dirPath);

    auto blockCache = std::shared_ptr<Cache<std::uint64_t, Bytes>>();
    if (option.blockCache > 0) {
      auto lruSize = option.blockCache / kBlockSize;
      if (option.blockCache % kBlockSize != 0) {
        lruSize++;
      }
      blockCache = std::make_unique<Cache<std::uint64_t, Bytes>>(lruSize);
    }
    auto segmentIDs = std::vector<SegmentID>();

    auto entry_iter = fs::directory_iterator(option.dirPath);
    for (auto& entry : entry_iter) {
      if (entry.is_directory()) {
        continue;
      }
      auto id = SegmentID();
      if (entry.path().extension() != option.segmentFileExt) {
        continue;
      }
      if (auto r = std::sscanf(entry.path().filename().c_str(), "%u", &id); r != 1) {
        continue;
      }
      segmentIDs.push_back(id);
    }
    auto activeSegment = std::shared_ptr<Segment>();
    auto olderSegments = std::map<SegmentID, std::shared_ptr<Segment>>();
    if (segmentIDs.empty()) {
      activeSegment =
          std::make_shared<Segment>(option.dirPath.string(), option.segmentFileExt, kInitSegmentFileID, blockCache);
    } else {
      std::sort(segmentIDs.begin(), segmentIDs.end());
      for (auto i = 0; i < segmentIDs.size(); i++) {
        auto segment =
            std::make_shared<Segment>(option.dirPath.string(), option.segmentFileExt, segmentIDs[i], blockCache);
        if (i == segmentIDs.size() - 1) {
          activeSegment = segment;
        } else {
          olderSegments[segmentIDs[i]] = segment;
        }
      }
    }
    return std::make_unique<Wal>(activeSegment, olderSegments, option, std::move(blockCache), 0);
  }

  auto isFull(std::int64_t delta) const -> bool
  {
    return mActiveSegment->size() + delta + kChunkHeaderSize > mOption.segmentSize;
  }
  auto empty() const -> bool
  {
    auto lk = std::shared_lock(mMutex);
    return mOlderSegments.empty() && mActiveSegment->size() == 0;
  }
  auto option() const -> WalOption const& { return mOption; }
  auto activeSegmentID() const -> SegmentID
  {
    auto lk = std::shared_lock(mMutex);
    return mActiveSegment->id();
  }
  auto useNewAciveSegment() -> std::error_code
  {
    auto lk = std::scoped_lock(mMutex);

    if (auto err = mActiveSegment->sync(); err) {
      return err;
    }
    auto newSegment = std::make_shared<Segment>(mOption.dirPath.string(), mOption.segmentFileExt,
                                                mActiveSegment->id() + 1, mBlockCache);

    mOlderSegments[mActiveSegment->id()] = mActiveSegment;
    mActiveSegment = newSegment;
    return SegmentErr::Ok;
  }
  auto write(std::span<std::byte const> data) -> ext::expected<ChunkPosition, std::error_code>
  {
    auto lk = std::scoped_lock(mMutex);

    if (data.size() + kChunkHeaderSize > mOption.segmentSize) {
      return ext::make_unexpected(WalErr::TooLargeValue);
    }
    if (isFull(data.size())) {
      auto err = mActiveSegment->sync();
      if (err) {
        return ext::make_unexpected(err);
      }
      mBytesWrite = 0;
      auto segment = std::make_shared<Segment>(mOption.dirPath.string(), mOption.segmentFileExt,
                                               mActiveSegment->id() + 1, mBlockCache);
      mOlderSegments[mActiveSegment->id()] = mActiveSegment;
      mActiveSegment = segment;
      log_debug("create new segment %u\n", mActiveSegment->id());
    }

    auto pos = mActiveSegment->write(data);
    if (!pos.has_value()) {
      return ext::make_unexpected(pos.error());
    }
    mBytesWrite += pos->mChunkSize;

    auto needSync = mOption.syncWrite;
    if (!needSync && mOption.bytesPerSync > 0) {
      needSync = mBytesWrite >= mOption.bytesPerSync;
    }

    if (needSync) {
      auto err = mActiveSegment->sync();
      if (err) {
        return ext::make_unexpected(err);
      }
      mBytesWrite = 0;
    }
    return pos;
  }

  auto read(ChunkPosition const& pos) -> ext::expected<Bytes, std::error_code>
  {
    auto lk = std::shared_lock(mMutex);

    Segment* segment = nullptr;
    if (pos.mSegmentID == mActiveSegment->id()) {
      segment = mActiveSegment.get();
    } else {
      auto iter = mOlderSegments.find(pos.mSegmentID);
      if (iter == mOlderSegments.end()) {
        throw std::runtime_error("segment not found");
      }
      segment = iter->second.get();
    }
    return segment->read(pos.mBlockNumber, pos.mChunkOffset);
  }

  auto close() -> bool
  {
    auto lk = std::scoped_lock(mMutex);

    if (mBlockCache != nullptr) {
      mBlockCache->clear();
    }

    for (auto const& [id, segment] : mOlderSegments) {
      auto ok = segment->close();
      assert(ok);
    }
    mOlderSegments.clear();
    return mActiveSegment->close();
  }

  auto removeFiles() -> bool
  {
    auto lk = std::scoped_lock(mMutex);
    if (mBlockCache != nullptr) {
      mBlockCache->clear();
    }
    for (auto const& [id, segment] : mOlderSegments) {
      auto ok = segment->remove();
      assert(ok);
    }
    mOlderSegments.clear();
    return mActiveSegment->remove();
  }

  auto sync() -> std::error_code
  {
    auto lk = std::scoped_lock(mMutex);
    return mActiveSegment->sync();
  }

  auto readerWithMax(SegmentID segID) -> WALReader;
  auto readerWithStart(SegmentID segID) -> WALReader;
  auto reader() -> WALReader;

private:
  std::shared_ptr<Segment> mActiveSegment;
  std::map<SegmentID, std::shared_ptr<Segment>> mOlderSegments;

  WalOption mOption;
  mutable std::shared_mutex mMutex;
  std::shared_ptr<Cache<std::uint64_t, Bytes>> mBlockCache;
  std::uint32_t mBytesWrite;
};

class WALReader {
public:
  WALReader(std::vector<SegmentReader>&& readers, std::uint32_t currentReader)
      : mReaders(std::move(readers)), mCurrReader(currentReader)
  {
  }

  auto next(ChunkPosition& pos) -> ext::expected<Bytes, std::error_code>
  {
    if (mCurrReader >= mReaders.size()) {
      return ext::make_unexpected(WalErr::EndOfSegments);
    }
    pos = ChunkPosition{};
    auto data = mReaders[mCurrReader].next(pos);
    if (!data && data.error() == SegmentErr::EndOfSegment) {
      mCurrReader++;
      return next(pos);
    }
    return data;
  }

  auto skipCurrentSegment() -> void { mCurrReader++; }
  auto currentSegmentID() -> SegmentID { return mReaders[mCurrReader].id(); }
  auto currentChunkPosition() -> ChunkPosition
  {
    auto reader = mReaders[mCurrReader];
    return ChunkPosition{
        .mSegmentID = reader.id(),
        .mBlockNumber = reader.blockNumber(),
        .mChunkOffset = reader.chunkOffset(),
    };
  }
  auto readers() const -> std::vector<SegmentReader> const& { return mReaders; }
  auto currentReaderIdx() const -> std::uint32_t { return mCurrReader; }

private:
  std::vector<SegmentReader> mReaders;
  std::uint32_t mCurrReader;
};

inline auto Wal::readerWithMax(SegmentID segID) -> WALReader
{
  auto lk = std::shared_lock(mMutex);

  auto segmentReaders = std::vector<SegmentReader>();
  for (auto const& [id, segment] : mOlderSegments) {
    if (segID == 0 || segment->id() <= segID) {
      auto reader = segment->reader();
      segmentReaders.push_back(reader);
    }
  }

  if (segID == 0 || mActiveSegment->id() <= segID) {
    auto reader = mActiveSegment->reader();
    segmentReaders.push_back(reader);
  }

  std::sort(segmentReaders.begin(), segmentReaders.end(), [](auto const& a, auto const& b) { return a.id() < b.id(); });
  return WALReader{std::move(segmentReaders), 0};
}

inline auto Wal::reader() -> WALReader { return readerWithMax(0); }