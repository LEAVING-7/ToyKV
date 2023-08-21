#pragma once

#include "segment.hpp"

#include <algorithm>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <vector>

constexpr std::size_t INIT_SEGMENT_FILE_ID = 1;

class WALReader;

class WAL {
public:
  WAL(Option const& option) { setup(option); }

  auto setup(Option const& option) -> WALErrc
  {
    mOption = option;
    mBytesWrite = 0;
    if (!option.segmentFileExt.starts_with(".")) {
      throw std::runtime_error("segment file extension must start with '.'");
    }
    if (option.blockCache > option.segmentSize) {
      throw std::runtime_error("block cache size must be less than segment size");
    }
    namespace fs = std::filesystem;

    if (fs::create_directories(option.dirPath)) {
      throw std::runtime_error("failed to create directory");
    }

    if (option.blockCache > 0) {
      auto lruSize = option.blockCache / BLOCK_SIZE;
      if (option.blockCache % BLOCK_SIZE != 0) {
        lruSize++;
      }
      mBlockCache = std::make_unique<Cache<std::uint64_t, Bytes>>(lruSize);
    }

    auto segmentIDs = std::vector<SegmentID>();
    auto entry_iter = fs::directory_iterator(option.dirPath);
    for (auto& entry : entry_iter) {
      if (entry.is_directory()) {
        continue;
      }
      auto id = SegmentID();
      auto scanFormat = std::format("%u{}", option.segmentFileExt);
      ::sscanf(entry.path().filename().c_str(), scanFormat.c_str(), &id);
      segmentIDs.push_back(id);
    }

    if (segmentIDs.empty()) {
      auto segment =
          std::make_shared<Segment>(option.dirPath.string(), option.segmentFileExt, INIT_SEGMENT_FILE_ID, mBlockCache);
      mActiveSegment = segment;
    } else {
      std::sort(segmentIDs.begin(), segmentIDs.end());

      for (auto i = 0; i < segmentIDs.size(); i++) {
        auto segment =
            std::make_shared<Segment>(option.dirPath.string(), option.segmentFileExt, segmentIDs[i], mBlockCache);
        if (i == segmentIDs.size() - 1) {
          mActiveSegment = segment;
        } else {
          mOlderSegments[segmentIDs[i]] = segment;
        }
      }
    }
    return WALErrc::Ok;
  }

  auto isFull(std::int64_t delta) const -> bool
  {
    return mActiveSegment->size() + delta + CHUNK_HEADER_SIZE > mOption.segmentSize;
  }
  auto empty() const -> bool
  {
    auto lk = std::shared_lock(mMutex);
    return mOlderSegments.empty() && mActiveSegment->size() == 0;
  }
  auto option() const -> Option const& { return mOption; }
  auto activeSegmentID() const -> SegmentID
  {
    auto lk = std::shared_lock(mMutex);
    return mActiveSegment->id();
  }
  auto useNewAciveSegment() -> SegmentErrc
  {
    auto lk = std::scoped_lock(mMutex);

    if (auto err = mActiveSegment->sync(); !err.ok()) {
      return err;
    }
    auto newSegment = std::make_shared<Segment>(mOption.dirPath.string(), mOption.segmentFileExt,
                                                mActiveSegment->id() + 1, mBlockCache);

    mOlderSegments[mActiveSegment->id()] = mActiveSegment;
    mActiveSegment = newSegment;
    return SegmentErrc::Ok;
  }
  auto write(std::span<std::byte const> data) -> ext::expected<ChunkPosition, WALErrc>
  {
    auto lk = std::scoped_lock(mMutex);

    if (data.size() + CHUNK_HEADER_SIZE > mOption.segmentSize) {
      return ext::make_unexpected(WALErrc::TooLargeValue);
    }
    if (isFull(data.size())) {
      auto err = mActiveSegment->sync();
      if (!err.ok()) {
        return ext::make_unexpected(err);
      }
      mBytesWrite = 0;
      auto segment = std::make_shared<Segment>(mOption.dirPath.string(), mOption.segmentFileExt,
                                               mActiveSegment->id() + 1, mBlockCache);
      mOlderSegments[mActiveSegment->id()] = mActiveSegment;
      mActiveSegment = segment;
    }

    auto pos = mActiveSegment->write(data);
    if (!pos.has_value()) {
      return ext::make_unexpected(pos.error());
    }
    mBytesWrite += pos->mChunkSize;

    auto needSync = mOption.sync;
    if (!needSync && mOption.bytesPerSync > 0) {
      needSync = mBytesWrite >= mOption.bytesPerSync;
    }

    if (needSync) {
      auto err = mActiveSegment->sync();
      if (!err.ok()) {
        return ext::make_unexpected(err);
      }
      mBytesWrite = 0;
    }
    return pos;
  }

  auto read(ChunkPosition const& pos) -> ext::expected<Bytes, WALErrc>
  {
    auto lk = std::shared_lock(mMutex);

    Segment* segment = nullptr;
    if (pos.mSegmentID == mActiveSegment->id()) {
      segment = mActiveSegment.get();
    } else {
      auto iter = mOlderSegments.find(pos.mSegmentID);
      if (iter == mOlderSegments.end()) {
        return ext::make_unexpected(WALErrc::SegmentNotFound);
      }
      segment = iter->second.get();
    }
    return segment->read(pos.mBlockNumber, pos.mChunkOffset);
  }

  auto close() -> WALErrc
  {
    auto lk = std::scoped_lock(mMutex);

    if (mBlockCache != nullptr) {
      mBlockCache->clear();
    }

    for (auto const& [id, segment] : mOlderSegments) {
      auto err = segment->close();
      if (!err.ok()) {
        return err;
      }
    }
    return mActiveSegment->close();
  }

  auto removeFiles() -> WALErrc
  {
    auto lk = std::scoped_lock(mMutex);
    if (mBlockCache != nullptr) {
      mBlockCache->clear();
    }
    for (auto const& [id, segment] : mOlderSegments) {
      auto err = segment->remove();
      if (!err.ok()) {
        return err;
      }
    }
    return mActiveSegment->remove();
  }

  auto sync() -> WALErrc
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

  Option mOption;
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

  auto next(ChunkPosition& pos) -> ext::expected<Bytes, WALErrc>
  {
    if (mCurrReader >= mReaders.size()) {
      return ext::make_unexpected(WALErrc::EndOfSegments);
    }
    pos = ChunkPosition{};
    auto data = mReaders[mCurrReader].next(pos);
    if (!data && data.error().code() == SegmentErrc::EndOfSegment) {
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

private:
  std::vector<SegmentReader> mReaders;
  std::uint32_t mCurrReader;
};

inline auto WAL::readerWithMax(SegmentID segID) -> WALReader
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

inline auto WAL::reader() -> WALReader { return readerWithMax(0); }