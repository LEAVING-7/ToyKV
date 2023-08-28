#pragma once

#include "batch.hpp"
#include "file.hpp"
#include "indexer.hpp"
#include "wal.hpp"
#include <atomic>
#include <filesystem>

using namespace std::literals;
constexpr auto kFileLockName = "FLOCK"sv;
constexpr auto kDataFileNameSuffix = ".SEG"sv;
constexpr auto kHintFileNameSuffix = ".HINT"sv;
constexpr auto kMergeFinNameSuffix = ".MERGEFIN"sv;

constexpr auto kMergeDirSuffixName = "-merge"sv;
constexpr auto kMergeFinishedBatchID = 0;

auto mergeDirPath(std::filesystem::path const& dir) -> std::filesystem::path;

struct DatabaseStat {
  std::uint64_t keyCount;
  std::uint64_t diskSize;
};

class Database {
public:
  Database(DbOption const& option, std::unique_ptr<Wal> dataFiles, std::unique_ptr<Wal> hintFile, Indexer indexer,
           File lockFile, bool closed) noexcept;
  ~Database();
  static auto open(DbOption const& option) -> ext::expected<std::unique_ptr<Database>, std::error_code>;

  auto close() -> void;
  auto sync() -> std::error_code;
  auto stat() -> DatabaseStat;

  auto put(Bytes key, Bytes value) -> std::error_code;
  auto get(Bytes key) -> ext::expected<Bytes, std::error_code>;
  auto del(Bytes key) -> std::error_code;
  auto exist(Bytes key) -> ext::expected<bool, std::error_code>;

  auto newBatch(BatchOption opt) -> std::unique_ptr<Batch>;
  auto merge(bool reopenAfterDoen) -> std::error_code;
  auto isClosed() -> bool { return mClosed; }
  auto isMerging() -> bool { return mMerging.load(); }
  auto getOption() const -> DbOption const& { return mOption; }

  auto setHintFile(std::unique_ptr<Wal> hintFile) -> void;

private:
  friend class Batch;

  auto closeFiles() -> void;
  auto doMerge() -> std::error_code;

private:
  DbOption mOption;
  std::unique_ptr<Wal> mDataFiles;
  std::unique_ptr<Wal> mHintFile;
  std::shared_mutex mMt;
  std::atomic_bool mMerging;
  File mLockFile;
  Indexer mIndexer;
  bool mClosed = false;
};