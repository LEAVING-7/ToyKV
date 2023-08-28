#include "db.hpp"
#include <iostream>

auto loadMergeFiles(std::filesystem::path const& dir) -> std::error_code;
auto loadIndexFromWAL(DbOption const& opt, Wal& datafile, Indexer& indexer, std::error_code& ec) -> void;
auto openMergeFinishedFile(DbOption const& opt) -> ext::expected<std::unique_ptr<Wal>, std::error_code>;
static auto loadIndexFromHintFile(DbOption const& opt, Indexer& indexer, std::error_code& ec) -> std::unique_ptr<Wal>;
static auto openWALFiles(DbOption const& opt, std::error_code& ec) -> std::unique_ptr<Wal>;
auto openMergeDB(DbOption const& option) -> std::unique_ptr<Database>;

auto Database::open(DbOption const& opt) -> ext::expected<std::unique_ptr<Database>, std::error_code>
{
  checkDbOption(opt);
  std::error_code ec;
  if (!std::filesystem::exists(opt.dirPath, ec)) {
    return ext::make_unexpected(ec);
  }
  auto lockFilePath = opt.dirPath / kFileLockName;
  auto lockFile = File::open(lockFilePath, "w");
  if (!lockFile) {
    throw std::runtime_error("failed to open lock file");
  }
  auto e = lockFile->tryLock(File::Exclusive);
  if (e != std::errc(0)) {
    return ext::make_unexpected(make_error_code(e));
  }

  auto dataFiles = openWALFiles(opt, ec);
  if (dataFiles == nullptr) {
    return ext::make_unexpected(ec);
  }
  auto indexer = Indexer();

  if (ec = loadMergeFiles(opt.dirPath); ec) {
    return ext::make_unexpected(ec);
  }

  auto hintFile = loadIndexFromHintFile(opt, indexer, ec);
  if (hintFile == nullptr) {
    return ext::make_unexpected(ec);
  }

  ec = std::error_code();
  loadIndexFromWAL(opt, *dataFiles.get(), indexer, ec);
  if (ec) {
    return ext::make_unexpected(ec);
  }

  return std::make_unique<Database>(opt, std::move(dataFiles), std::move(hintFile), std::move(indexer),
                                    std::move(lockFile).value(), false);
}
Database::~Database() { closeFiles(); }
auto Database::close() -> void
{
  auto lk = std::scoped_lock(mMt);
  closeFiles();
  auto r = mLockFile.unlock();
  assert(r == std::errc(0));
  mClosed = true;
}
auto Database::sync() -> std::error_code
{
  auto lk = std::scoped_lock(mMt);
  return mDataFiles->sync();
}
auto Database::stat() -> DatabaseStat
{
  // auto lk = std::scoped_lock(mMt);
  // auto stat = DatabaseStat(
  // );
  throw std::runtime_error("not implemented");
}
auto Database::put(Bytes key, Bytes value) -> std::error_code
{
  auto batch = newBatch({false, false});
  auto r = batch->put(key, value);
  if (r != DbErr::Ok) {
    batch->rollback();
    return r;
  }
  return batch->commit();
}

auto Database::closeFiles() -> void
{
  if (mDataFiles) {
    auto ok = mDataFiles->close();
    assert(ok);
  }
  if (mHintFile) {
    auto ok = mHintFile->close();
    assert(ok);
  }
}

auto openWALFiles(DbOption const& opt, std::error_code& ec) -> std::unique_ptr<Wal>
{
  auto wal = Wal::create(WalOption{
      .dirPath = opt.dirPath,
      .segmentSize = opt.segmentSize,
      .segmentFileExt = std::string(kDataFileNameSuffix),
      .blockCache = opt.blockCache,
      .syncWrite = opt.syncWrite,
      .bytesPerSync = opt.bytesPerSync,
  });
  if (wal.has_value()) {
    return std::move(wal).value();
  } else {
    ec = wal.error();
    return nullptr;
  }
}

auto Database::newBatch(BatchOption opt) -> std::unique_ptr<Batch>
{
  auto batch = std::make_unique<Batch>(this, BatchOption{
                                                 .syncWrite = opt.syncWrite,
                                                 .readOnly = opt.readOnly,
                                             });
  return batch;
}
auto Database::get(Bytes key) -> ext::expected<Bytes, std::error_code>
{
  auto batch = newBatch({false, true});
  auto ret = batch->get(key);
  auto r = batch->commit();
  assert(!r);
  return ret;
};
auto Database::del(Bytes key) -> std::error_code
{
  auto batch = newBatch({false, false});
  if (auto r = batch->del(key); r != DbErr::Ok) {
    batch->rollback();
    return r;
  }
  return batch->commit();
};
auto Database::exist(Bytes key) -> ext::expected<bool, std::error_code>
{
  auto batch = newBatch({false, true});
  auto res = batch->exist(key);
  auto e = batch->commit();
  assert(e.value() == 0);
  return res;
};

auto mergeDirPath(std::filesystem::path const& dir) -> std::filesystem::path
{
  auto parent = dir.parent_path();
  auto name = dir.filename();
  return parent / name += kMergeDirSuffixName;
}

auto encHintRecord(Bytes key, ChunkPosition const& pos) -> Bytes
{
  auto buf = Bytes(16 + key.capacity());
  enc::put(buf.span(), std::span((std::byte*)&pos, 16));
  enc::put(buf.span().subspan(16), key.span());
  return buf;
}
auto getMergeFinSegmentId(std::filesystem::path const& mergePath) -> SegmentID
{
  auto mergeFileName = segmentFileName(mergePath.native(), kMergeFinNameSuffix, 1);
  if (!std::filesystem::exists(mergeFileName)) {
    return 0;
  }
  auto file = File::open(mergeFileName, "r");
  if (!file) {
    return 0;
  }
  auto buf = std::array<std::byte, 4>();
  auto r = file->read(std::as_writable_bytes(std::span(buf)));
  if (!r) {
    return 0;
  }
  auto segId = std::uint32_t();
  enc::get(buf, segId);
  return segId;
}

auto decHintRecord(std::span<std::byte const> bytes) -> std::pair<Bytes, ChunkPosition>
{
  ChunkPosition pos;
  enc::get(bytes, std::span((std::byte*)&pos, 16));
  auto key = Bytes(bytes.size() - 16);
  enc::get(bytes.subspan(16), key.span());
  return {key, pos};
}

auto loadMergeFiles(std::filesystem::path const& dir) -> std::error_code
{
  auto mergeDir = mergeDirPath(dir);
  if (!std::filesystem::exists(mergeDir)) {
    return DbErr::Ok;
  }

  auto copyFile = [&](std::string_view suffix, std::uint32_t fileId, bool force) {
    auto srcFile = segmentFileName(mergeDir.native(), suffix, fileId);
    if (!std::filesystem::exists(srcFile)) {
      return;
    }
    auto fileStat = std::filesystem::status(srcFile);
    if (!std::filesystem::is_regular_file(fileStat)) {
      return;
    }
    if (!force && std::filesystem::file_size(srcFile) == 0) {
      return;
    }
    auto dstFile = segmentFileName(dir.native(), suffix, fileId);
    std::filesystem::rename(srcFile, dstFile);
  };

  auto mergeFinSegmentId = getMergeFinSegmentId(mergeDir);
  for (int fileId = 1; fileId <= mergeFinSegmentId; fileId++) {
    auto destFile = segmentFileName(dir.native(), kDataFileNameSuffix, fileId);

    if (std::filesystem::exists(destFile)) {
      std::filesystem::remove(destFile);
    }

    copyFile(kDataFileNameSuffix, fileId, false);
  }

  copyFile(kHintFileNameSuffix, 1, true);
  copyFile(kHintFileNameSuffix, 1, true);
  std::filesystem::remove_all(mergeDir);
  return DbErr::Ok;
}

auto Database::merge(bool reopenAfterDoen) -> std::error_code
{
  if (auto e = doMerge(); e) {
    return e;
  }
  if (!reopenAfterDoen) {
    return DbErr::Ok;
  }

  auto lk = std::scoped_lock(mMt);
  closeFiles();

  if (auto e = loadMergeFiles(mOption.dirPath); e) {
    return e;
  }
  auto ec = std::error_code();
  auto dataFiles = openWALFiles(mOption, ec);
  if (!dataFiles) {
    return ec;
  }
  mDataFiles = std::move(dataFiles);

  mHintFile = loadIndexFromHintFile(mOption, mIndexer, ec);
  if (mHintFile == nullptr) {
    return ec;
  }
  ec = std::error_code();
  loadIndexFromWAL(mOption, *dataFiles, mIndexer, ec);
  if (ec) {
    return ec;
  }

  return DbErr::Ok;
}

auto Database::doMerge() -> std::error_code
{
  mMt.lock();
  if (isClosed()) {
    mMt.unlock();
    return DbErr::DBClosed;
  }

  if (mDataFiles->empty()) {
    mMt.unlock();
    return DbErr::Ok;
  }

  if (mMerging.load()) {
    mMt.unlock();
    return DbErr::MergeRunning;
  }

  mMerging.store(true);
  auto _d1 = Defer([&] { mMerging.store(false); });
  auto prevActiveSegId = mDataFiles->activeSegmentID();
  if (auto e = mDataFiles->useNewAciveSegment(); e) {
    mMt.unlock();
    return e;
  }

  mMt.unlock();
  auto mergeDB = openMergeDB(mOption);

  auto reader = mDataFiles->readerWithMax(prevActiveSegId);
  for (;;) {
    auto pos = ChunkPosition();
    auto chunk = reader.next(pos);
    if (!chunk) {
      if (chunk.error() == WalErr::EndOfSegments) {
        break;
      }
      return chunk.error();
    }
    auto record = LogRecord(chunk.value().span());
    if (record.type() == LogRecordType::Normal) {

      mMt.lock_shared();
      auto idxPos = mIndexer.get(record.key());
      mMt.unlock_shared();

      if (idxPos.has_value() && idxPos == pos) {
        record.setBatchID(kMergeFinishedBatchID);
        auto newPos = mergeDB->mDataFiles->write(record.asBytes().span());
        if (!newPos) {
          return newPos.error();
        }
        auto hintRecord = encHintRecord(record.key(), newPos.value());
        auto res = mergeDB->mHintFile->write(hintRecord.span());
        if (!res) {
          return res.error();
        }
      }
    }
  }

  auto mergeFinFile = openMergeFinishedFile(mOption);
  if (!mergeFinFile) {
    throw std::runtime_error("failed to open merge db finished file");
  }
  auto segIdBuf = std::array<std::byte, 4>();
  enc::put(segIdBuf, std::uint32_t(prevActiveSegId));
  auto res = mergeFinFile->get()->write(std::as_bytes(std::span(segIdBuf)));
  if (!res) {
    return res.error();
  }

  auto ok = mergeFinFile->get()->close();
  assert(ok);
  return DbErr::Ok;
};

auto openMergeDB(DbOption const& option) -> std::unique_ptr<Database>
{
  auto mergePath = mergeDirPath(option.dirPath);
  if (std::filesystem::exists(mergePath)) {
    std::filesystem::remove_all(mergePath);
  }
  auto opt = option;
  opt.syncWrite = false;
  opt.bytesPerSync = 0;
  opt.dirPath = mergePath;

  auto mergeDB = Database::open(opt);
  if (!mergeDB) {
    throw std::runtime_error("failed to open merge db");
  }
  auto hintFile = Wal::create(WalOption{
      .dirPath = opt.dirPath,
      .segmentSize = std::numeric_limits<std::int64_t>().max(),
      .segmentFileExt = std::string(kHintFileNameSuffix),
      .blockCache = 0,
      .syncWrite = false,
      .bytesPerSync = 0,
  });
  if (!hintFile) {
    throw std::runtime_error("failed to open merge db hint file");
  }
  mergeDB->get()->setHintFile(std::move(hintFile).value());
  return std::move(mergeDB).value();
};

auto openMergeFinishedFile(DbOption const& opt) -> ext::expected<std::unique_ptr<Wal>, std::error_code>
{
  auto ret = Wal::create(WalOption{
      .dirPath = opt.dirPath,
      .segmentSize = 1 * GiB,
      .segmentFileExt = std::string(kMergeFinNameSuffix),
      .blockCache = 0,
      .syncWrite = false,
      .bytesPerSync = 0,
  });
  return ret;
}

auto loadIndexFromHintFile(DbOption const& opt, Indexer& indexer, std::error_code& ec) -> std::unique_ptr<Wal>
{
  auto hintFile = Wal::create(WalOption{
      .dirPath = opt.dirPath,
      .segmentSize = std::numeric_limits<std::int64_t>().max(),
      .segmentFileExt = std::string(kHintFileNameSuffix),
      .blockCache = 32 * KiB * 10,
  });
  if (!hintFile) {
    ec = hintFile.error();
    return nullptr;
  }
  auto reader = hintFile->get()->reader();
  for (;;) {
    auto pos = ChunkPosition();
    auto chunk = reader.next(pos);
    if (!chunk) {
      if (chunk.error() == WalErr::EndOfSegments) {
        break;
      }
      ec = chunk.error();
      return nullptr;
    }
    auto [key, idxPos] = decHintRecord(chunk.value().span());

    indexer.put(key, idxPos);
  }
  return std::move(hintFile).value();
};

auto loadIndexFromWAL(DbOption const& opt, Wal& datafile, Indexer& indexer, std::error_code& ec) -> void
{
  auto mergeFinSegmentId = getMergeFinSegmentId(opt.dirPath);
  auto indexRecords = std::unordered_map<std::uint64_t, std::vector<IndexRecord>>();

  auto reader = datafile.reader();
  for (;;) {
    auto readers = reader.readers();
    auto currentReaderIdx = reader.currentReaderIdx();
    auto const& currReader = readers[currentReaderIdx];

    if (currReader.id() <= mergeFinSegmentId) {
      reader.skipCurrentSegment();
      continue;
    }

    auto pos = ChunkPosition();
    auto chunk = reader.next(pos);
    if (!chunk) {
      if (chunk.error() == WalErr::EndOfSegments) {
        break;
      }
      ec = chunk.error();
      return;
    }
    auto record = LogRecord(chunk.value().span());
    if (record.type() == LogRecordType::Finished) {
      std::uint64_t batchId = 0;
      enc::get(record.key().span(), batchId);
      for (auto const& indexRecord : indexRecords[batchId]) {
        if (indexRecord.mType == LogRecordType::Normal) {
          indexer.put(indexRecord.mKey, indexRecord.position);
        }
        if (indexRecord.mType == LogRecordType::Delted) {
          indexer.del(indexRecord.mKey);
        }
      }
      indexRecords.erase(batchId);
    } else if (record.type() == LogRecordType::Normal && record.batchID() == mergeFinSegmentId) {
      indexer.put(record.key(), pos);
    } else {
      indexRecords[record.batchID()].push_back(IndexRecord{
          .mKey = record.key(),
          .mType = record.type(),
          .position = pos,
      });
    }
  }
};

Database::Database(DbOption const& option, std::unique_ptr<Wal> dataFiles, std::unique_ptr<Wal> hintFile,
                   Indexer indexer, File lockFile, bool closed) noexcept
    : mOption(option), mDataFiles(std::move(dataFiles)), mHintFile(std::move(hintFile)), mLockFile(std::move(lockFile)),
      mIndexer(std::move(indexer)), mClosed(closed)
{
}
auto Database::setHintFile(std::unique_ptr<Wal> hintFile) -> void
{
  std::unique_lock lock(mMt);
  mHintFile = std::move(hintFile);
}
