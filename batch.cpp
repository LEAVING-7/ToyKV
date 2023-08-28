#include "batch.hpp"
#include "db.hpp"

Batch::Batch(Database* db, BatchOption option) : mDB(db), mOption(option)
{
  mCommitted = false;
  mRollbacked = false;
  if (!mOption.readOnly) {
    mBatchID = snowflake::Node(1);
  }
  // lock
  lockDB();
}
auto Batch::reset() -> void
{
  mCommitted = false;
  mRollbacked = false;
  mPendingWrites.clear();
  mDB = nullptr;
}
auto Batch::lockDB() -> void
{
  if (mOption.readOnly) {
    mDB->mMt.lock_shared();
  } else {
    mDB->mMt.lock();
  }
}
auto Batch::unlockDB() -> void
{
  if (mOption.readOnly) {
    mDB->mMt.unlock_shared();
  } else {
    mDB->mMt.unlock();
  }
}
auto Batch::put(Bytes key, Bytes value) -> std::error_code
{
  if (key.capacity() == 0) {
    return DbErr::KeyEmpty;
  }
  if (mDB->isClosed()) {
    return DbErr::DBClosed;
  }
  if (mOption.readOnly) {
    return DbErr::ReadOnlyBatch;
  }

  mMt.lock();
  mPendingWrites[key] = std::make_unique<LogRecord>(key, value, LogRecordType::Normal, 0);
  mMt.unlock();

  return DbErr::Ok;
}
auto Batch::get(Bytes key) -> ext::expected<Bytes, std::error_code>
{
  if (key.capacity() == 0) {
    return ext::make_unexpected(DbErr::KeyEmpty);
  }
  if (mDB->isClosed()) {
    return ext::make_unexpected(DbErr::DBClosed);
  }
  {
    auto lk = std::shared_lock(mMt);
    if (!mPendingWrites.empty()) {
      // rlock
      if (auto it = mPendingWrites.find(key); it != mPendingWrites.end()) {
        if (it->second->type() == LogRecordType::Delted) {
          return ext::make_unexpected(DbErr::KeyNotFound);
        }
        return it->second->value();
      }
    }
  }
  auto chunkPos = mDB->mIndexer.getPtr(key);
  if (chunkPos == nullptr) {
    return ext::make_unexpected(DbErr::KeyNotFound);
  }
  auto chunk = mDB->mDataFiles->read(*chunkPos);
  if (!chunk.has_value()) {
    return ext::make_unexpected(chunk.error());
  }

  auto record = LogRecord(chunk->span());
  if (record.type() == LogRecordType::Delted) {
    throw std::runtime_error("Deleted record found in data file");
  }
  return record.value();
}
auto Batch::del(Bytes key) -> std::error_code
{
  if (key.capacity() == 0) {
    return DbErr::KeyEmpty;
  }
  if (mDB->isClosed()) {
    return DbErr::DBClosed;
  }
  if (mOption.readOnly) {
    return DbErr::ReadOnlyBatch;
  }

  mMt.lock();
  if (auto pos = mDB->mIndexer.getPtr(key); pos != nullptr) {
    mPendingWrites[key] = std::make_unique<LogRecord>(key, Bytes(), LogRecordType::Delted, 0);
  } else {
    mPendingWrites.erase(key);
  }
  mMt.unlock();

  return DbErr::Ok;
};
auto Batch::exist(Bytes key) -> ext::expected<bool, std::error_code>
{
  if (key.capacity() == 0) {
    return ext::make_unexpected(DbErr::KeyEmpty);
  }
  if (mDB->isClosed()) {
    return ext::make_unexpected(DbErr::DBClosed);
  }
  // rlock
  {
    auto lk = std::shared_lock(mMt);
    if (!mPendingWrites.empty()) {
      if (auto it = mPendingWrites.find(key); it != mPendingWrites.end()) {
        if (it->second->type() == LogRecordType::Delted) {
          return false;
        }
        return true;
      }
    }
  }
  auto pos = mDB->mIndexer.getPtr(key);
  return pos != nullptr;
};
auto Batch::commit() -> std::error_code
{
  if (mDB->isClosed()) {
    unlockDB();
    return DbErr::DBClosed;
  }

  if (mOption.readOnly || mPendingWrites.empty()) {
    unlockDB();
    return DbErr::Ok;
  }

  {
    auto lk = std::scoped_lock(mMt);
    if (mCommitted) {
      unlockDB();
      return DbErr::BatchCommitted;
    }
    if (mRollbacked) {
      unlockDB();
      return DbErr::BatchRollbacked;
    }

    auto batchID = mBatchID.gen();
    auto positions = std::unordered_map<Bytes, ChunkPosition, BytesHash>();

    for (auto const& [k, record] : mPendingWrites) {
      record->setBatchID(batchID.id);
      auto recordBytes = record->asBytes();
      auto pos = mDB->mDataFiles->write(recordBytes.span());
      if (!pos.has_value()) {
        unlockDB();
        return pos.error();
      }
      positions.emplace(record->key(), *pos);
    }

    auto endRecord = LogRecord(Bytes::from(batchID), Bytes(), LogRecordType::Finished, 0);
    if (auto pos = mDB->mDataFiles->write(endRecord.asBytes().span()); !pos.has_value()) {
      unlockDB();
      return pos.error();
    }

    if (mOption.syncWrite && !mDB->mOption.syncWrite) {
      auto err = mDB->mDataFiles->sync();
      if (err) {
        unlockDB();
        return err;
      }
    }

    for (auto const& [k, record] : mPendingWrites) {
      if (record->type() == LogRecordType::Delted) {
        mDB->mIndexer.del(k);
      } else {
        mDB->mIndexer.put(k, positions[k]);
      }

      // TODO watch queue
    }
    mCommitted = true;
    unlockDB();
    return DbErr::Ok;
  }
};
auto Batch::rollback() -> std::error_code
{
  if (mDB->isClosed()) {
    unlockDB();
    return DbErr::DBClosed;
  }

  if (mCommitted) {
    unlockDB();
    return DbErr::BatchCommitted;
  }

  if (mRollbacked) {
    unlockDB();
    return DbErr::BatchRollbacked;
  }

  if (!mOption.readOnly) {
    mPendingWrites.clear();
  }
  mRollbacked = true;
  unlockDB();
  return DbErr::Ok;
};