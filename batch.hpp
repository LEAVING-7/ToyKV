#pragma once

#include "record.hpp"
#include "snowflake.hpp"
#include <shared_mutex>

struct BatchOption {
  bool syncWrite;
  bool readOnly;
};

class Database;

class Batch {
public:
  Batch(Database* db, BatchOption option);

  auto reset() -> void;
  auto lockDB() -> void;
  auto unlockDB() -> void;
  auto put(Bytes key, Bytes value) -> std::error_code;
  auto get(Bytes key) -> ext::expected<Bytes, std::error_code>;
  auto del(Bytes key) -> std::error_code;
  auto exist(Bytes key) -> ext::expected<bool, std::error_code>;
  auto commit() -> std::error_code;
  auto rollback() -> std::error_code;

private:
  Database* mDB = nullptr;
  std::unordered_map<Bytes, std::unique_ptr<LogRecord>, BytesHash> mPendingWrites;
  std::shared_mutex mMt;
  snowflake::Node mBatchID;
  BatchOption mOption;
  bool mCommitted;
  bool mRollbacked;
};