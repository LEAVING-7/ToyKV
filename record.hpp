#pragma once

#include "encoding.hpp"
#include "preclude.hpp"
#include "segment.hpp"

enum LogRecordType : std::uint8_t {
  Normal,
  Delted,
  Finished,
};
class LogRecord {
public:
  LogRecord() = delete;
  LogRecord(Bytes key, Bytes value, LogRecordType type, std::uint64_t batchID, std::uint64_t expireAt = 0)
      : mKey(std::move(key)), mValue(std::move(value)), mType(type), mBatchID(batchID)
  {
  }
  LogRecord(std::span<std::byte const> bytes)
  {
    auto span = bytes;
    mType = LogRecordType(std::to_integer<std::uint8_t>(span[0]));
    enc::get(span.subspan(1), mBatchID);
    std::uint32_t keySize, valueSize;
    enc::get(span.subspan(9), keySize);
    enc::get(span.subspan(13), valueSize);
    mKey = Bytes(keySize);
    mValue = Bytes(valueSize);
    enc::get(span.subspan(17), mKey.span());
    enc::get(span.subspan(17 + keySize), mValue.span());
  }
  LogRecord(LogRecord const&) = default;
  LogRecord& operator=(LogRecord const&) = default;
  LogRecord(LogRecord&&) = default;
  LogRecord& operator=(LogRecord&&) = default;

  auto asBytes() const -> Bytes
  {
    auto ret = Bytes(1 + 8 + 8 + 4 + 4 + mKey.capacity() + mValue.capacity());
    auto span = ret.span();
    span[0] = std::byte(mType);
    enc::put(span.subspan(1), mBatchID);
    enc::put(span.subspan(9), std::uint32_t(mKey.capacity()));
    enc::put(span.subspan(13), std::uint32_t(mValue.capacity()));
    enc::put(span.subspan(17), mKey.span());
    enc::put(span.subspan(17 + mKey.capacity()), mValue.span());
    return ret;
  }
  auto key() const -> Bytes const& { return mKey; }
  auto value() const -> Bytes const& { return mValue; }
  auto type() const -> LogRecordType { return mType; }
  auto batchID() const -> std::uint64_t { return mBatchID; }

  auto setBatchID(std::uint64_t id) -> void { mBatchID = id; }

private:
  Bytes mKey;
  Bytes mValue;
  LogRecordType mType;
  std::uint64_t mBatchID;
};

struct IndexRecord {
  Bytes mKey;
  LogRecordType mType;
  ChunkPosition position;
};
