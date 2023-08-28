#pragma once
#include "preclude.hpp"
#include "segment.hpp"
#include <unordered_map>

class MemoryMap {
public:
  MemoryMap() = default;
  MemoryMap(MemoryMap const&) = delete;
  MemoryMap& operator=(MemoryMap const&) = delete;
  MemoryMap(MemoryMap&&) = default;
  MemoryMap& operator=(MemoryMap&&) = default;
  ~MemoryMap() = default;

  auto put(Bytes bytes, ChunkPosition position) -> void { mMap.emplace(std::move(bytes), position); }

  auto get(Bytes bytes) -> std::optional<ChunkPosition>
  {
    if (auto it = mMap.find(bytes); it != mMap.end()) {
      return it->second;
    }
    return std::nullopt;
  }
  auto getPtr(Bytes bytes) -> ChunkPosition*
  {
    if (auto it = mMap.find(bytes); it != mMap.end()) {
      return &it->second;
    }
    return nullptr;
  }
  auto del(Bytes bytes) -> bool
  {
    if (auto it = mMap.find(bytes); it != mMap.end()) {
      mMap.erase(it);
      return true;
    }
    return false;
  }
  auto remove(Bytes bytes) -> std::optional<ChunkPosition>
  {
    if (auto it = mMap.find(bytes); it != mMap.end()) {
      auto position = it->second;
      mMap.erase(it);
      return position;
    }
    return std::nullopt;
  }
  auto size() const -> std::size_t { return mMap.size(); }

private:
  std::unordered_map<Bytes, ChunkPosition, BytesHash> mMap;
};

using Indexer = MemoryMap;