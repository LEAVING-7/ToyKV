#pragma once
#include <cstdint>
#include <list>
#include <optional>
#include <unordered_map>

template <typename K, typename V>
struct KVPair {
  K key;
  V value;
};

template <typename K, typename V>
class Cache {
public:
  explicit Cache(std::size_t capacity = 64, std::size_t elasticity = 10) : mCapacity(capacity), mElasticity(elasticity)
  {
  }
  Cache(Cache const& other) = delete;
  Cache(Cache&& other) = delete;
  Cache& operator=(Cache&& other) = delete;
  Cache& operator=(Cache const& other) = delete;
  ~Cache() = default;

  auto size() const -> std::size_t { return mCache.size(); }
  auto capacity() const -> std::size_t { return mCapacity; }
  auto empty() const -> bool { return mCache.empty(); }
  auto clear() -> void
  {
    mCache.clear();
    mIndex.clear();
  }

  auto put(K&& key, V&& value) -> std::size_t
  {
    auto const it = mIndex.find(key);
    if (it != mIndex.end()) {
      it->second->value = std::move(value);
      mCache.splice(mCache.begin(), mCache, it->second);
      return 0;
    }
    mCache.emplace_front(key, std::move(value));
    mIndex[key] = mCache.begin();
    return prune();
  }

  auto get(K const& key) -> V*
  {
    auto const it = mIndex.find(key);
    if (it == mIndex.end()) {
      return nullptr;
    }
    mCache.splice(mCache.begin(), mCache, it->second);
    return &it->second->value;
  }

  auto remove(K const& key) -> std::optional<V>
  {
    auto const it = mIndex.find(key);
    if (it == mIndex.end()) {
      return std::nullopt;
    }
    auto value = std::move(it->second->value);
    mCache.erase(it->second);
    mIndex.erase(it);
    return value;
  }

  auto contains(K const& key) const -> bool { return mIndex.find(key) != mIndex.end(); }

private:
  auto prune() -> std::size_t
  {
    auto maxAllowed = mCapacity + mElasticity;
    if (mCapacity == 0 || mIndex.size() <= maxAllowed) {
      return 0;
    }
    std::size_t count = 0;
    while (mIndex.size() > mCapacity) {
      mIndex.erase(mCache.back().key);
      mCache.pop_back();
      ++count;
    }
    return count;
  }
  std::list<KVPair<K, V>> mCache;
  std::unordered_map<K, typename decltype(mCache)::iterator> mIndex;

  const std::size_t mCapacity;
  const std::size_t mElasticity;
};