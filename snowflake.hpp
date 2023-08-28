#pragma once

#include <chrono>
#include <cstdint>

namespace snowflake {
constexpr std::uint64_t kEpoch = 1288834974657;
struct ID {
  union {
    struct {
      std::uint64_t unused : 1;
      std::uint64_t timestamp : 41;
      std::uint64_t node : 10;
      std::uint64_t seq : 12;
    } raw;
    std::uint64_t id;
  };

  operator std::span<std::byte const>()
  {
    return std::span<std::byte const>(reinterpret_cast<std::byte const*>(&id), sizeof(id));
  }
};

class Node {
public:
  Node() = default;
  Node(std::uint32_t nodeId) noexcept : mNodeId(nodeId) {}
  auto gen() noexcept -> ID
  {
    using namespace std::chrono;
    auto now = (std::uint64_t)duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
    if (now == mLastTimestamp) {
      mStep = (mStep + 1) & 0xFFF;
      if (mStep == 0) {
        while (now <= mLastTimestamp) {
          now = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
        }
      }
    } else {
      mStep = 0;
    }
    mLastTimestamp = now;
    return {
        .raw =
            {
                .unused = 0,
                .timestamp = now - kEpoch,
                .node = mNodeId,
                .seq = mStep,
            },
    };
  }

private:
  std::uint64_t mLastTimestamp;
  std::uint32_t mStep;
  std::uint32_t mNodeId;
};
}; // namespace snowflake
