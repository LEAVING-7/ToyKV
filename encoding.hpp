#pragma once
#include <algorithm>
#include <cstddef>
#include <cstring>
#include <limits>
#include <span>
#include <stdexcept>

namespace enc {
template <typename T>
inline constexpr bool is_span = false;
template <typename T, std::size_t N>
inline constexpr bool is_span<std::span<T, N>> = true;

constexpr std::size_t PTR_TAG = std::numeric_limits<std::size_t>::max();

template <typename T>
constexpr auto getReadableBytes(T const& buf) -> std::span<std::byte const>
{
  using RawType = std::remove_cvref_t<T>;
  if constexpr (std::is_pointer_v<RawType>) {
    return {reinterpret_cast<std::byte const*>(buf), PTR_TAG};
  } else if constexpr (is_span<RawType>) {
    return std::as_bytes(buf);
  } else if constexpr (requires { std::span(buf); }) {
    return std::as_bytes(std::span(buf));
  } else if constexpr (std::is_trivially_copyable_v<RawType>) {
    return {reinterpret_cast<std::byte const*>(&buf), sizeof(RawType)};
  } else {
    static_assert(std::is_trivially_copyable_v<RawType>, "not support type");
  }
}

template <typename T>
constexpr auto getWritableBytes(T& buf) -> std::span<std::byte>
{
  using RawType = std::remove_cvref_t<T>;
  if constexpr (std::is_pointer_v<RawType>) {
    return {reinterpret_cast<std::byte*>(buf), PTR_TAG};
  } else if constexpr (is_span<RawType>) {
    return std::as_writable_bytes(buf);
  } else if constexpr (requires { std::span(buf); }) {
    return std::as_writable_bytes(std::span(buf));
  } else if constexpr (std::is_trivially_copyable_v<RawType>) {
    return {reinterpret_cast<std::byte*>(&buf), sizeof(RawType)};
  } else {
    static_assert(std::is_trivially_copyable_v<RawType>, "not support type");
  }
}

template <std::endian to, std::endian from>
inline auto copyNBytes(std::byte* dst, std::byte const* src, std::size_t n)
{
  if constexpr (from == to) {
    std::copy_n(src, n, dst);
  } else {
    std::reverse_copy(src, src + n, dst);
  }
}

template <std::endian to = std::endian::native, std::endian from = std::endian::native>
auto put(auto&& buf, auto const& value) -> void
{
  auto dst = getWritableBytes(buf);
  auto src = getReadableBytes(value);
  if (dst.size() == PTR_TAG && src.size() == PTR_TAG) {
    return;
  }
  if (dst.size() < src.size() && src.size() != PTR_TAG) {
    throw std::out_of_range("put: dst is too small");
  }
  auto n = std::min(src.size(), dst.size());
  copyNBytes<to, from>(dst.data(), src.data(), n);
}

template <std::endian to = std::endian::native, std::endian from = std::endian::native>
auto get(auto const& buf, auto&& value) -> void
{
  auto src = getReadableBytes(buf);
  auto dst = getWritableBytes(value);
  if (dst.size() == PTR_TAG && src.size() == PTR_TAG) {
    return;
  }
  auto n = std::min(src.size(), dst.size());
  copyNBytes<to, from>(dst.data(), src.data(), n);
}
} // namespace enc
