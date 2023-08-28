#pragma once

#include "errors.hpp"
#include "preclude.hpp"
#include <cstdlib>
#include <filesystem>
#include <unistd.h>

#include <sys/file.h>

namespace stdc {
class File {
public:
  File() = default;
  File(File&& other) : mFp(std::exchange(other.mFp, nullptr)){};
  File& operator=(File&& other)
  {
    mFp = std::exchange(other.mFp, nullptr);
    return *this;
  }
  ~File()
  {
    if (mFp != nullptr) {
      auto r = close();
      assert(r);
    }
  }

  inline static auto open(std::filesystem::path const& path, std::string_view mode, std::size_t bufferSize = 0)
      -> ext::expected<File, std::errc>
  {
    auto fp = std::fopen(path.c_str(), mode.data());
    if (fp == nullptr) {
      return ext::make_unexpected(std::errc(errno));
    }
    if (bufferSize == 0) {
      if (std::setvbuf(fp, nullptr, _IONBF, 0) != 0) {
        return ext::make_unexpected(std::errc::io_error);
      }
    } else if (auto r = std::setvbuf(fp, nullptr, _IOFBF, bufferSize); r != 0) {
      return ext::make_unexpected(std::errc::io_error);
    }
    return File(fp);
  }

  auto write(std::span<std::byte const> bytes) -> std::optional<std::size_t>
  {
    return write(bytes.data(), bytes.size());
  }
  auto write(void const* ptr, std::size_t size) -> std::optional<std::size_t>
  {
    auto n = std::fwrite(ptr, size, 1, mFp);
    if (std::ferror(mFp)) {
      return std::nullopt;
    } else {
      return n;
    }
  }
  auto read(std::span<std::byte> bytes) -> std::optional<std::size_t> { return read(bytes.data(), bytes.size()); }
  auto read(void* ptr, std::size_t size) -> std::optional<std::size_t>
  {
    auto n = std::fread(ptr, size, 1, mFp);
    if (std::ferror(mFp)) {
      return std::nullopt;
    } else {
      return n;
    }
  }
  enum Seek { Set = SEEK_SET, Cur = SEEK_CUR, End = SEEK_END };
  auto seek(std::int64_t offset, Seek whence) -> std::errc
  {
    if (std::fseek(mFp, offset, whence) != 0) {
      return std::errc(errno);
    }
    return std::errc(0);
  }
  auto tell() -> ext::expected<std::int64_t, std::errc>
  {
    auto r = std::ftell(mFp);
    if (r == -1) {
      return ext::make_unexpected(std::errc(errno));
    }
    return r;
  }
  auto close() -> bool
  {
    auto r = std::fclose(mFp) == 0;
    mFp = nullptr;
    return r;
  }
  auto sync() -> std::errc
  {
    auto r = std::fflush(mFp);
    if (r != 0) {
      return std::errc::io_error;
    }
    if (::fsync(fd()) != 0) {
      return std::errc(errno);
    }
    return std::errc(0);
  }
  auto isClosed() const -> bool { return mFp == nullptr; }
  auto rewind() -> decltype(auto) { return seek(0, Set); }
  auto eof() -> bool { return std::feof(mFp) != 0; }
  auto cleareof() -> void { std::clearerr(mFp); }

  auto flush() -> std::errc
  {
    if (std::fflush(mFp) != 0) {
      return std::errc(errno);
    }
    return std::errc(0);
  }

  // platform specific
  auto fd() -> int { return ::fileno(mFp); }
  auto truncate(off64_t length) -> std::errc
  {
    if (auto r = ::ftruncate64(fd(), length); r == -1) {
      return std::errc(errno);
    }
    return std::errc(0);
  }
  enum LockType : std::uint8_t { Shared = LOCK_SH, Exclusive = LOCK_EX };

  auto lock(LockType type) -> std::errc
  {
    if (auto r = ::flock(fd(), type); r == -1) {
      return std::errc(errno);
    }
    return std::errc(0);
  }
  auto tryLock(LockType type) -> std::errc { return lock(LockType(std::uint8_t(type) | LOCK_NB)); }
  auto unlock() -> std::errc
  {
    if (auto r = ::flock(fd(), LOCK_UN); r == -1) {
      return std::errc(errno);
    }
    return std::errc(0);
  }

protected:
  File(std::FILE* fp) : mFp(fp) {}

private:
  std::FILE* mFp = nullptr;
};
} // namespace stdc

#include <fcntl.h>
#include <unistd.h>

namespace np_linux {
class File {
public:
  File() = default;
  File(File const&) = delete;
  File& operator=(File const&) = delete;
  File(File&& other) { std::swap(mFd, other.mFd); };
  File& operator=(File&& other)
  {
    std::swap(mFd, other.mFd);
    return *this;
  }

  ~File()
  {
    if (mFd != -1) {
      auto r = ::close(mFd);
      assert(r);
    }
  }

  inline static auto open(std::filesystem::path const& path, std::string_view mode, std::size_t bufferSize = 0)
      -> ext::expected<File, std::errc>
  {
    auto fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0777);
    if (fd == -1) {
      return ext::make_unexpected(std::errc(errno));
    }
    return File(fd);
  }

  auto write(std::span<std::byte const> bytes) -> std::optional<std::size_t>
  {
    return write(bytes.data(), bytes.size());
  }
  auto write(void const* ptr, std::size_t size) -> std::optional<std::size_t>
  {
    auto n = ::write(mFd, ptr, size);
    if (n == -1) {
      return std::nullopt;
    } else {
      return n;
    }
  }
  auto read(std::span<std::byte> bytes) -> std::optional<std::size_t> { return read(bytes.data(), bytes.size()); }
  auto read(void* ptr, std::size_t size) -> std::optional<std::size_t>
  {
    auto n = ::read(mFd, ptr, size);
    if (n == -1) {
      return std::nullopt;
    } else {
      return n;
    }
  }

  auto seek(std::int64_t offset, int whence) -> std::errc
  {
    if (auto r = ::lseek64(mFd, offset, whence); r == -1) {
      return std::errc(errno);
    }
    return std::errc(0);
  }
  auto tell() -> ext::expected<std::int64_t, std::errc>
  {
    if (auto r = ::lseek64(mFd, 0, SEEK_CUR); r == -1) {
      return ext::make_unexpected(std::errc(errno));
    } else {
      return r;
    }
  }
  auto close() -> bool
  {
    auto r = ::close(mFd) == 0;
    mFd = -1;
    return r;
  }

  auto sync() -> std::errc
  {
    if (::fsync(mFd) != 0) {
      return std::errc(errno);
    }
    return std::errc(0);
  }

  auto isClosed() const -> bool { return mFd == -1; }
  auto rewind() -> decltype(auto) { return seek(0, SEEK_SET); }
  auto eof() -> bool { return ::lseek64(mFd, 0, SEEK_CUR) == ::lseek64(mFd, 0, SEEK_END); }
  auto cleareof() -> void {}
  auto truncate(off64_t length) -> std::errc
  {
    if (auto r = ::ftruncate64(mFd, length); r == -1) {
      return std::errc(errno);
    }
    return std::errc(0);
  }

protected:
  File(int fd) : mFd(fd) {}

private:
  int mFd = -1;
};
} // namespace np_linux

using namespace stdc;

inline auto dirSize(std::filesystem::path const& path) -> std::optional<std::size_t>
{
  if (!std::filesystem::is_directory(path)) {
    return std::nullopt;
  }
  auto size = std::size_t();
  for (auto& p : std::filesystem::recursive_directory_iterator(path)) {
    if (p.is_regular_file()) {
      size += std::filesystem::file_size(p);
    }
  }
  return size;
}