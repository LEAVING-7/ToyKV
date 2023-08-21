#include <cerrno>
#include <cstdint>

class Errc {
public:
  enum class Type : std::uint8_t {
    Sys,
    Segment,
    WAL,
  };
  static constexpr inline std::uint16_t Ok = 0;
  Errc() = delete;
  Errc(Type type) : mErrCode(Ok), mErrType(type) {}
  Errc(Type type, std::uint16_t code) : mErrCode(code), mErrType(type) {}

  [[nodiscard]] auto ok() const -> bool { return mErrCode == Ok; }
  [[nodiscard]] auto code() const -> std::uint16_t { return mErrCode; }
  [[nodiscard]] auto type() const -> Type { return mErrType; }

  template <Type errType>
  [[nodiscard]] auto is() const -> bool
  {
    return mErrType == errType;
  }

  operator bool() const { return ok(); }

  auto operator==(Errc const& rhs) const -> bool = default;

private:
  std::uint16_t mErrCode;
  Type mErrType;
};

class SysErrc : public Errc {
public:
  SysErrc() : Errc(Errc::Type::Sys, Errc::Ok) {}
  SysErrc(std::uint16_t code) : Errc(Errc::Type::Sys, code) {}

  static inline auto lastSysErrc() -> SysErrc { return {std::uint16_t(errno)}; }

protected:
  SysErrc(Errc::Type type, std::uint16_t code) : Errc(type, code) {}
};

class SegmentErrc : public SysErrc {
public:
  enum Type : std::uint16_t {
    Ok = 0,
    SegmentClosed,
    InvalidChecksum,
    EndOfSegment,
  };
  SegmentErrc() : SysErrc(Errc::Type::Segment, Errc::Ok) {}
  SegmentErrc(Type type) : SysErrc(Errc::Type::Segment, static_cast<std::uint16_t>(type)) {}
  SegmentErrc(SysErrc syserr) : SysErrc(syserr) {}

protected:
  SegmentErrc(Errc::Type type, std::uint16_t code) : SysErrc(type, code) {}
};

class WALErrc : public SegmentErrc {
public:
  enum Type : std::uint16_t { Ok = 0, TooLargeValue, SegmentNotFound, EndOfSegments };

  WALErrc() : SegmentErrc(Errc::Type::WAL, Errc::Ok) {}
  WALErrc(Type type) : SegmentErrc(Errc::Type::WAL, static_cast<std::uint16_t>(type)) {}
  WALErrc(SegmentErrc segerr) : SegmentErrc(segerr) {}
};
