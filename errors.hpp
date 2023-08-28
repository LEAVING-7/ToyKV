#pragma once
#include <system_error>

enum class SegmentErr {
  Ok = 0,
  SegmentClosed,
  InvalidCheckSum,
  EndOfSegment,
};
struct SegmentErrCatagory : std::error_category {
  auto name() const noexcept -> char const* override;
  auto message(int ev) const -> std::string override;
};
auto segmentErrCatagory() -> SegmentErrCatagory const&;
auto make_error_code(SegmentErr e) -> std::error_code;
auto make_error_condition(SegmentErr e) -> std::error_condition;

enum class WalErr {
  Ok = 0,
  TooLargeValue,
  SegmentClosed,
  InvalidCheckSum,
  EndOfSegments,
  InvalidOption,
};
struct WalErrCatagory : std::error_category {
  auto name() const noexcept -> char const* override;
  auto message(int ev) const -> std::string override;
};
auto walErrCagagory() -> WalErrCatagory const&;
auto make_error_code(WalErr e) -> std::error_code;
auto make_error_condition(WalErr e) -> std::error_condition;

enum class DbErr {
  Ok = 0,
  KeyEmpty,
  KeyNotFound,
  DBIsUsing,
  ReadOnlyBatch,
  BatchCommitted,
  BatchRollbacked,
  DBClosed,
  MergeRunning,
  InvalidDbOption,
};
struct DbErrCatagory : std::error_category {
  auto name() const noexcept -> char const* override;
  auto message(int ev) const -> std::string override;
};
auto make_error_code(DbErr e) -> std::error_code;
auto make_error_condition(DbErr e) -> std::error_condition;

namespace std {
template <>
struct is_error_code_enum<SegmentErr> : true_type {};
template <>
struct is_error_code_enum<WalErr> : true_type {};
template <>
struct is_error_code_enum<DbErr> : true_type {};
} // namespace std
