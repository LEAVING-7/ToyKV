#include "errors.hpp"
auto SegmentErrCatagory::name() const noexcept -> char const* { return "SegmentError"; }
auto SegmentErrCatagory::message(int ev) const -> std::string
{
  switch (static_cast<SegmentErr>(ev)) {
  case SegmentErr::Ok:
    return "Ok";
  case SegmentErr::SegmentClosed:
    return "SegmentClosed";
  case SegmentErr::InvalidCheckSum:
    return "InvalidCheckSum";
  case SegmentErr::EndOfSegment:
    return "EndOfSegment";

  default:
    return "Unknown";
  }
}
auto segmentErrCatagory() -> SegmentErrCatagory const&
{
  static SegmentErrCatagory catagory;
  return catagory;
}
auto make_error_code(SegmentErr e) -> std::error_code { return {static_cast<int>(e), segmentErrCatagory()}; }
auto make_error_condition(SegmentErr e) -> std::error_condition { return {static_cast<int>(e), segmentErrCatagory()}; }
auto WalErrCatagory::name() const noexcept -> char const* { return "WALError"; }
auto WalErrCatagory::message(int ev) const -> std::string
{
  switch (static_cast<WalErr>(ev)) {
  case WalErr::Ok:
    return "Ok";
  case WalErr::TooLargeValue:
    return "TooLargeValue";
  case WalErr::SegmentClosed:
    return "SegmentClosed";
  case WalErr::InvalidCheckSum:
    return "InvalidCheckSum";
  case WalErr::EndOfSegments:
    return "EndOfSegments";
  case WalErr::InvalidOption:
    return "InvalidOption";
  default:
    return "Unknown";
  }
}
auto walErrCagagory() -> WalErrCatagory const&
{
  static WalErrCatagory catagory;
  return catagory;
}
auto make_error_code(WalErr e) -> std::error_code { return {static_cast<int>(e), walErrCagagory()}; }
auto make_error_condition(WalErr e) -> std::error_condition { return {static_cast<int>(e), walErrCagagory()}; }
auto DbErrCatagory::name() const noexcept -> char const* { return "DBError"; }
auto DbErrCatagory::message(int ev) const -> std::string
{
  switch (static_cast<DbErr>(ev)) {
  case DbErr::Ok:
    return "Ok";
  case DbErr::KeyEmpty:
    return "KeyEmpty";
  case DbErr::KeyNotFound:
    return "KeyNotFound";
  case DbErr::DBIsUsing:
    return "DBIsUsing";
  case DbErr::ReadOnlyBatch:
    return "ReadOnlyBatch";
  case DbErr::BatchCommitted:
    return "BatchCommitted";
  case DbErr::BatchRollbacked:
    return "BatchRollbacked";
  case DbErr::DBClosed:
    return "DBClosed";
  case DbErr::MergeRunning:
    return "MergeRunning";
  case DbErr::InvalidDbOption:
    return "InvalidDbOption";
  default:
    return "Unknown";
  }
}
auto dbErrCatagory() -> DbErrCatagory const&
{
  static DbErrCatagory catagory;
  return catagory;
}
auto make_error_code(DbErr e) -> std::error_code { return {static_cast<int>(e), dbErrCatagory()}; }
auto make_error_condition(DbErr e) -> std::error_condition { return {static_cast<int>(e), dbErrCatagory()}; }
