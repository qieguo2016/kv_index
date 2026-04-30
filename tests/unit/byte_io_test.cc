#include "src/core/byte_io.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>

#include "kv_index/status.h"
#include "tests/test_support/test_macros.h"

namespace {

using kv_index::StatusCode;
using kv_index::internal::AlignUp;
using kv_index::internal::DecodeValueRef16;
using kv_index::internal::ReadLittleEndian;
using kv_index::internal::ValueRef16;
using kv_index::internal::WriteLittleEndian;
using kv_index::internal::WriteValueRef16;

void FixedWidthIntegersUsePinnedLittleEndianBytes() {
  std::array<std::byte, 16> bytes{};

  KV_INDEX_CHECK(WriteLittleEndian<std::uint64_t>(
                     0x0123456789abcdefULL, std::span<std::byte>(bytes), 1)
                     .ok());
  KV_INDEX_CHECK_EQ(static_cast<unsigned>(bytes[1]), 0xef);
  KV_INDEX_CHECK_EQ(static_cast<unsigned>(bytes[2]), 0xcd);
  KV_INDEX_CHECK_EQ(static_cast<unsigned>(bytes[3]), 0xab);
  KV_INDEX_CHECK_EQ(static_cast<unsigned>(bytes[4]), 0x89);
  KV_INDEX_CHECK_EQ(static_cast<unsigned>(bytes[5]), 0x67);
  KV_INDEX_CHECK_EQ(static_cast<unsigned>(bytes[6]), 0x45);
  KV_INDEX_CHECK_EQ(static_cast<unsigned>(bytes[7]), 0x23);
  KV_INDEX_CHECK_EQ(static_cast<unsigned>(bytes[8]), 0x01);

  const auto read =
      ReadLittleEndian<std::uint64_t>(std::span<const std::byte>(bytes), 1);
  KV_INDEX_CHECK(read.ok());
  KV_INDEX_CHECK_EQ(read.value(), 0x0123456789abcdefULL);
}

void FixedWidthReadsAndWritesRejectTruncatedBuffers() {
  std::array<std::byte, 4> bytes{};

  const auto write_status =
      WriteLittleEndian<std::uint64_t>(7, std::span<std::byte>(bytes), 0);
  KV_INDEX_CHECK(!write_status.ok());
  KV_INDEX_CHECK_EQ(write_status.code(), StatusCode::kInvalidArgument);

  const auto read =
      ReadLittleEndian<std::uint64_t>(std::span<const std::byte>(bytes), 0);
  KV_INDEX_CHECK(!read.ok());
  KV_INDEX_CHECK_EQ(read.status().code(), StatusCode::kInvalidArgument);
}

void AlignUpHandlesRowSlotAlignmentEdges() {
  KV_INDEX_CHECK_EQ(AlignUp(0, 8), 0);
  KV_INDEX_CHECK_EQ(AlignUp(1, 8), 8);
  KV_INDEX_CHECK_EQ(AlignUp(8, 8), 8);
  KV_INDEX_CHECK_EQ(AlignUp(9, 8), 16);
  KV_INDEX_CHECK_EQ(AlignUp(63, 16), 64);
}

void ValueRef16HasPinnedEncodingAndRoundTrips() {
  const ValueRef16 ref{
      .offset = 0x0102030405060708ULL,
      .byte_length = 0x11223344U,
      .element_count_or_flags = 0x55667788U,
  };
  std::array<std::byte, 16> bytes{};

  KV_INDEX_CHECK(
      WriteValueRef16(ref, std::span<std::byte>(bytes), 0).ok());
  KV_INDEX_CHECK_EQ(static_cast<unsigned>(bytes[0]), 0x08);
  KV_INDEX_CHECK_EQ(static_cast<unsigned>(bytes[7]), 0x01);
  KV_INDEX_CHECK_EQ(static_cast<unsigned>(bytes[8]), 0x44);
  KV_INDEX_CHECK_EQ(static_cast<unsigned>(bytes[11]), 0x11);
  KV_INDEX_CHECK_EQ(static_cast<unsigned>(bytes[12]), 0x88);
  KV_INDEX_CHECK_EQ(static_cast<unsigned>(bytes[15]), 0x55);

  const auto decoded =
      DecodeValueRef16(std::span<const std::byte>(bytes), 0);
  KV_INDEX_CHECK(decoded.ok());
  KV_INDEX_CHECK_EQ(decoded->offset, ref.offset);
  KV_INDEX_CHECK_EQ(decoded->byte_length, ref.byte_length);
  KV_INDEX_CHECK_EQ(decoded->element_count_or_flags,
                    ref.element_count_or_flags);
}

void ValueRef16RejectsTruncatedInput() {
  std::array<std::byte, 15> bytes{};

  const auto decoded =
      DecodeValueRef16(std::span<const std::byte>(bytes), 0);
  KV_INDEX_CHECK(!decoded.ok());
  KV_INDEX_CHECK_EQ(decoded.status().code(), StatusCode::kInvalidArgument);

  const ValueRef16 ref{};
  const auto write_status =
      WriteValueRef16(ref, std::span<std::byte>(bytes), 0);
  KV_INDEX_CHECK(!write_status.ok());
  KV_INDEX_CHECK_EQ(write_status.code(), StatusCode::kInvalidArgument);
}

}  // namespace

int main() {
  FixedWidthIntegersUsePinnedLittleEndianBytes();
  FixedWidthReadsAndWritesRejectTruncatedBuffers();
  AlignUpHandlesRowSlotAlignmentEdges();
  ValueRef16HasPinnedEncodingAndRoundTrips();
  ValueRef16RejectsTruncatedInput();
  return 0;
}
