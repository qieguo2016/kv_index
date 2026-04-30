#ifndef KV_INDEX_SRC_CORE_BYTE_IO_H_
#define KV_INDEX_SRC_CORE_BYTE_IO_H_

#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <span>
#include <type_traits>

#include "kv_index/status.h"

namespace kv_index::internal {

constexpr std::uint64_t AlignUp(std::uint64_t value,
                                std::uint64_t alignment) noexcept {
  if (alignment == 0) {
    return value;
  }
  const std::uint64_t remainder = value % alignment;
  if (remainder == 0) {
    return value;
  }
  return value + (alignment - remainder);
}

template <typename T>
concept LittleEndianInteger =
    std::integral<T> && !std::same_as<std::remove_cv_t<T>, bool>;

inline Status CheckSpanBounds(std::size_t size, std::size_t offset,
                              std::size_t byte_count) {
  if (offset > size || size - offset < byte_count) {
    return Status::InvalidArgument("little-endian fixed-width access is out of bounds");
  }
  return Status::Ok();
}

template <LittleEndianInteger T>
Status WriteLittleEndian(T value, std::span<std::byte> bytes,
                         std::size_t offset = 0) {
  if (const Status status = CheckSpanBounds(bytes.size(), offset, sizeof(T));
      !status.ok()) {
    return status;
  }

  using UnsignedT = std::make_unsigned_t<T>;
  UnsignedT unsigned_value;
  if constexpr (std::is_signed_v<T>) {
    unsigned_value = std::bit_cast<UnsignedT>(value);
  } else {
    unsigned_value = value;
  }

  for (std::size_t i = 0; i < sizeof(T); ++i) {
    bytes[offset + i] =
        static_cast<std::byte>((unsigned_value >> (i * 8U)) & 0xffU);
  }
  return Status::Ok();
}

template <LittleEndianInteger T>
StatusOr<T> ReadLittleEndian(std::span<const std::byte> bytes,
                             std::size_t offset = 0) {
  if (const Status status = CheckSpanBounds(bytes.size(), offset, sizeof(T));
      !status.ok()) {
    return status;
  }

  using UnsignedT = std::make_unsigned_t<T>;
  UnsignedT unsigned_value = 0;
  for (std::size_t i = 0; i < sizeof(T); ++i) {
    unsigned_value |=
        static_cast<UnsignedT>(std::to_integer<unsigned char>(bytes[offset + i]))
        << (i * 8U);
  }

  if constexpr (std::is_signed_v<T>) {
    return std::bit_cast<T>(unsigned_value);
  } else {
    return unsigned_value;
  }
}

struct ValueRef16 {
  std::uint64_t offset = 0;
  std::uint32_t byte_length = 0;
  std::uint32_t element_count_or_flags = 0;
};

static_assert(sizeof(ValueRef16) == 16);

inline Status WriteValueRef16(const ValueRef16& ref, std::span<std::byte> bytes,
                              std::size_t offset = 0) {
  if (const Status status = CheckSpanBounds(bytes.size(), offset, 16);
      !status.ok()) {
    return status;
  }
  if (const Status status = WriteLittleEndian(ref.offset, bytes, offset);
      !status.ok()) {
    return status;
  }
  if (const Status status = WriteLittleEndian(ref.byte_length, bytes, offset + 8);
      !status.ok()) {
    return status;
  }
  return WriteLittleEndian(ref.element_count_or_flags, bytes, offset + 12);
}

inline StatusOr<ValueRef16> DecodeValueRef16(std::span<const std::byte> bytes,
                                             std::size_t offset = 0) {
  if (const Status status = CheckSpanBounds(bytes.size(), offset, 16);
      !status.ok()) {
    return status;
  }

  auto ref_offset = ReadLittleEndian<std::uint64_t>(bytes, offset);
  if (!ref_offset.ok()) {
    return ref_offset.status();
  }
  auto byte_length = ReadLittleEndian<std::uint32_t>(bytes, offset + 8);
  if (!byte_length.ok()) {
    return byte_length.status();
  }
  auto element_count_or_flags =
      ReadLittleEndian<std::uint32_t>(bytes, offset + 12);
  if (!element_count_or_flags.ok()) {
    return element_count_or_flags.status();
  }

  return ValueRef16{
      .offset = ref_offset.value(),
      .byte_length = byte_length.value(),
      .element_count_or_flags = element_count_or_flags.value(),
  };
}

}  // namespace kv_index::internal

#endif  // KV_INDEX_SRC_CORE_BYTE_IO_H_
