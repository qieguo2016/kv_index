#ifndef KV_INDEX_SRC_CORE_ROW_STORAGE_H_
#define KV_INDEX_SRC_CORE_ROW_STORAGE_H_

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "kv_index/row.h"
#include "kv_index/schema.h"
#include "kv_index/status.h"
#include "src/core/byte_io.h"

namespace kv_index::internal {

struct EncodedRow {
  std::uint64_t schema_version = 0;
  std::uint64_t layout_fingerprint = 0;
  std::vector<std::byte> row_slot;
  std::vector<std::byte> arena;
  std::vector<std::string> string_dictionary;
  std::vector<std::vector<std::byte>> scalar_list_dictionary;
  std::vector<std::vector<std::string>> string_list_dictionary;
  std::vector<std::string> string_element_dictionary;
};

EncodedRow CreateEncodedRow(const CompiledRowLayout& layout);
StatusOr<Row> MaterializeRow(std::shared_ptr<const CompiledRowLayout> layout,
                             EncodedRow encoded);

Status SetFieldPresent(const FieldLayout& field, EncodedRow* encoded);
Status WriteValueRefField(const FieldLayout& field, const ValueRef16& ref,
                          EncodedRow* encoded);
Status WriteArenaStringField(const FieldLayout& field, std::string_view value,
                             EncodedRow* encoded);
Status WriteDictionaryStringField(const FieldLayout& field,
                                  std::uint32_t dictionary_id,
                                  EncodedRow* encoded);
Status WriteListDictionaryField(const FieldLayout& field,
                                std::uint32_t dictionary_id,
                                std::uint32_t element_count,
                                EncodedRow* encoded);
Status WriteElementDictionaryStringListField(
    const FieldLayout& field, std::span<const std::uint32_t> dictionary_ids,
    EncodedRow* encoded);

template <SupportedFieldCppType T>
StatusOr<std::vector<std::byte>> EncodeScalarListSpan(
    std::span<const T> values) {
  if constexpr (std::same_as<std::remove_cv_t<T>, std::string>) {
    return Status::InvalidArgument("string lists require ref-based encoding");
  } else {
    std::vector<std::byte> bytes(values.size() * FieldTypeSize(FieldTypeFor<T>()));
    for (std::size_t i = 0; i < values.size(); ++i) {
      if constexpr (std::same_as<std::remove_cv_t<T>, bool>) {
        const std::uint8_t encoded_bool = values[i] ? 1 : 0;
        if (const Status status = WriteLittleEndian<std::uint8_t>(
                encoded_bool, std::span<std::byte>(bytes),
                i * FieldTypeSize(FieldTypeFor<T>()));
            !status.ok()) {
          return status;
        }
      } else {
        if (const Status status = WriteLittleEndian<std::remove_cv_t<T>>(
                values[i], std::span<std::byte>(bytes),
                i * FieldTypeSize(FieldTypeFor<T>()));
            !status.ok()) {
          return status;
        }
      }
    }
    return bytes;
  }
}

template <typename Container>
auto EncodeScalarList(const Container& values)
    -> StatusOr<std::vector<std::byte>> {
  using T = typename Container::value_type;
  return EncodeScalarListSpan<T>(
      std::span<const T>(values.data(), values.size()));
}

template <SupportedFieldCppType T>
Status WriteScalarField(const FieldLayout& field, T value,
                        EncodedRow* encoded) {
  if (encoded == nullptr) {
    return Status::InvalidArgument("encoded row must not be null");
  }
  if (field.is_list || field.uses_ref || field.type != FieldTypeFor<T>()) {
    return Status::InvalidArgument("scalar field writer does not match field");
  }
  if (field.slot_offset > encoded->row_slot.size() ||
      encoded->row_slot.size() - field.slot_offset < field.value_width) {
    return Status::InvalidArgument("scalar field offset is out of row-slot bounds");
  }

  Status status = Status::Ok();
  if constexpr (std::same_as<std::remove_cv_t<T>, bool>) {
    status = WriteLittleEndian<std::uint8_t>(
        value ? 1 : 0, std::span<std::byte>(encoded->row_slot),
        field.slot_offset);
  } else {
    status = WriteLittleEndian<std::remove_cv_t<T>>(
        value, std::span<std::byte>(encoded->row_slot), field.slot_offset);
  }
  if (!status.ok()) {
    return status;
  }
  return SetFieldPresent(field, encoded);
}

template <SupportedFieldCppType T>
Status WriteArenaListField(const FieldLayout& field, std::span<const T> values,
                           EncodedRow* encoded) {
  if (encoded == nullptr) {
    return Status::InvalidArgument("encoded row must not be null");
  }
  if (!field.is_list || field.encoding != FieldEncoding::kArena ||
      field.type != FieldTypeFor<T>()) {
    return Status::InvalidArgument("arena list writer does not match field");
  }
  if constexpr (std::same_as<std::remove_cv_t<T>, std::string>) {
    return Status::InvalidArgument("arena list<string> builder is not implemented");
  } else {
    auto payload = EncodeScalarListSpan<T>(values);
    if (!payload.ok()) {
      return payload.status();
    }

    if (encoded->arena.size() >
        static_cast<std::size_t>(std::numeric_limits<std::uint64_t>::max())) {
      return Status::InvalidArgument("arena offset is too large");
    }
    const std::uint64_t offset = encoded->arena.size();
    const std::uint32_t byte_length =
        static_cast<std::uint32_t>(payload->size());
    const std::uint32_t element_count =
        static_cast<std::uint32_t>(values.size());
    encoded->arena.insert(encoded->arena.end(), payload->begin(), payload->end());
    return WriteValueRefField(field,
                              ValueRef16{
                                  .offset = offset,
                                  .byte_length = byte_length,
                                  .element_count_or_flags = element_count,
                              },
                              encoded);
  }
}

}  // namespace kv_index::internal

#endif  // KV_INDEX_SRC_CORE_ROW_STORAGE_H_
