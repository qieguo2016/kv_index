#include "src/model/row_storage.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace kv_index {
namespace {

bool HasBytes(std::size_t size, std::size_t offset, std::size_t byte_count) {
  return offset <= size && size - offset >= byte_count;
}

bool FieldPresent(const FieldLayout& field, const internal::model::EncodedRow& row) {
  const std::size_t byte_index = field.presence_bit_index / 8U;
  const std::size_t bit_index = field.presence_bit_index % 8U;
  if (byte_index >= row.row_slot.size()) {
    return false;
  }
  const auto byte =
      std::to_integer<unsigned char>(row.row_slot[byte_index]);
  return (byte & (1U << bit_index)) != 0;
}

Status ValidateAccessor(const Row::AccessorMetadata* accessor,
                        const FieldLayout& field,
                        const internal::model::EncodedRow& row,
                        const CompiledRowLayout& layout,
                        FieldType expected_type, bool expected_list) {
  if (accessor == nullptr) {
    return Status::Ok();
  }
  if (accessor->schema_version != row.schema_version ||
      accessor->schema_version != layout.schema_version() ||
      accessor->layout_fingerprint != row.layout_fingerprint ||
      accessor->layout_fingerprint != layout.layout_fingerprint() ||
      accessor->field_id != field.field_id ||
      accessor->is_list != expected_list ||
      accessor->physical_type != expected_type ||
      accessor->encoding != field.encoding ||
      accessor->offset != field.slot_offset ||
      accessor->presence_bit_index != field.presence_bit_index) {
    return Status::FailedPrecondition("field accessor does not match this row layout");
  }
  return Status::Ok();
}

StatusOr<const FieldLayout*> ResolveField(
    const std::shared_ptr<const CompiledRowLayout>& layout,
    const std::shared_ptr<const internal::model::EncodedRow>& row, FieldId field_id,
    FieldType expected_type, bool expected_list,
    const Row::AccessorMetadata* accessor) {
  if (layout == nullptr || row == nullptr) {
    return Status::FailedPrecondition("row has no materialized backing");
  }
  if (row->schema_version != layout->schema_version() ||
      row->layout_fingerprint != layout->layout_fingerprint()) {
    return Status::FailedPrecondition("row backing does not match compiled layout");
  }
  if (accessor != nullptr &&
      (accessor->schema_version != row->schema_version ||
       accessor->schema_version != layout->schema_version() ||
       accessor->layout_fingerprint != row->layout_fingerprint ||
       accessor->layout_fingerprint != layout->layout_fingerprint())) {
    return Status::FailedPrecondition(
        "field accessor does not match this row layout");
  }

  const FieldLayout* field = layout->FindField(field_id);
  if (field == nullptr) {
    if (accessor != nullptr) {
      return Status::FailedPrecondition(
          "field accessor does not match this row layout");
    }
    return Status::NotFound("field is not active in row layout");
  }
  if (field->is_list != expected_list) {
    return Status::InvalidArgument("field scalar/list shape does not match read");
  }
  if (field->type != expected_type) {
    return Status::InvalidArgument("field physical type does not match read");
  }
  if (const Status status =
          ValidateAccessor(accessor, *field, *row, *layout, expected_type,
                           expected_list);
      !status.ok()) {
    return status;
  }
  return field;
}

StatusOr<internal::base::ValueRef16> ReadRef(const FieldLayout& field,
                                       const internal::model::EncodedRow& row) {
  if (!HasBytes(row.row_slot.size(), field.slot_offset, 16)) {
    return Status::InvalidArgument("value ref is out of row-slot bounds");
  }
  return internal::base::DecodeValueRef16(std::span<const std::byte>(row.row_slot),
                                    field.slot_offset);
}

Status ValidateRefBounds(const internal::base::ValueRef16& ref,
                         std::size_t payload_size) {
  if (ref.offset > payload_size) {
    return Status::InvalidArgument("value ref offset is out of bounds");
  }
  const std::size_t offset = static_cast<std::size_t>(ref.offset);
  if (payload_size - offset < ref.byte_length) {
    return Status::InvalidArgument("value ref length is out of bounds");
  }
  return Status::Ok();
}

StatusOr<Row::ScalarValue> DefaultScalarValue(const FieldLayout& field) {
  if (!field.HasDefault()) {
    if (!field.nullable) {
      return Status::FailedPrecondition("required field is missing");
    }
    return Status::NotFound("field is absent and has no default");
  }

  switch (field.type) {
    case FieldType::kInt8:
      return std::get<std::int8_t>(field.default_value);
    case FieldType::kInt32:
      return std::get<std::int32_t>(field.default_value);
    case FieldType::kInt64:
      return std::get<std::int64_t>(field.default_value);
    case FieldType::kUInt64:
      return std::get<std::uint64_t>(field.default_value);
    case FieldType::kBool:
      return std::get<bool>(field.default_value);
    case FieldType::kString:
      return std::get<std::string>(field.default_value);
  }
  return Status::InvalidArgument("unsupported scalar default type");
}

StatusOr<Row::ListValue> DefaultListValue(const FieldLayout& field) {
  if (!field.HasDefault()) {
    if (!field.nullable) {
      return Status::FailedPrecondition("required list field is missing");
    }
    return Status::NotFound("list field is absent and has no default");
  }

  switch (field.type) {
    case FieldType::kInt8:
      return std::get<std::vector<std::int8_t>>(field.default_value);
    case FieldType::kInt32:
      return std::get<std::vector<std::int32_t>>(field.default_value);
    case FieldType::kInt64:
      return std::get<std::vector<std::int64_t>>(field.default_value);
    case FieldType::kUInt64:
      return std::get<std::vector<std::uint64_t>>(field.default_value);
    case FieldType::kBool:
      return std::get<std::vector<bool>>(field.default_value);
    case FieldType::kString:
      return std::get<std::vector<std::string>>(field.default_value);
  }
  return Status::InvalidArgument("unsupported list default type");
}

template <typename T>
StatusOr<std::vector<T>> DecodeScalarListBytes(
    std::span<const std::byte> bytes, std::uint32_t element_count) {
  const std::size_t width = FieldTypeSize(FieldTypeFor<T>());
  if (bytes.size() != static_cast<std::size_t>(element_count) * width) {
    return Status::InvalidArgument("list byte length does not match element count");
  }

  std::vector<T> values;
  values.reserve(element_count);
  for (std::uint32_t i = 0; i < element_count; ++i) {
    if constexpr (std::same_as<T, bool>) {
      auto value =
          internal::base::ReadLittleEndian<std::uint8_t>(bytes, i * width);
      if (!value.ok()) {
        return value.status();
      }
      if (value.value() > 1) {
        return Status::InvalidArgument("encoded bool list value is invalid");
      }
      values.push_back(value.value() != 0);
    } else {
      auto value = internal::base::ReadLittleEndian<T>(bytes, i * width);
      if (!value.ok()) {
        return value.status();
      }
      values.push_back(value.value());
    }
  }
  return values;
}

StatusOr<Row::ListValue> DecodeScalarListByType(
    FieldType type, std::span<const std::byte> bytes,
    std::uint32_t element_count) {
  switch (type) {
    case FieldType::kInt8: {
      auto values = DecodeScalarListBytes<std::int8_t>(bytes, element_count);
      if (!values.ok()) {
        return values.status();
      }
      return values.value();
    }
    case FieldType::kInt32: {
      auto values = DecodeScalarListBytes<std::int32_t>(bytes, element_count);
      if (!values.ok()) {
        return values.status();
      }
      return values.value();
    }
    case FieldType::kInt64: {
      auto values = DecodeScalarListBytes<std::int64_t>(bytes, element_count);
      if (!values.ok()) {
        return values.status();
      }
      return values.value();
    }
    case FieldType::kUInt64: {
      auto values = DecodeScalarListBytes<std::uint64_t>(bytes, element_count);
      if (!values.ok()) {
        return values.status();
      }
      return values.value();
    }
    case FieldType::kBool: {
      auto values = DecodeScalarListBytes<bool>(bytes, element_count);
      if (!values.ok()) {
        return values.status();
      }
      return values.value();
    }
    case FieldType::kString:
      return Status::InvalidArgument("string list is not a scalar byte list");
  }
  return Status::InvalidArgument("unsupported scalar list type");
}

StatusOr<Row::ListValue> DecodeArenaStringList(
    const internal::base::ValueRef16& ref, const internal::model::EncodedRow& row) {
  const std::size_t ref_bytes =
      static_cast<std::size_t>(ref.element_count_or_flags) * 16U;
  if (ref.byte_length != ref_bytes) {
    return Status::InvalidArgument("string list ref payload has invalid length");
  }
  if (const Status status = ValidateRefBounds(ref, row.arena.size());
      !status.ok()) {
    return status;
  }

  std::vector<std::string> values;
  values.reserve(ref.element_count_or_flags);
  for (std::uint32_t i = 0; i < ref.element_count_or_flags; ++i) {
    auto element_ref = internal::base::DecodeValueRef16(
        std::span<const std::byte>(row.arena),
        static_cast<std::size_t>(ref.offset) + i * 16U);
    if (!element_ref.ok()) {
      return element_ref.status();
    }
    if (const Status status = ValidateRefBounds(element_ref.value(), row.arena.size());
        !status.ok()) {
      return status;
    }
    values.emplace_back(
        reinterpret_cast<const char*>(row.arena.data()) + element_ref->offset,
        element_ref->byte_length);
  }
  return values;
}

}  // namespace

namespace internal::model {

EncodedRow CreateEncodedRow(const CompiledRowLayout& layout) {
  return EncodedRow{
      .schema_version = layout.schema_version(),
      .layout_fingerprint = layout.layout_fingerprint(),
      .row_slot = std::vector<std::byte>(layout.row_slot_size()),
  };
}

Status SetFieldPresent(const FieldLayout& field, EncodedRow* encoded) {
  if (encoded == nullptr) {
    return Status::InvalidArgument("encoded row must not be null");
  }
  const std::size_t byte_index = field.presence_bit_index / 8U;
  const std::size_t bit_index = field.presence_bit_index % 8U;
  if (byte_index >= encoded->row_slot.size()) {
    return Status::InvalidArgument("presence bit is out of row-slot bounds");
  }
  auto byte = std::to_integer<unsigned char>(encoded->row_slot[byte_index]);
  byte |= static_cast<unsigned char>(1U << bit_index);
  encoded->row_slot[byte_index] = static_cast<std::byte>(byte);
  return Status::Ok();
}

Status WriteValueRefField(const FieldLayout& field, const base::ValueRef16& ref,
                          EncodedRow* encoded) {
  if (encoded == nullptr) {
    return Status::InvalidArgument("encoded row must not be null");
  }
  if (!field.uses_ref) {
    return Status::InvalidArgument("field does not use a value ref");
  }
  if (const Status status =
          WriteValueRef16(ref, std::span<std::byte>(encoded->row_slot),
                          field.slot_offset);
      !status.ok()) {
    return status;
  }
  return SetFieldPresent(field, encoded);
}

Status WriteArenaStringField(const FieldLayout& field, std::string_view value,
                             EncodedRow* encoded) {
  if (encoded == nullptr) {
    return Status::InvalidArgument("encoded row must not be null");
  }
  if (field.is_list || field.type != FieldType::kString ||
      field.encoding != FieldEncoding::kArena) {
    return Status::InvalidArgument("arena string writer does not match field");
  }
  if (value.size() > std::numeric_limits<std::uint32_t>::max()) {
    return Status::InvalidArgument("string value is too large");
  }
  const std::uint64_t offset = encoded->arena.size();
  encoded->arena.insert(encoded->arena.end(),
                        reinterpret_cast<const std::byte*>(value.data()),
                        reinterpret_cast<const std::byte*>(value.data()) +
                            value.size());
  return WriteValueRefField(field,
                            base::ValueRef16{
                                .offset = offset,
                                .byte_length =
                                    static_cast<std::uint32_t>(value.size()),
                                .element_count_or_flags = 0,
                            },
                            encoded);
}

Status WriteDictionaryStringField(const FieldLayout& field,
                                  std::uint32_t dictionary_id,
                                  EncodedRow* encoded) {
  if (field.is_list || field.type != FieldType::kString ||
      field.encoding != FieldEncoding::kDictionary) {
    return Status::InvalidArgument("dictionary string writer does not match field");
  }
  return WriteValueRefField(field,
                            base::ValueRef16{
                                .offset = dictionary_id,
                                .byte_length = 0,
                                .element_count_or_flags = 0,
                            },
                            encoded);
}

Status WriteListDictionaryField(const FieldLayout& field,
                                std::uint32_t dictionary_id,
                                std::uint32_t element_count,
                                EncodedRow* encoded) {
  if (!field.is_list || field.encoding != FieldEncoding::kListDictionary) {
    return Status::InvalidArgument("list dictionary writer does not match field");
  }
  return WriteValueRefField(field,
                            base::ValueRef16{
                                .offset = dictionary_id,
                                .byte_length = 0,
                                .element_count_or_flags = element_count,
                            },
                            encoded);
}

Status WriteElementDictionaryStringListField(
    const FieldLayout& field, std::span<const std::uint32_t> dictionary_ids,
    EncodedRow* encoded) {
  if (encoded == nullptr) {
    return Status::InvalidArgument("encoded row must not be null");
  }
  if (!field.is_list || field.type != FieldType::kString ||
      field.encoding != FieldEncoding::kElementDictionary) {
    return Status::InvalidArgument(
        "element dictionary string-list writer does not match field");
  }
  if (dictionary_ids.size() > std::numeric_limits<std::uint32_t>::max()) {
    return Status::InvalidArgument("string-list dictionary id count is too large");
  }

  const std::uint64_t offset = encoded->arena.size();
  const std::size_t byte_length = dictionary_ids.size() * sizeof(std::uint32_t);
  encoded->arena.resize(encoded->arena.size() + byte_length);
  for (std::size_t i = 0; i < dictionary_ids.size(); ++i) {
    if (const Status status = base::WriteLittleEndian<std::uint32_t>(
            dictionary_ids[i], std::span<std::byte>(encoded->arena),
            static_cast<std::size_t>(offset) + i * sizeof(std::uint32_t));
        !status.ok()) {
      return status;
    }
  }
  return WriteValueRefField(field,
                            base::ValueRef16{
                                .offset = offset,
                                .byte_length =
                                    static_cast<std::uint32_t>(byte_length),
                                .element_count_or_flags =
                                    static_cast<std::uint32_t>(
                                        dictionary_ids.size()),
                            },
                            encoded);
}

StatusOr<Row> MaterializeRow(std::shared_ptr<const CompiledRowLayout> layout,
                             EncodedRow encoded) {
  if (layout == nullptr) {
    return Status::InvalidArgument("compiled row layout must not be null");
  }
  if (encoded.schema_version != layout->schema_version() ||
      encoded.layout_fingerprint != layout->layout_fingerprint()) {
    return Status::FailedPrecondition("encoded row metadata does not match layout");
  }
  if (encoded.row_slot.size() < layout->row_slot_size()) {
    return Status::InvalidArgument("encoded row slot is shorter than layout");
  }

  auto owned = std::make_shared<EncodedRow>(std::move(encoded));
  for (const FieldLayout& field : layout->fields()) {
    if (!field.nullable && !field.HasDefault() && !FieldPresent(field, *owned)) {
      return Status::FailedPrecondition("required field is absent in encoded row");
    }
  }
  return Row(std::move(layout), std::move(owned));
}

}  // namespace internal::model

Row::Row(std::shared_ptr<const CompiledRowLayout> layout,
         std::shared_ptr<const internal::model::EncodedRow> encoded)
    : layout_(std::move(layout)), encoded_(std::move(encoded)) {}

bool Row::Has(FieldId field_id) const noexcept {
  if (layout_ == nullptr || encoded_ == nullptr) {
    return false;
  }
  const FieldLayout* field = layout_->FindField(field_id);
  if (field == nullptr) {
    return false;
  }
  return FieldPresent(*field, *encoded_);
}

StatusOr<Row::ScalarValue> Row::ReadScalarValue(
    FieldId field_id, FieldType expected_type,
    const AccessorMetadata* accessor) const {
  auto field_or = ResolveField(layout_, encoded_, field_id, expected_type,
                               false, accessor);
  if (!field_or.ok()) {
    return field_or.status();
  }
  const FieldLayout& field = **field_or;
  if (!FieldPresent(field, *encoded_)) {
    return DefaultScalarValue(field);
  }

  if (field.type == FieldType::kString) {
    auto ref = ReadRef(field, *encoded_);
    if (!ref.ok()) {
      return ref.status();
    }
    if (field.encoding == FieldEncoding::kArena) {
      if (const Status status = ValidateRefBounds(ref.value(), encoded_->arena.size());
          !status.ok()) {
        return status;
      }
      return std::string(
          reinterpret_cast<const char*>(encoded_->arena.data()) + ref->offset,
          ref->byte_length);
    }
    if (field.encoding == FieldEncoding::kDictionary) {
      if (ref->offset >= encoded_->string_dictionary.size()) {
        return Status::InvalidArgument("string dictionary id is out of bounds");
      }
      return encoded_->string_dictionary[static_cast<std::size_t>(ref->offset)];
    }
    return Status::InvalidArgument("string field has unsupported encoding");
  }

  if (!HasBytes(encoded_->row_slot.size(), field.slot_offset,
                field.value_width)) {
    return Status::InvalidArgument("scalar field is out of row-slot bounds");
  }

  switch (field.type) {
    case FieldType::kInt8: {
      auto value = internal::base::ReadLittleEndian<std::int8_t>(
          std::span<const std::byte>(encoded_->row_slot), field.slot_offset);
      if (!value.ok()) {
        return value.status();
      }
      return value.value();
    }
    case FieldType::kInt32: {
      auto value = internal::base::ReadLittleEndian<std::int32_t>(
          std::span<const std::byte>(encoded_->row_slot), field.slot_offset);
      if (!value.ok()) {
        return value.status();
      }
      return value.value();
    }
    case FieldType::kInt64: {
      auto value = internal::base::ReadLittleEndian<std::int64_t>(
          std::span<const std::byte>(encoded_->row_slot), field.slot_offset);
      if (!value.ok()) {
        return value.status();
      }
      return value.value();
    }
    case FieldType::kUInt64: {
      auto value = internal::base::ReadLittleEndian<std::uint64_t>(
          std::span<const std::byte>(encoded_->row_slot), field.slot_offset);
      if (!value.ok()) {
        return value.status();
      }
      return value.value();
    }
    case FieldType::kBool: {
      auto value = internal::base::ReadLittleEndian<std::uint8_t>(
          std::span<const std::byte>(encoded_->row_slot), field.slot_offset);
      if (!value.ok()) {
        return value.status();
      }
      if (value.value() > 1) {
        return Status::InvalidArgument("encoded bool value is invalid");
      }
      return value.value() != 0;
    }
    case FieldType::kString:
      break;
  }
  return Status::InvalidArgument("unsupported scalar field type");
}

StatusOr<Row::ListValue> Row::ReadListValue(
    FieldId field_id, FieldType expected_type,
    const AccessorMetadata* accessor) const {
  auto field_or =
      ResolveField(layout_, encoded_, field_id, expected_type, true, accessor);
  if (!field_or.ok()) {
    return field_or.status();
  }
  const FieldLayout& field = **field_or;
  if (!FieldPresent(field, *encoded_)) {
    return DefaultListValue(field);
  }

  auto ref = ReadRef(field, *encoded_);
  if (!ref.ok()) {
    return ref.status();
  }

  if (field.encoding == FieldEncoding::kArena) {
    if (const Status status = ValidateRefBounds(ref.value(), encoded_->arena.size());
        !status.ok()) {
      return status;
    }
    const auto payload = std::span<const std::byte>(encoded_->arena).subspan(
        static_cast<std::size_t>(ref->offset), ref->byte_length);
    if (field.type == FieldType::kString) {
      return DecodeArenaStringList(ref.value(), *encoded_);
    }
    return DecodeScalarListByType(field.type, payload,
                                  ref->element_count_or_flags);
  }

  if (field.encoding == FieldEncoding::kListDictionary) {
    if (field.type == FieldType::kString) {
      if (ref->offset >= encoded_->string_list_dictionary.size()) {
        return Status::InvalidArgument("string-list dictionary id is out of bounds");
      }
      const auto& values =
          encoded_->string_list_dictionary[static_cast<std::size_t>(ref->offset)];
      if (values.size() != ref->element_count_or_flags) {
        return Status::InvalidArgument(
            "string-list dictionary count does not match ref");
      }
      return values;
    }
    if (ref->offset >= encoded_->scalar_list_dictionary.size()) {
      return Status::InvalidArgument("scalar-list dictionary id is out of bounds");
    }
    const auto& payload =
        encoded_->scalar_list_dictionary[static_cast<std::size_t>(ref->offset)];
    return DecodeScalarListByType(field.type,
                                  std::span<const std::byte>(payload),
                                  ref->element_count_or_flags);
  }

  if (field.encoding == FieldEncoding::kElementDictionary) {
    if (field.type != FieldType::kString) {
      return Status::InvalidArgument(
          "element dictionary lists are only supported for strings");
    }
    if (const Status status = ValidateRefBounds(ref.value(), encoded_->arena.size());
        !status.ok()) {
      return status;
    }
    if (ref->byte_length !=
        static_cast<std::uint32_t>(ref->element_count_or_flags *
                                   sizeof(std::uint32_t))) {
      return Status::InvalidArgument(
          "element dictionary ref length does not match element count");
    }
    std::vector<std::string> values;
    values.reserve(ref->element_count_or_flags);
    for (std::uint32_t i = 0; i < ref->element_count_or_flags; ++i) {
      auto dictionary_id = internal::base::ReadLittleEndian<std::uint32_t>(
          std::span<const std::byte>(encoded_->arena),
          static_cast<std::size_t>(ref->offset) + i * sizeof(std::uint32_t));
      if (!dictionary_id.ok()) {
        return dictionary_id.status();
      }
      if (dictionary_id.value() >= encoded_->string_element_dictionary.size()) {
        return Status::InvalidArgument(
            "element dictionary string id is out of bounds");
      }
      values.push_back(encoded_->string_element_dictionary[dictionary_id.value()]);
    }
    return values;
  }

  return Status::InvalidArgument("list field has unsupported encoding");
}

}  // namespace kv_index
