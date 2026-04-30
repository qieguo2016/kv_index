#include "kv_index/schema.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <utility>

#include "src/core/byte_io.h"

namespace kv_index {
namespace {

constexpr std::size_t kValueRef16Size = 16;
constexpr std::uint64_t kFnvOffsetBasis = 14695981039346656037ULL;
constexpr std::uint64_t kFnvPrime = 1099511628211ULL;

bool IsRefField(const FieldSpec& field) {
  return field.is_list || field.type == FieldType::kString;
}

void FingerprintByte(std::uint64_t* hash, std::uint8_t byte) {
  *hash ^= byte;
  *hash *= kFnvPrime;
}

void FingerprintUInt64(std::uint64_t* hash, std::uint64_t value) {
  for (std::size_t i = 0; i < sizeof(value); ++i) {
    FingerprintByte(hash, static_cast<std::uint8_t>((value >> (i * 8U)) & 0xffU));
  }
}

void FingerprintString(std::uint64_t* hash, std::string_view value) {
  FingerprintUInt64(hash, value.size());
  for (const char c : value) {
    FingerprintByte(hash, static_cast<std::uint8_t>(c));
  }
}

template <typename T>
void FingerprintVector(std::uint64_t* hash, const std::vector<T>& values) {
  FingerprintUInt64(hash, values.size());
  for (const auto& value : values) {
    if constexpr (std::is_same_v<T, std::string>) {
      FingerprintString(hash, value);
    } else if constexpr (std::is_same_v<T, bool>) {
      FingerprintByte(hash, value ? 1 : 0);
    } else {
      FingerprintUInt64(hash, static_cast<std::uint64_t>(value));
    }
  }
}

void FingerprintDefault(std::uint64_t* hash, const FieldDefaultValue& value) {
  FingerprintUInt64(hash, value.index());
  std::visit(
      [&](const auto& typed_value) {
        using T = std::decay_t<decltype(typed_value)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
          return;
        } else if constexpr (std::is_same_v<T, std::string>) {
          FingerprintString(hash, typed_value);
        } else if constexpr (std::is_same_v<T, bool>) {
          FingerprintByte(hash, typed_value ? 1 : 0);
        } else if constexpr (requires { typed_value.size(); }) {
          FingerprintVector(hash, typed_value);
        } else {
          FingerprintUInt64(hash, static_cast<std::uint64_t>(typed_value));
        }
      },
      value);
}

bool DefaultMatchesField(const FieldSpec& field) {
  if (!field.HasDefault()) {
    return true;
  }

  if (field.is_list) {
    switch (field.type) {
      case FieldType::kInt8:
        return std::holds_alternative<std::vector<std::int8_t>>(
            field.default_value);
      case FieldType::kInt32:
        return std::holds_alternative<std::vector<std::int32_t>>(
            field.default_value);
      case FieldType::kInt64:
        return std::holds_alternative<std::vector<std::int64_t>>(
            field.default_value);
      case FieldType::kUInt64:
        return std::holds_alternative<std::vector<std::uint64_t>>(
            field.default_value);
      case FieldType::kBool:
        return std::holds_alternative<std::vector<bool>>(field.default_value);
      case FieldType::kString:
        return std::holds_alternative<std::vector<std::string>>(
            field.default_value);
    }
  }

  switch (field.type) {
    case FieldType::kInt8:
      return std::holds_alternative<std::int8_t>(field.default_value);
    case FieldType::kInt32:
      return std::holds_alternative<std::int32_t>(field.default_value);
    case FieldType::kInt64:
      return std::holds_alternative<std::int64_t>(field.default_value);
    case FieldType::kUInt64:
      return std::holds_alternative<std::uint64_t>(field.default_value);
    case FieldType::kBool:
      return std::holds_alternative<bool>(field.default_value);
    case FieldType::kString:
      return std::holds_alternative<std::string>(field.default_value);
  }
  return false;
}

Status ValidateEncodingPolicy(const FieldSpec& field) {
  if (field.name.empty()) {
    return Status::InvalidArgument("field name must not be empty");
  }
  if (field.deleted) {
    return Status::InvalidArgument("new fields must not be marked deleted");
  }
  if (!DefaultMatchesField(field)) {
    return Status::InvalidArgument("field default does not match field type");
  }

  if (field.is_list) {
    switch (field.encoding) {
      case FieldEncoding::kArena:
      case FieldEncoding::kListDictionary:
        return Status::Ok();
      case FieldEncoding::kElementDictionary:
        if (field.type == FieldType::kString) {
          return Status::Ok();
        }
        return Status::InvalidArgument(
            "element dictionary lists are only supported for strings");
      case FieldEncoding::kFixed:
      case FieldEncoding::kDictionary:
        return Status::InvalidArgument("list field has invalid encoding");
    }
  }

  if (field.type == FieldType::kString) {
    switch (field.encoding) {
      case FieldEncoding::kArena:
      case FieldEncoding::kDictionary:
        return Status::Ok();
      case FieldEncoding::kFixed:
      case FieldEncoding::kListDictionary:
      case FieldEncoding::kElementDictionary:
        return Status::InvalidArgument("string field has invalid encoding");
    }
  }

  if (field.encoding != FieldEncoding::kFixed) {
    return Status::InvalidArgument("fixed scalar field has invalid encoding");
  }
  return Status::Ok();
}

std::uint64_t BuildFingerprint(const CompiledRowLayout& layout) {
  std::uint64_t hash = kFnvOffsetBasis;
  FingerprintString(&hash, "kv_index_row_layout_v1");
  FingerprintUInt64(&hash, layout.schema_version());
  FingerprintUInt64(&hash, layout.presence_bitmap_bytes());
  FingerprintUInt64(&hash, layout.fixed_area_offset());
  FingerprintUInt64(&hash, layout.ref_area_offset());
  FingerprintUInt64(&hash, layout.row_slot_size());
  FingerprintUInt64(&hash, layout.fields().size());
  for (const FieldLayout& field : layout.fields()) {
    FingerprintUInt64(&hash, field.field_id);
    FingerprintString(&hash, field.name);
    FingerprintUInt64(&hash, static_cast<std::uint8_t>(field.type));
    FingerprintByte(&hash, field.is_list ? 1 : 0);
    FingerprintByte(&hash, field.nullable ? 1 : 0);
    FingerprintUInt64(&hash, static_cast<std::uint8_t>(field.encoding));
    FingerprintDefault(&hash, field.default_value);
    FingerprintUInt64(&hash, field.presence_bit_index);
    FingerprintUInt64(&hash, field.slot_offset);
    FingerprintUInt64(&hash, field.value_width);
    FingerprintByte(&hash, field.uses_ref ? 1 : 0);
  }
  return hash;
}

}  // namespace

std::size_t FieldTypeSize(FieldType type) noexcept {
  switch (type) {
    case FieldType::kInt8:
    case FieldType::kBool:
      return 1;
    case FieldType::kInt32:
      return 4;
    case FieldType::kInt64:
    case FieldType::kUInt64:
      return 8;
    case FieldType::kString:
      return kValueRef16Size;
  }
  return 0;
}

std::size_t FieldTypeAlignment(FieldType type) noexcept {
  switch (type) {
    case FieldType::kInt8:
    case FieldType::kBool:
      return 1;
    case FieldType::kInt32:
      return 4;
    case FieldType::kInt64:
    case FieldType::kUInt64:
    case FieldType::kString:
      return 8;
  }
  return 1;
}

Status RuntimeSchema::AddField(FieldSpec field) {
  if (FindField(field.field_id) != nullptr ||
      FindDeletedField(field.field_id) != nullptr) {
    return Status::FailedPrecondition("field id already exists in schema history");
  }
  if (const Status status = ValidateEncodingPolicy(field); !status.ok()) {
    return status;
  }
  active_fields_.push_back(std::move(field));
  return Status::Ok();
}

Status RuntimeSchema::DeleteField(FieldId field_id) {
  const auto it = std::find_if(
      active_fields_.begin(), active_fields_.end(),
      [field_id](const FieldSpec& field) { return field.field_id == field_id; });
  if (it == active_fields_.end()) {
    return Status::NotFound("field id is not active");
  }

  FieldSpec deleted = std::move(*it);
  deleted.deleted = true;
  deleted_fields_.push_back(std::move(deleted));
  active_fields_.erase(it);
  return Status::Ok();
}

const FieldSpec* RuntimeSchema::FindField(FieldId field_id) const noexcept {
  const auto it = std::find_if(
      active_fields_.begin(), active_fields_.end(),
      [field_id](const FieldSpec& field) { return field.field_id == field_id; });
  if (it == active_fields_.end()) {
    return nullptr;
  }
  return &*it;
}

const FieldSpec* RuntimeSchema::FindDeletedField(
    FieldId field_id) const noexcept {
  const auto it =
      std::find_if(deleted_fields_.begin(), deleted_fields_.end(),
                   [field_id](const FieldSpec& field) {
                     return field.field_id == field_id;
                   });
  if (it == deleted_fields_.end()) {
    return nullptr;
  }
  return &*it;
}

StatusOr<CompiledRowLayout> CompiledRowLayout::Compile(
    const RuntimeSchema& schema) {
  CompiledRowLayout layout;
  layout.schema_version_ = schema.version();

  std::vector<FieldSpec> active_fields = schema.active_fields();
  std::sort(active_fields.begin(), active_fields.end(),
            [](const FieldSpec& lhs, const FieldSpec& rhs) {
              return lhs.field_id < rhs.field_id;
            });

  layout.presence_bitmap_bytes_ =
      internal::AlignUp((active_fields.size() + 7U) / 8U, 8U);
  layout.fixed_area_offset_ = layout.presence_bitmap_bytes_;

  layout.fields_.reserve(active_fields.size());
  for (std::size_t i = 0; i < active_fields.size(); ++i) {
    const FieldSpec& field = active_fields[i];
    layout.fields_.push_back(FieldLayout{
        .field_id = field.field_id,
        .name = field.name,
        .type = field.type,
        .is_list = field.is_list,
        .nullable = field.nullable,
        .encoding = field.encoding,
        .default_value = field.default_value,
        .presence_bit_index = i,
        .slot_offset = 0,
        .value_width = IsRefField(field) ? kValueRef16Size : FieldTypeSize(field.type),
        .uses_ref = IsRefField(field),
    });
  }

  std::size_t fixed_cursor = layout.fixed_area_offset_;
  for (FieldLayout& field : layout.fields_) {
    if (field.uses_ref) {
      continue;
    }
    fixed_cursor = internal::AlignUp(fixed_cursor, FieldTypeAlignment(field.type));
    field.slot_offset = fixed_cursor;
    fixed_cursor += field.value_width;
  }

  layout.ref_area_offset_ = internal::AlignUp(fixed_cursor, 8U);
  std::size_t ref_cursor = layout.ref_area_offset_;
  for (FieldLayout& field : layout.fields_) {
    if (!field.uses_ref) {
      continue;
    }
    field.slot_offset = ref_cursor;
    ref_cursor += kValueRef16Size;
  }

  layout.row_slot_size_ = internal::AlignUp(ref_cursor, 8U);
  layout.layout_fingerprint_ = BuildFingerprint(layout);
  return layout;
}

const FieldLayout* CompiledRowLayout::FindField(
    FieldId field_id) const noexcept {
  const auto it =
      std::find_if(fields_.begin(), fields_.end(),
                   [field_id](const FieldLayout& field) {
                     return field.field_id == field_id;
                   });
  if (it == fields_.end()) {
    return nullptr;
  }
  return &*it;
}

}  // namespace kv_index
