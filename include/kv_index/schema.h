#ifndef KV_INDEX_SCHEMA_H_
#define KV_INDEX_SCHEMA_H_

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "kv_index/status.h"
#include "kv_index/types.h"

namespace kv_index {

enum class FieldType : std::uint8_t {
  kInt8 = 1,
  kInt32 = 2,
  kInt64 = 3,
  kUInt64 = 4,
  kBool = 5,
  kString = 6,
};

enum class FieldEncoding : std::uint8_t {
  kFixed = 1,
  kArena = 2,
  kDictionary = 3,
  kListDictionary = 4,
  kElementDictionary = 5,
};

using FieldDefaultValue =
    std::variant<std::monostate, std::int8_t, std::int32_t, std::int64_t,
                 std::uint64_t, bool, std::string, std::vector<std::int8_t>,
                 std::vector<std::int32_t>, std::vector<std::int64_t>,
                 std::vector<std::uint64_t>, std::vector<bool>,
                 std::vector<std::string>>;

struct FieldSpec {
  FieldId field_id = 0;
  std::string name;
  FieldType type = FieldType::kInt64;
  bool is_list = false;
  bool nullable = true;
  FieldEncoding encoding = FieldEncoding::kFixed;
  FieldDefaultValue default_value;
  bool deleted = false;

  bool HasDefault() const noexcept {
    return !std::holds_alternative<std::monostate>(default_value);
  }
};

struct FieldLayout {
  FieldId field_id = 0;
  std::string name;
  FieldType type = FieldType::kInt64;
  bool is_list = false;
  bool nullable = true;
  FieldEncoding encoding = FieldEncoding::kFixed;
  FieldDefaultValue default_value;
  std::size_t presence_bit_index = 0;
  std::size_t slot_offset = 0;
  std::size_t value_width = 0;
  bool uses_ref = false;

  bool HasDefault() const noexcept {
    return !std::holds_alternative<std::monostate>(default_value);
  }
};

class RuntimeSchema {
 public:
  explicit RuntimeSchema(std::uint64_t version = 0) : version_(version) {}

  std::uint64_t version() const noexcept { return version_; }
  const std::vector<FieldSpec>& active_fields() const noexcept {
    return active_fields_;
  }
  const std::vector<FieldSpec>& deleted_fields() const noexcept {
    return deleted_fields_;
  }

  Status AddField(FieldSpec field);
  Status DeleteField(FieldId field_id);

  const FieldSpec* FindField(FieldId field_id) const noexcept;
  const FieldSpec* FindDeletedField(FieldId field_id) const noexcept;

 private:
  std::uint64_t version_ = 0;
  std::vector<FieldSpec> active_fields_;
  std::vector<FieldSpec> deleted_fields_;
};

class CompiledRowLayout {
 public:
  static StatusOr<CompiledRowLayout> Compile(const RuntimeSchema& schema);

  std::uint64_t schema_version() const noexcept { return schema_version_; }
  std::uint64_t layout_fingerprint() const noexcept {
    return layout_fingerprint_;
  }
  std::size_t presence_bitmap_bytes() const noexcept {
    return presence_bitmap_bytes_;
  }
  std::size_t fixed_area_offset() const noexcept { return fixed_area_offset_; }
  std::size_t ref_area_offset() const noexcept { return ref_area_offset_; }
  std::size_t row_slot_size() const noexcept { return row_slot_size_; }
  const std::vector<FieldLayout>& fields() const noexcept { return fields_; }

  const FieldLayout* FindField(FieldId field_id) const noexcept;

 private:
  std::uint64_t schema_version_ = 0;
  std::uint64_t layout_fingerprint_ = 0;
  std::size_t presence_bitmap_bytes_ = 0;
  std::size_t fixed_area_offset_ = 0;
  std::size_t ref_area_offset_ = 0;
  std::size_t row_slot_size_ = 0;
  std::vector<FieldLayout> fields_;
};

std::size_t FieldTypeSize(FieldType type) noexcept;
std::size_t FieldTypeAlignment(FieldType type) noexcept;
std::string_view FieldEncodingConfigName(FieldEncoding encoding) noexcept;
StatusOr<FieldEncoding> ParseFieldEncoding(std::string_view name, bool is_list,
                                           FieldType type);

}  // namespace kv_index

#endif  // KV_INDEX_SCHEMA_H_
