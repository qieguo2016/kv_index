#ifndef KV_INDEX_ROW_H_
#define KV_INDEX_ROW_H_

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "kv_index/schema.h"
#include "kv_index/status.h"
#include "kv_index/types.h"

namespace kv_index {

namespace internal::model {
struct EncodedRow;
}  // namespace internal::model

template <typename T>
struct FieldTypeTraits;

template <>
struct FieldTypeTraits<std::int8_t> {
  static constexpr FieldType kType = FieldType::kInt8;
};

template <>
struct FieldTypeTraits<std::int32_t> {
  static constexpr FieldType kType = FieldType::kInt32;
};

template <>
struct FieldTypeTraits<std::int64_t> {
  static constexpr FieldType kType = FieldType::kInt64;
};

template <>
struct FieldTypeTraits<std::uint64_t> {
  static constexpr FieldType kType = FieldType::kUInt64;
};

template <>
struct FieldTypeTraits<bool> {
  static constexpr FieldType kType = FieldType::kBool;
};

template <>
struct FieldTypeTraits<std::string> {
  static constexpr FieldType kType = FieldType::kString;
};

template <typename T>
concept SupportedFieldCppType =
    requires { FieldTypeTraits<std::remove_cv_t<T>>::kType; };

template <SupportedFieldCppType T>
constexpr FieldType FieldTypeFor() {
  return FieldTypeTraits<std::remove_cv_t<T>>::kType;
}

template <typename T>
class ListView {
 public:
  using value_type = T;
  using Storage = std::vector<T>;
  using const_iterator = typename Storage::const_iterator;

  ListView() : values_(EmptyStorage()) {}
  explicit ListView(Storage values)
      : values_(std::make_shared<const Storage>(std::move(values))) {}

  const T* data() const noexcept requires(!std::same_as<T, bool>) {
    return values_->data();
  }
  std::size_t size() const noexcept { return values_->size(); }
  bool empty() const noexcept { return values_->empty(); }

  decltype(auto) operator[](std::size_t index) const {
    return (*values_)[index];
  }
  const_iterator begin() const noexcept { return values_->begin(); }
  const_iterator end() const noexcept { return values_->end(); }

 private:
  static std::shared_ptr<const Storage> EmptyStorage() {
    static const auto* empty = new std::shared_ptr<const Storage>(
        std::make_shared<const Storage>());
    return *empty;
  }

  std::shared_ptr<const Storage> values_;
};

template <SupportedFieldCppType T>
struct FieldAccessor {
  std::uint64_t schema_version = 0;
  std::uint64_t layout_fingerprint = 0;
  FieldId field_id = 0;
  bool is_list = false;
  FieldType physical_type = FieldTypeFor<T>();
  bool nullable = true;
  bool has_default = false;
  FieldEncoding encoding = FieldEncoding::kFixed;
  std::size_t offset = 0;
  std::size_t presence_bit_index = 0;
};

template <SupportedFieldCppType T>
StatusOr<FieldAccessor<T>> MakeFieldAccessor(const CompiledRowLayout& layout,
                                             FieldId field_id) {
  const FieldLayout* field = layout.FindField(field_id);
  if (field == nullptr) {
    return Status::NotFound("field is not active in layout");
  }
  if (field->is_list) {
    return Status::InvalidArgument("field is a list, not a scalar");
  }
  if (field->type != FieldTypeFor<T>()) {
    return Status::InvalidArgument("field scalar type does not match accessor");
  }
  return FieldAccessor<T>{
      .schema_version = layout.schema_version(),
      .layout_fingerprint = layout.layout_fingerprint(),
      .field_id = field->field_id,
      .is_list = false,
      .physical_type = field->type,
      .nullable = field->nullable,
      .has_default = field->HasDefault(),
      .encoding = field->encoding,
      .offset = field->slot_offset,
      .presence_bit_index = field->presence_bit_index,
  };
}

template <SupportedFieldCppType T>
StatusOr<FieldAccessor<T>> MakeListFieldAccessor(
    const CompiledRowLayout& layout, FieldId field_id) {
  const FieldLayout* field = layout.FindField(field_id);
  if (field == nullptr) {
    return Status::NotFound("field is not active in layout");
  }
  if (!field->is_list) {
    return Status::InvalidArgument("field is a scalar, not a list");
  }
  if (field->type != FieldTypeFor<T>()) {
    return Status::InvalidArgument("field list element type does not match accessor");
  }
  return FieldAccessor<T>{
      .schema_version = layout.schema_version(),
      .layout_fingerprint = layout.layout_fingerprint(),
      .field_id = field->field_id,
      .is_list = true,
      .physical_type = field->type,
      .nullable = field->nullable,
      .has_default = field->HasDefault(),
      .encoding = field->encoding,
      .offset = field->slot_offset,
      .presence_bit_index = field->presence_bit_index,
  };
}

class Row {
 public:
  using ScalarValue =
      std::variant<std::int8_t, std::int32_t, std::int64_t, std::uint64_t,
                   bool, std::string>;
  using ListValue =
      std::variant<std::vector<std::int8_t>, std::vector<std::int32_t>,
                   std::vector<std::int64_t>, std::vector<std::uint64_t>,
                   std::vector<bool>, std::vector<std::string>>;

  Row() = default;
  Row(std::shared_ptr<const CompiledRowLayout> layout,
      std::shared_ptr<const internal::model::EncodedRow> encoded);

  bool Has(FieldId field_id) const noexcept;

  template <SupportedFieldCppType T>
  std::optional<T> Get(FieldId field_id) const {
    auto value = ReadScalarValue(field_id, FieldTypeFor<T>(), nullptr);
    if (!value.ok()) {
      if (value.status().code() == StatusCode::kNotFound) {
        return std::nullopt;
      }
      ThrowStatus(value.status());
    }

    const auto* typed = std::get_if<std::remove_cv_t<T>>(&value.value());
    if (typed == nullptr) {
      throw std::runtime_error("row decoder returned an unexpected scalar type");
    }
    return *typed;
  }

  template <SupportedFieldCppType T>
  std::optional<T> Get(FieldAccessor<T> accessor) const {
    const AccessorMetadata metadata{
        .schema_version = accessor.schema_version,
        .layout_fingerprint = accessor.layout_fingerprint,
        .field_id = accessor.field_id,
        .is_list = accessor.is_list,
        .physical_type = accessor.physical_type,
        .encoding = accessor.encoding,
        .offset = accessor.offset,
        .presence_bit_index = accessor.presence_bit_index,
    };
    auto value = ReadScalarValue(accessor.field_id, FieldTypeFor<T>(),
                                 &metadata);
    if (!value.ok()) {
      if (value.status().code() == StatusCode::kNotFound) {
        return std::nullopt;
      }
      ThrowStatus(value.status());
    }

    const auto* typed = std::get_if<std::remove_cv_t<T>>(&value.value());
    if (typed == nullptr) {
      throw std::runtime_error("row decoder returned an unexpected scalar type");
    }
    return *typed;
  }

  template <SupportedFieldCppType T>
  ListView<T> GetList(FieldId field_id) const {
    auto value = ReadListValue(field_id, FieldTypeFor<T>(), nullptr);
    if (!value.ok()) {
      if (value.status().code() == StatusCode::kNotFound) {
        return ListView<T>();
      }
      ThrowStatus(value.status());
    }

    const auto* typed =
        std::get_if<std::vector<std::remove_cv_t<T>>>(&value.value());
    if (typed == nullptr) {
      throw std::runtime_error("row decoder returned an unexpected list type");
    }
    return ListView<T>(*typed);
  }

  template <SupportedFieldCppType T>
  ListView<T> GetList(FieldAccessor<T> accessor) const {
    const AccessorMetadata metadata{
        .schema_version = accessor.schema_version,
        .layout_fingerprint = accessor.layout_fingerprint,
        .field_id = accessor.field_id,
        .is_list = accessor.is_list,
        .physical_type = accessor.physical_type,
        .encoding = accessor.encoding,
        .offset = accessor.offset,
        .presence_bit_index = accessor.presence_bit_index,
    };
    auto value = ReadListValue(accessor.field_id, FieldTypeFor<T>(), &metadata);
    if (!value.ok()) {
      if (value.status().code() == StatusCode::kNotFound) {
        return ListView<T>();
      }
      ThrowStatus(value.status());
    }

    const auto* typed =
        std::get_if<std::vector<std::remove_cv_t<T>>>(&value.value());
    if (typed == nullptr) {
      throw std::runtime_error("row decoder returned an unexpected list type");
    }
    return ListView<T>(*typed);
  }

 private:
 public:
  struct AccessorMetadata {
    std::uint64_t schema_version = 0;
    std::uint64_t layout_fingerprint = 0;
    FieldId field_id = 0;
    bool is_list = false;
    FieldType physical_type = FieldType::kInt64;
    FieldEncoding encoding = FieldEncoding::kFixed;
    std::size_t offset = 0;
    std::size_t presence_bit_index = 0;
  };

 private:
  static void ThrowStatus(const Status& status) {
    std::string message = status.message().empty() ? "row access failed"
                                                   : status.message();
    throw std::runtime_error(message);
  }

  StatusOr<ScalarValue> ReadScalarValue(
      FieldId field_id, FieldType expected_type,
      const AccessorMetadata* accessor) const;
  StatusOr<ListValue> ReadListValue(FieldId field_id, FieldType expected_type,
                                    const AccessorMetadata* accessor) const;

  std::shared_ptr<const CompiledRowLayout> layout_;
  std::shared_ptr<const internal::model::EncodedRow> encoded_;
};

}  // namespace kv_index

#endif  // KV_INDEX_ROW_H_
