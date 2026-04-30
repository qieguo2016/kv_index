#ifndef KV_INDEX_ROW_H_
#define KV_INDEX_ROW_H_

#include <cstddef>
#include <cstdint>
#include <optional>

#include "kv_index/types.h"

namespace kv_index {

template <typename T>
class ListView {
 public:
  constexpr ListView() = default;
  constexpr ListView(const T* data, std::size_t size) : data_(data), size_(size) {}

  constexpr const T* data() const noexcept { return data_; }
  constexpr std::size_t size() const noexcept { return size_; }
  constexpr bool empty() const noexcept { return size_ == 0; }

  constexpr const T* begin() const noexcept { return data_; }
  constexpr const T* end() const noexcept {
    return data_ == nullptr ? nullptr : data_ + size_;
  }

 private:
  const T* data_ = nullptr;
  std::size_t size_ = 0;
};

template <typename T>
struct FieldAccessor {
  std::uint64_t schema_version = 0;
  std::uint64_t layout_fingerprint = 0;
  FieldId field_id = 0;
  bool is_list = false;
  std::uint32_t physical_type = 0;
  std::uint32_t offset = 0;
};

class Row {
 public:
  bool Has(FieldId field_id) const noexcept;

  template <typename T>
  std::optional<T> Get(FieldId field_id) const {
    (void)field_id;
    return std::nullopt;
  }

  template <typename T>
  std::optional<T> Get(FieldAccessor<T> accessor) const {
    (void)accessor;
    return std::nullopt;
  }

  template <typename T>
  ListView<T> GetList(FieldId field_id) const {
    (void)field_id;
    return {};
  }
};

inline bool Row::Has(FieldId field_id) const noexcept {
  (void)field_id;
  return false;
}

}  // namespace kv_index

#endif  // KV_INDEX_ROW_H_
