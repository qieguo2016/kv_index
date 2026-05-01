#include "kv_index/schema.h"
#include "src/store/realtime_delta.h"
#include "src/model/row_storage.h"

#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>

namespace {

using kv_index::CompiledRowLayout;
using kv_index::FieldEncoding;
using kv_index::FieldSpec;
using kv_index::FieldType;
namespace storage = kv_index::internal::model;

std::shared_ptr<const CompiledRowLayout> Layout() {
  kv_index::RuntimeSchema schema(1002);
  (void)schema.AddField(FieldSpec{.field_id = 1,
                                  .name = "score",
                                  .type = FieldType::kInt32,
                                  .is_list = false,
                                  .nullable = false,
                                  .encoding = FieldEncoding::kFixed});
  auto layout = CompiledRowLayout::Compile(schema);
  return std::make_shared<const CompiledRowLayout>(std::move(layout).value());
}

storage::EncodedRow Row(const CompiledRowLayout& layout, std::int32_t score) {
  auto row = storage::CreateEncodedRow(layout);
  (void)storage::WriteScalarField(*layout.FindField(1), score, &row);
  return row;
}

}  // namespace

int main() {
  auto layout = Layout();
  kv_index::internal::store::RealtimeDeltaAtomicTable table(
      kv_index::internal::store::RealtimeDeltaAtomicTable::Options{
          .layout = layout,
          .capacity = 4096,
      });

  constexpr std::uint64_t kIterations = 2000;
  const auto start = std::chrono::steady_clock::now();
  for (std::uint64_t key = 1; key <= kIterations; ++key) {
    (void)table.Publish(
        key, kv_index::SourcePosition{.partition = 0,
                                      .offset = static_cast<std::int64_t>(key)},
        Row(*layout, static_cast<std::int32_t>(key)));
  }
  const auto elapsed = std::chrono::steady_clock::now() - start;
  const auto micros =
      std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
  const auto stats = table.stats();
  std::cout << "realtime_publish rows=" << kIterations
            << " micros=" << micros
            << " unique=" << stats.unique_visible_keys << "\n";
  return 0;
}
