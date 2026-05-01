#include "kv_index/schema.h"
#include "src/model/row_storage.h"
#include "src/store/snapshot.h"
#include "src/store/snapshot_builder.h"

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
namespace storage = kv_index::internal;

std::shared_ptr<const CompiledRowLayout> Layout() {
  kv_index::RuntimeSchema schema(1003);
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
  kv_index::core::SnapshotBuilder builder(layout);
  for (std::uint64_t key = 1; key <= 4096; ++key) {
    (void)builder.AddRow(key, Row(*layout, static_cast<std::int32_t>(key)));
  }
  auto backing = std::move(builder).Seal().value();

  constexpr std::uint64_t kIterations = 200;
  std::uint64_t checksum = 0;
  const auto start = std::chrono::steady_clock::now();
  for (std::uint64_t i = 0; i < kIterations; ++i) {
    auto rows = kv_index::core::EnumerateSnapshotRows(*backing).value();
    checksum += rows.size();
  }
  const auto elapsed = std::chrono::steady_clock::now() - start;
  const auto micros =
      std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
  std::cout << "snapshot_decode iterations=" << kIterations
            << " rows=" << backing->row_count() << " micros=" << micros
            << " checksum=" << checksum << "\n";
  return 0;
}
