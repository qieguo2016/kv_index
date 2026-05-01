#include "src/store/snapshot.h"
#include "src/store/snapshot_builder.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "kv_index/row.h"
#include "kv_index/schema.h"
#include "src/store/frozen_primary_key_index.h"
#include "src/model/row_storage.h"
#include "tests/test_support/test_macros.h"

namespace {

using kv_index::CompiledRowLayout;
using kv_index::FieldEncoding;
using kv_index::FieldLayout;
using kv_index::FieldSpec;
using kv_index::FieldType;
using kv_index::RuntimeSchema;
using kv_index::Row;
using kv_index::StatusCode;
using kv_index::internal::store::BuildFrozenPrimaryKeyIndex;
using kv_index::internal::store::CompactDeltaSnapshot;
using kv_index::internal::store::FrozenPrimaryKeyIndexEntry;
using kv_index::internal::store::FullSnapshotView;
using kv_index::internal::store::ImmutableRowSnapshotView;
using kv_index::internal::store::OwnedSnapshotBacking;
using kv_index::internal::store::OwnedSnapshotRowPayload;
using kv_index::internal::store::SnapshotBuildOptions;
using kv_index::internal::store::SnapshotBuilder;
namespace storage = kv_index::internal::model;

FieldSpec Scalar(kv_index::FieldId field_id, std::string name,
                 FieldType type) {
  return FieldSpec{
      .field_id = field_id,
      .name = std::move(name),
      .type = type,
      .is_list = false,
      .nullable = true,
      .encoding = type == FieldType::kString ? FieldEncoding::kArena
                                             : FieldEncoding::kFixed,
  };
}

FieldSpec List(kv_index::FieldId field_id, std::string name,
               FieldType element_type,
               FieldEncoding encoding = FieldEncoding::kArena) {
  return FieldSpec{
      .field_id = field_id,
      .name = std::move(name),
      .type = element_type,
      .is_list = true,
      .nullable = true,
      .encoding = encoding,
  };
}

CompiledRowLayout Compile(RuntimeSchema schema) {
  auto layout = CompiledRowLayout::Compile(schema);
  KV_INDEX_CHECK(layout.ok());
  return layout.value();
}

const FieldLayout& Field(const CompiledRowLayout& layout,
                         kv_index::FieldId field_id) {
  const FieldLayout* field = layout.FindField(field_id);
  KV_INDEX_CHECK(field != nullptr);
  return *field;
}

std::shared_ptr<const CompiledRowLayout> MakeLayout() {
  RuntimeSchema schema(100);
  KV_INDEX_CHECK(schema.AddField(Scalar(1, "score", FieldType::kInt32)).ok());
  KV_INDEX_CHECK(schema.AddField(Scalar(2, "title", FieldType::kString)).ok());
  FieldSpec country = Scalar(3, "country", FieldType::kString);
  country.encoding = FieldEncoding::kDictionary;
  KV_INDEX_CHECK(schema.AddField(country).ok());
  KV_INDEX_CHECK(schema.AddField(List(4, "scores", FieldType::kInt32)).ok());
  KV_INDEX_CHECK(
      schema.AddField(List(5, "dict_scores", FieldType::kInt32,
                           FieldEncoding::kListDictionary))
          .ok());
  KV_INDEX_CHECK(
      schema.AddField(List(6, "dict_words", FieldType::kString,
                           FieldEncoding::kListDictionary))
          .ok());
  KV_INDEX_CHECK(
      schema.AddField(List(7, "element_words", FieldType::kString,
                           FieldEncoding::kElementDictionary))
          .ok());
  return std::make_shared<const CompiledRowLayout>(Compile(std::move(schema)));
}

storage::EncodedRow MakeEncodedRow(const CompiledRowLayout& layout,
                                   std::int32_t score, std::string title,
                                   std::uint32_t country_id) {
  auto encoded = storage::CreateEncodedRow(layout);
  encoded.string_dictionary = {"us", "sg", "br"};
  encoded.string_list_dictionary.push_back({"red", "blue"});
  encoded.string_element_dictionary = {"shoe", "bag", "watch"};

  const std::array<std::int32_t, 2> dict_scores = {8, 13};
  auto encoded_dict_scores = storage::EncodeScalarList(dict_scores);
  KV_INDEX_CHECK(encoded_dict_scores.ok());
  encoded.scalar_list_dictionary.push_back(std::move(encoded_dict_scores).value());

  KV_INDEX_CHECK(
      storage::WriteScalarField(Field(layout, 1), score, &encoded).ok());
  KV_INDEX_CHECK(
      storage::WriteArenaStringField(Field(layout, 2), title, &encoded).ok());
  KV_INDEX_CHECK(storage::WriteDictionaryStringField(Field(layout, 3),
                                                     country_id, &encoded)
                     .ok());

  const std::array<std::int32_t, 3> scores = {1, 2, 3};
  KV_INDEX_CHECK(storage::WriteArenaListField(
                     Field(layout, 4),
                     std::span<const std::int32_t>(scores.data(),
                                                   scores.size()),
                     &encoded)
                     .ok());
  KV_INDEX_CHECK(
      storage::WriteListDictionaryField(Field(layout, 5), 0, 2, &encoded).ok());
  KV_INDEX_CHECK(
      storage::WriteListDictionaryField(Field(layout, 6), 0, 2, &encoded).ok());

  const std::array<std::uint32_t, 2> word_ids = {0, 1};
  KV_INDEX_CHECK(storage::WriteElementDictionaryStringListField(
                     Field(layout, 7),
                     std::span<const std::uint32_t>(word_ids.data(),
                                                    word_ids.size()),
                     &encoded)
                     .ok());
  return encoded;
}

std::shared_ptr<const OwnedSnapshotBacking> BuildTwoRowSnapshot(
    std::shared_ptr<const CompiledRowLayout> layout) {
  SnapshotBuilder builder(layout,
                          SnapshotBuildOptions{.hash_seed = 19,
                                               .hash_version = 4});
  KV_INDEX_CHECK(
      builder.AddRow(100, MakeEncodedRow(*layout, 7, "hello", 1)).ok());
  KV_INDEX_CHECK(
      builder.AddRow(200, MakeEncodedRow(*layout, 9, "world", 2)).ok());
  auto backing = builder.Seal();
  KV_INDEX_CHECK(backing.ok());
  KV_INDEX_CHECK_EQ(backing.value()->row_count(), 2U);
  return backing.value();
}

void CheckDecodedRow(const Row& row, std::int32_t score, std::string title,
                     std::string country) {
  KV_INDEX_CHECK(row.Has(1));
  KV_INDEX_CHECK_EQ(row.Get<std::int32_t>(1).value(), score);
  KV_INDEX_CHECK_EQ(row.Get<std::string>(2).value(), title);
  KV_INDEX_CHECK_EQ(row.Get<std::string>(3).value(), country);

  const auto scores = row.GetList<std::int32_t>(4);
  KV_INDEX_CHECK_EQ(scores.size(), 3U);
  KV_INDEX_CHECK_EQ(scores[0], 1);
  KV_INDEX_CHECK_EQ(scores[2], 3);

  const auto dict_scores = row.GetList<std::int32_t>(5);
  KV_INDEX_CHECK_EQ(dict_scores.size(), 2U);
  KV_INDEX_CHECK_EQ(dict_scores[0], 8);
  KV_INDEX_CHECK_EQ(dict_scores[1], 13);

  const auto dict_words = row.GetList<std::string>(6);
  KV_INDEX_CHECK_EQ(dict_words.size(), 2U);
  KV_INDEX_CHECK_EQ(dict_words[0], std::string("red"));
  KV_INDEX_CHECK_EQ(dict_words[1], std::string("blue"));

  const auto element_words = row.GetList<std::string>(7);
  KV_INDEX_CHECK_EQ(element_words.size(), 2U);
  KV_INDEX_CHECK_EQ(element_words[0], std::string("shoe"));
  KV_INDEX_CHECK_EQ(element_words[1], std::string("bag"));
}

void SnapshotBuilderServesEncodedRowsThroughImmutableView() {
  auto layout = MakeLayout();
  auto backing = BuildTwoRowSnapshot(layout);
  ImmutableRowSnapshotView view(backing);

  auto first = view.Get(100);
  KV_INDEX_CHECK(first.ok());
  KV_INDEX_CHECK(first->has_value());
  CheckDecodedRow(first->value(), 7, "hello", "sg");

  auto second = view.Get(200);
  KV_INDEX_CHECK(second.ok());
  KV_INDEX_CHECK(second->has_value());
  CheckDecodedRow(second->value(), 9, "world", "br");

  auto miss = view.Get(999);
  KV_INDEX_CHECK(miss.ok());
  KV_INDEX_CHECK(!miss->has_value());
}

void SnapshotBuilderRejectsDuplicateAndMismatchedEncodedRows() {
  auto layout = MakeLayout();
  SnapshotBuilder builder(layout);

  KV_INDEX_CHECK(
      builder.AddRow(1, MakeEncodedRow(*layout, 1, "first", 0)).ok());
  auto duplicate = builder.AddRow(1, MakeEncodedRow(*layout, 2, "dupe", 1));
  KV_INDEX_CHECK(!duplicate.ok());
  KV_INDEX_CHECK_EQ(duplicate.code(), StatusCode::kInvalidArgument);

  auto bad_schema = MakeEncodedRow(*layout, 3, "bad_schema", 0);
  bad_schema.schema_version += 1;
  auto bad_schema_status = builder.AddRow(2, std::move(bad_schema));
  KV_INDEX_CHECK(!bad_schema_status.ok());
  KV_INDEX_CHECK_EQ(bad_schema_status.code(), StatusCode::kFailedPrecondition);

  auto short_slot = MakeEncodedRow(*layout, 4, "short", 0);
  short_slot.row_slot.pop_back();
  auto short_slot_status = builder.AddRow(3, std::move(short_slot));
  KV_INDEX_CHECK(!short_slot_status.ok());
  KV_INDEX_CHECK_EQ(short_slot_status.code(), StatusCode::kInvalidArgument);
}

void ReturnedRowsOutliveBuilderBackingAndViewTemporaries() {
  Row pinned;
  {
    auto layout = MakeLayout();
    auto backing = BuildTwoRowSnapshot(layout);
    {
      ImmutableRowSnapshotView view(backing);
      auto result = view.Get(100);
      KV_INDEX_CHECK(result.ok());
      KV_INDEX_CHECK(result->has_value());
      pinned = std::move(result->value());
    }
    backing.reset();
    layout.reset();
  }

  CheckDecodedRow(pinned, 7, "hello", "sg");
}

void FullAndCompactSnapshotsDelegateToTheSameImmutableDecodePath() {
  auto layout = MakeLayout();
  auto backing = BuildTwoRowSnapshot(layout);
  FullSnapshotView full(backing);
  CompactDeltaSnapshot compact(backing);

  auto full_row = full.Get(200);
  auto compact_row = compact.Get(200);
  KV_INDEX_CHECK(full_row.ok());
  KV_INDEX_CHECK(compact_row.ok());
  KV_INDEX_CHECK(full_row->has_value());
  KV_INDEX_CHECK(compact_row->has_value());
  CheckDecodedRow(full_row->value(), 9, "world", "br");
  CheckDecodedRow(compact_row->value(), 9, "world", "br");

  auto full_miss = full.Get(404);
  auto compact_miss = compact.Get(404);
  KV_INDEX_CHECK(full_miss.ok());
  KV_INDEX_CHECK(compact_miss.ok());
  KV_INDEX_CHECK(!full_miss->has_value());
  KV_INDEX_CHECK(!compact_miss->has_value());
}

void MalformedBackingReturnsExplicitErrorInsteadOfMiss() {
  auto layout = MakeLayout();
  const std::array<FrozenPrimaryKeyIndexEntry, 1> entries = {{
      {.primary_key = 123,
       .row_offset = static_cast<std::uint64_t>(layout->row_slot_size() * 4)},
  }};
  auto index_bytes = BuildFrozenPrimaryKeyIndex(entries);
  KV_INDEX_CHECK(index_bytes.ok());

  std::vector<std::byte> row_slot_bytes(layout->row_slot_size());
  std::vector<OwnedSnapshotRowPayload> payloads(1);
  auto corrupt_backing = std::make_shared<OwnedSnapshotBacking>(
      layout, std::move(index_bytes).value(), std::move(row_slot_bytes),
      std::move(payloads));
  ImmutableRowSnapshotView view(corrupt_backing);

  auto result = view.Get(123);
  KV_INDEX_CHECK(!result.ok());
  KV_INDEX_CHECK_EQ(result.status().code(), StatusCode::kInvalidArgument);
}

}  // namespace

int main() {
  SnapshotBuilderServesEncodedRowsThroughImmutableView();
  SnapshotBuilderRejectsDuplicateAndMismatchedEncodedRows();
  ReturnedRowsOutliveBuilderBackingAndViewTemporaries();
  FullAndCompactSnapshotsDelegateToTheSameImmutableDecodePath();
  MalformedBackingReturnsExplicitErrorInsteadOfMiss();
  return 0;
}
