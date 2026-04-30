#include "kv_index/row.h"
#include "kv_index/schema.h"
#include "src/core/row_storage.h"

#include <array>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include "tests/test_support/test_macros.h"

namespace {

using kv_index::CompiledRowLayout;
using kv_index::FieldEncoding;
using kv_index::FieldLayout;
using kv_index::FieldSpec;
using kv_index::FieldType;
using kv_index::MakeFieldAccessor;
using kv_index::MakeListFieldAccessor;
using kv_index::RuntimeSchema;
using kv_index::Row;
namespace storage = kv_index::internal;

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

Row MakeRow(const CompiledRowLayout& layout, storage::EncodedRow encoded) {
  auto row = storage::MaterializeRow(
      std::make_shared<CompiledRowLayout>(layout), std::move(encoded));
  KV_INDEX_CHECK(row.ok());
  return std::move(row).value();
}

void AccessorCreationRejectsUnknownDeletedShapeAndTypeMismatches() {
  RuntimeSchema schema(10);
  KV_INDEX_CHECK(schema.AddField(Scalar(1, "age", FieldType::kInt32)).ok());
  KV_INDEX_CHECK(schema.AddField(Scalar(2, "name", FieldType::kString)).ok());
  KV_INDEX_CHECK(schema.AddField(List(3, "scores", FieldType::kInt32)).ok());
  KV_INDEX_CHECK(schema.AddField(Scalar(4, "old", FieldType::kInt64)).ok());
  KV_INDEX_CHECK(schema.DeleteField(4).ok());
  const CompiledRowLayout layout = Compile(schema);

  const auto age = MakeFieldAccessor<std::int32_t>(layout, 1);
  KV_INDEX_CHECK(age.ok());
  KV_INDEX_CHECK_EQ(age->schema_version, 10);
  KV_INDEX_CHECK_EQ(age->field_id, 1U);
  KV_INDEX_CHECK(!age->is_list);
  KV_INDEX_CHECK_EQ(age->physical_type, FieldType::kInt32);

  KV_INDEX_CHECK(!MakeFieldAccessor<std::int64_t>(layout, 1).ok());
  KV_INDEX_CHECK(!MakeFieldAccessor<std::int32_t>(layout, 3).ok());
  KV_INDEX_CHECK(!MakeFieldAccessor<std::int64_t>(layout, 4).ok());
  KV_INDEX_CHECK(!MakeFieldAccessor<std::int64_t>(layout, 999).ok());

  const auto scores = MakeListFieldAccessor<std::int32_t>(layout, 3);
  KV_INDEX_CHECK(scores.ok());
  KV_INDEX_CHECK(scores->is_list);
  KV_INDEX_CHECK_EQ(scores->physical_type, FieldType::kInt32);
  KV_INDEX_CHECK(!MakeListFieldAccessor<std::int64_t>(layout, 3).ok());
  KV_INDEX_CHECK(!MakeListFieldAccessor<std::int32_t>(layout, 1).ok());
}

void RowHasUsesPhysicalPresenceWhileGetAppliesDefaults() {
  RuntimeSchema schema(20);
  FieldSpec default_age = Scalar(1, "age", FieldType::kInt32);
  default_age.default_value = std::int32_t{99};
  KV_INDEX_CHECK(schema.AddField(default_age).ok());
  KV_INDEX_CHECK(schema.AddField(Scalar(2, "flag", FieldType::kBool)).ok());
  FieldSpec required_counter = Scalar(3, "counter", FieldType::kInt64);
  required_counter.nullable = false;
  KV_INDEX_CHECK(schema.AddField(required_counter).ok());
  const CompiledRowLayout layout = Compile(schema);

  auto encoded = storage::CreateEncodedRow(layout);
  KV_INDEX_CHECK(
      storage::WriteScalarField(Field(layout, 3), std::int64_t{123}, &encoded)
          .ok());
  Row row = MakeRow(layout, std::move(encoded));

  KV_INDEX_CHECK(!row.Has(1));
  const auto age = row.Get<std::int32_t>(1);
  KV_INDEX_CHECK(age.has_value());
  KV_INDEX_CHECK_EQ(age.value(), 99);
  KV_INDEX_CHECK(!row.Has(2));
  KV_INDEX_CHECK(!row.Get<bool>(2).has_value());
  KV_INDEX_CHECK(row.Has(3));
  KV_INDEX_CHECK_EQ(row.Get<std::int64_t>(3).value(), 123);
  KV_INDEX_CHECK_THROWS(row.Get<std::int64_t>(1), std::runtime_error);

  RuntimeSchema next_schema(21);
  KV_INDEX_CHECK(next_schema.AddField(Scalar(1, "age", FieldType::kInt32))
                     .ok());
  const CompiledRowLayout next_layout = Compile(next_schema);
  const auto incompatible = MakeFieldAccessor<std::int32_t>(next_layout, 1);
  KV_INDEX_CHECK(incompatible.ok());
  KV_INDEX_CHECK_THROWS(row.Get<std::int32_t>(incompatible.value()),
                        std::runtime_error);
}

void IncompatibleAccessorsForInactiveFieldsSurfaceMismatch() {
  RuntimeSchema target_schema(21);
  KV_INDEX_CHECK(
      target_schema.AddField(Scalar(1, "age", FieldType::kInt32)).ok());
  KV_INDEX_CHECK(
      target_schema.AddField(Scalar(9, "legacy", FieldType::kInt64)).ok());
  KV_INDEX_CHECK(
      target_schema.AddField(List(10, "legacy_scores", FieldType::kInt32))
          .ok());
  KV_INDEX_CHECK(target_schema.DeleteField(9).ok());
  KV_INDEX_CHECK(target_schema.DeleteField(10).ok());
  const CompiledRowLayout target_layout = Compile(target_schema);

  RuntimeSchema old_schema(20);
  KV_INDEX_CHECK(
      old_schema.AddField(Scalar(9, "legacy", FieldType::kInt64)).ok());
  KV_INDEX_CHECK(
      old_schema.AddField(List(10, "legacy_scores", FieldType::kInt32)).ok());
  const CompiledRowLayout old_layout = Compile(old_schema);
  const auto legacy = MakeFieldAccessor<std::int64_t>(old_layout, 9);
  KV_INDEX_CHECK(legacy.ok());
  const auto legacy_scores =
      MakeListFieldAccessor<std::int32_t>(old_layout, 10);
  KV_INDEX_CHECK(legacy_scores.ok());

  auto encoded = storage::CreateEncodedRow(target_layout);
  KV_INDEX_CHECK(
      storage::WriteScalarField(Field(target_layout, 1), std::int32_t{7},
                                &encoded)
          .ok());
  Row row = MakeRow(target_layout, std::move(encoded));

  KV_INDEX_CHECK(!row.Get<std::int64_t>(9).has_value());
  KV_INDEX_CHECK(row.GetList<std::int32_t>(10).empty());
  KV_INDEX_CHECK_THROWS(row.Get<std::int64_t>(legacy.value()),
                        std::runtime_error);
  KV_INDEX_CHECK_THROWS(row.GetList<std::int32_t>(legacy_scores.value()),
                        std::runtime_error);
}

void RowDecodesArenaAndDictionaryStringsThroughOneMaterializationShape() {
  RuntimeSchema schema(30);
  KV_INDEX_CHECK(schema.AddField(Scalar(1, "title", FieldType::kString)).ok());
  FieldSpec country = Scalar(2, "country", FieldType::kString);
  country.encoding = FieldEncoding::kDictionary;
  KV_INDEX_CHECK(schema.AddField(country).ok());
  const CompiledRowLayout layout = Compile(schema);

  auto encoded = storage::CreateEncodedRow(layout);
  encoded.string_dictionary = {"us", "sg"};
  KV_INDEX_CHECK(
      storage::WriteArenaStringField(Field(layout, 1), "hello", &encoded).ok());
  KV_INDEX_CHECK(
      storage::WriteDictionaryStringField(Field(layout, 2), 1, &encoded).ok());

  Row row = MakeRow(layout, std::move(encoded));
  KV_INDEX_CHECK(row.Has(1));
  KV_INDEX_CHECK(row.Has(2));
  KV_INDEX_CHECK_EQ(row.Get<std::string>(1).value(), std::string("hello"));
  KV_INDEX_CHECK_EQ(row.Get<std::string>(2).value(), std::string("sg"));
}

void RowDistinguishesMissingDefaultAndPresentEmptyLists() {
  RuntimeSchema schema(40);
  KV_INDEX_CHECK(schema.AddField(List(1, "scores", FieldType::kInt32)).ok());
  FieldSpec defaults = List(2, "defaults", FieldType::kInt32);
  defaults.default_value = std::vector<std::int32_t>{7, 8};
  KV_INDEX_CHECK(schema.AddField(defaults).ok());
  KV_INDEX_CHECK(schema.AddField(List(3, "empty", FieldType::kInt32)).ok());
  KV_INDEX_CHECK(schema.AddField(List(4, "missing", FieldType::kInt32)).ok());
  const CompiledRowLayout layout = Compile(schema);

  auto encoded = storage::CreateEncodedRow(layout);
  const std::array<std::int32_t, 3> scores = {1, 2, 3};
  KV_INDEX_CHECK(storage::WriteArenaListField(Field(layout, 1),
                                              std::span<const std::int32_t>(
                                                  scores.data(), scores.size()),
                                              &encoded)
                     .ok());
  KV_INDEX_CHECK(storage::WriteArenaListField(
                     Field(layout, 3), std::span<const std::int32_t>(), &encoded)
                     .ok());
  Row row = MakeRow(layout, std::move(encoded));

  const auto score_view = row.GetList<std::int32_t>(1);
  KV_INDEX_CHECK(row.Has(1));
  KV_INDEX_CHECK_EQ(score_view.size(), 3U);
  KV_INDEX_CHECK_EQ(score_view[0], 1);
  KV_INDEX_CHECK_EQ(score_view[2], 3);

  const auto default_view = row.GetList<std::int32_t>(2);
  KV_INDEX_CHECK(!row.Has(2));
  KV_INDEX_CHECK_EQ(default_view.size(), 2U);
  KV_INDEX_CHECK_EQ(default_view[0], 7);
  KV_INDEX_CHECK_EQ(default_view[1], 8);

  const auto empty_view = row.GetList<std::int32_t>(3);
  KV_INDEX_CHECK(row.Has(3));
  KV_INDEX_CHECK(empty_view.empty());

  KV_INDEX_CHECK(!row.Has(4));
  KV_INDEX_CHECK(row.GetList<std::int32_t>(4).empty());
  KV_INDEX_CHECK_THROWS(row.GetList<std::int64_t>(1), std::runtime_error);
}

void RowDecodesWholeListAndElementDictionaryLists() {
  RuntimeSchema schema(50);
  KV_INDEX_CHECK(
      schema.AddField(List(1, "dict_scores", FieldType::kInt32,
                           FieldEncoding::kListDictionary))
          .ok());
  KV_INDEX_CHECK(
      schema.AddField(List(2, "dict_words", FieldType::kString,
                           FieldEncoding::kListDictionary))
          .ok());
  KV_INDEX_CHECK(
      schema.AddField(List(3, "element_words", FieldType::kString,
                           FieldEncoding::kElementDictionary))
          .ok());
  const CompiledRowLayout layout = Compile(schema);

  auto encoded = storage::CreateEncodedRow(layout);
  const std::array<std::int32_t, 2> dict_scores = {5, 6};
  auto encoded_scores = storage::EncodeScalarList(dict_scores);
  KV_INDEX_CHECK(encoded_scores.ok());
  encoded.scalar_list_dictionary.push_back(std::move(encoded_scores).value());
  encoded.string_list_dictionary.push_back({"red", "blue"});
  encoded.string_element_dictionary = {"shoe", "bag"};
  KV_INDEX_CHECK(
      storage::WriteListDictionaryField(Field(layout, 1), 0, 2, &encoded).ok());
  KV_INDEX_CHECK(
      storage::WriteListDictionaryField(Field(layout, 2), 0, 2, &encoded).ok());
  const std::array<std::uint32_t, 2> word_ids = {0, 1};
  KV_INDEX_CHECK(storage::WriteElementDictionaryStringListField(
                     Field(layout, 3),
                     std::span<const std::uint32_t>(word_ids.data(),
                                                    word_ids.size()),
                     &encoded)
                     .ok());

  Row row = MakeRow(layout, std::move(encoded));
  const auto score_view = row.GetList<std::int32_t>(1);
  KV_INDEX_CHECK_EQ(score_view.size(), 2U);
  KV_INDEX_CHECK_EQ(score_view[0], 5);
  KV_INDEX_CHECK_EQ(score_view[1], 6);

  const auto dict_words = row.GetList<std::string>(2);
  KV_INDEX_CHECK_EQ(dict_words.size(), 2U);
  KV_INDEX_CHECK_EQ(dict_words[0], std::string("red"));
  KV_INDEX_CHECK_EQ(dict_words[1], std::string("blue"));

  const auto element_words = row.GetList<std::string>(3);
  KV_INDEX_CHECK_EQ(element_words.size(), 2U);
  KV_INDEX_CHECK_EQ(element_words[0], std::string("shoe"));
  KV_INDEX_CHECK_EQ(element_words[1], std::string("bag"));
}

}  // namespace

int main() {
  AccessorCreationRejectsUnknownDeletedShapeAndTypeMismatches();
  RowHasUsesPhysicalPresenceWhileGetAppliesDefaults();
  IncompatibleAccessorsForInactiveFieldsSurfaceMismatch();
  RowDecodesArenaAndDictionaryStringsThroughOneMaterializationShape();
  RowDistinguishesMissingDefaultAndPresentEmptyLists();
  RowDecodesWholeListAndElementDictionaryLists();
  return 0;
}
