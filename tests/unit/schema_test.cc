#include "kv_index/schema.h"

#include <cstdint>
#include <string>

#include "kv_index/status.h"
#include "tests/test_support/test_macros.h"

namespace {

using kv_index::CompiledRowLayout;
using kv_index::FieldEncoding;
using kv_index::FieldSpec;
using kv_index::FieldType;
using kv_index::FieldEncodingConfigName;
using kv_index::ParseFieldEncoding;
using kv_index::RuntimeSchema;
using kv_index::StatusCode;

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

void RuntimeSchemaTracksActiveAndDeletedFields() {
  RuntimeSchema schema(7);

  FieldSpec age = Scalar(10, "age", FieldType::kInt32);
  age.nullable = false;
  FieldSpec country = Scalar(20, "country", FieldType::kString);
  country.default_value = std::string("unknown");

  KV_INDEX_CHECK(schema.AddField(age).ok());
  KV_INDEX_CHECK(schema.AddField(country).ok());
  KV_INDEX_CHECK_EQ(schema.version(), 7);
  KV_INDEX_CHECK(schema.FindField(10) != nullptr);
  KV_INDEX_CHECK(schema.FindField(20)->HasDefault());
  KV_INDEX_CHECK(schema.FindDeletedField(10) == nullptr);

  KV_INDEX_CHECK(schema.DeleteField(10).ok());
  KV_INDEX_CHECK(schema.FindField(10) == nullptr);
  KV_INDEX_CHECK(schema.FindDeletedField(10) != nullptr);
  KV_INDEX_CHECK(!schema.AddField(Scalar(10, "age_again", FieldType::kInt64))
                      .ok());
  KV_INDEX_CHECK(!schema.DeleteField(10).ok());
}

void RuntimeSchemaRejectsInvalidPoliciesAndEvolution() {
  RuntimeSchema schema(3);

  FieldSpec bad_string = Scalar(1, "bad_string", FieldType::kString);
  bad_string.encoding = FieldEncoding::kFixed;
  const auto bad_string_status = schema.AddField(bad_string);
  KV_INDEX_CHECK(!bad_string_status.ok());
  KV_INDEX_CHECK_EQ(bad_string_status.code(), StatusCode::kInvalidArgument);

  FieldSpec bad_default = Scalar(2, "bad_default", FieldType::kInt32);
  bad_default.default_value = std::string("wrong type");
  KV_INDEX_CHECK(!schema.AddField(bad_default).ok());

  KV_INDEX_CHECK(schema.AddField(Scalar(5, "score", FieldType::kInt64)).ok());
  KV_INDEX_CHECK(!schema.AddField(Scalar(5, "score_v2", FieldType::kInt32))
                      .ok());
  KV_INDEX_CHECK(!schema.AddField(List(5, "score_list", FieldType::kInt64))
                      .ok());

  FieldSpec dict_int = Scalar(6, "dict_int", FieldType::kInt32);
  dict_int.encoding = FieldEncoding::kDictionary;
  KV_INDEX_CHECK(!schema.AddField(dict_int).ok());
}

void FieldEncodingConfigUsesUnifiedDictionarySpelling() {
  const auto scalar_dictionary =
      ParseFieldEncoding("dictionary", false, FieldType::kString);
  KV_INDEX_CHECK(scalar_dictionary.ok());
  KV_INDEX_CHECK_EQ(scalar_dictionary.value(), FieldEncoding::kDictionary);

  const auto scalar_list_dictionary =
      ParseFieldEncoding("dictionary", true, FieldType::kInt32);
  KV_INDEX_CHECK(scalar_list_dictionary.ok());
  KV_INDEX_CHECK_EQ(scalar_list_dictionary.value(),
                    FieldEncoding::kListDictionary);

  const auto string_list_dictionary =
      ParseFieldEncoding("dictionary", true, FieldType::kString);
  KV_INDEX_CHECK(string_list_dictionary.ok());
  KV_INDEX_CHECK_EQ(string_list_dictionary.value(),
                    FieldEncoding::kListDictionary);

  const auto element_dictionary =
      ParseFieldEncoding("element_dictionary", true, FieldType::kString);
  KV_INDEX_CHECK(element_dictionary.ok());
  KV_INDEX_CHECK_EQ(element_dictionary.value(),
                    FieldEncoding::kElementDictionary);

  KV_INDEX_CHECK_EQ(FieldEncodingConfigName(FieldEncoding::kDictionary),
                    "dictionary");
  KV_INDEX_CHECK_EQ(FieldEncodingConfigName(FieldEncoding::kListDictionary),
                    "dictionary");

  const auto legacy_list_dictionary =
      ParseFieldEncoding("list_dictionary", true, FieldType::kInt32);
  KV_INDEX_CHECK(!legacy_list_dictionary.ok());
  KV_INDEX_CHECK_EQ(legacy_list_dictionary.status().code(),
                    StatusCode::kInvalidArgument);

  const auto scalar_int_dictionary =
      ParseFieldEncoding("dictionary", false, FieldType::kInt32);
  KV_INDEX_CHECK(!scalar_int_dictionary.ok());
  KV_INDEX_CHECK_EQ(scalar_int_dictionary.status().code(),
                    StatusCode::kInvalidArgument);
}

void CompiledLayoutIsDeterministicAndAligned() {
  RuntimeSchema schema(42);
  KV_INDEX_CHECK(schema.AddField(Scalar(30, "wide", FieldType::kInt64)).ok());
  KV_INDEX_CHECK(schema.AddField(Scalar(10, "tiny", FieldType::kInt8)).ok());
  KV_INDEX_CHECK(schema.AddField(Scalar(20, "name", FieldType::kString)).ok());
  KV_INDEX_CHECK(schema.AddField(List(40, "tags", FieldType::kUInt64)).ok());

  const auto compiled = CompiledRowLayout::Compile(schema);
  KV_INDEX_CHECK(compiled.ok());
  const auto& layout = compiled.value();

  KV_INDEX_CHECK_EQ(layout.schema_version(), 42);
  KV_INDEX_CHECK_EQ(layout.fields().size(), 4U);
  KV_INDEX_CHECK_EQ(layout.fields()[0].field_id, 10U);
  KV_INDEX_CHECK_EQ(layout.fields()[1].field_id, 20U);
  KV_INDEX_CHECK_EQ(layout.fields()[2].field_id, 30U);
  KV_INDEX_CHECK_EQ(layout.fields()[3].field_id, 40U);
  KV_INDEX_CHECK_EQ(layout.presence_bitmap_bytes(), 8U);
  KV_INDEX_CHECK_EQ(layout.FindField(10)->presence_bit_index, 0U);
  KV_INDEX_CHECK_EQ(layout.FindField(20)->presence_bit_index, 1U);
  KV_INDEX_CHECK_EQ(layout.FindField(30)->presence_bit_index, 2U);
  KV_INDEX_CHECK_EQ(layout.FindField(40)->presence_bit_index, 3U);
  KV_INDEX_CHECK_EQ(layout.FindField(10)->slot_offset, 8U);
  KV_INDEX_CHECK_EQ(layout.FindField(30)->slot_offset, 16U);
  KV_INDEX_CHECK_EQ(layout.ref_area_offset(), 24U);
  KV_INDEX_CHECK_EQ(layout.FindField(20)->slot_offset, 24U);
  KV_INDEX_CHECK_EQ(layout.FindField(40)->slot_offset, 40U);
  KV_INDEX_CHECK_EQ(layout.row_slot_size(), 56U);

  const auto compiled_again = CompiledRowLayout::Compile(schema);
  KV_INDEX_CHECK(compiled_again.ok());
  KV_INDEX_CHECK_EQ(layout.layout_fingerprint(),
                    compiled_again->layout_fingerprint());

  RuntimeSchema next_version(43);
  KV_INDEX_CHECK(
      next_version.AddField(Scalar(30, "wide", FieldType::kInt64)).ok());
  KV_INDEX_CHECK(
      next_version.AddField(Scalar(10, "tiny", FieldType::kInt8)).ok());
  KV_INDEX_CHECK(
      next_version.AddField(Scalar(20, "name", FieldType::kString)).ok());
  KV_INDEX_CHECK(
      next_version.AddField(List(40, "tags", FieldType::kUInt64)).ok());
  const auto next_layout = CompiledRowLayout::Compile(next_version);
  KV_INDEX_CHECK(next_layout.ok());
  KV_INDEX_CHECK_NE(layout.layout_fingerprint(),
                    next_layout->layout_fingerprint());
}

void DeletedFieldsDoNotAllocateRowStorage() {
  RuntimeSchema schema(9);
  KV_INDEX_CHECK(schema.AddField(Scalar(1, "live", FieldType::kUInt64)).ok());
  KV_INDEX_CHECK(
      schema.AddField(Scalar(2, "deleted", FieldType::kInt32)).ok());
  KV_INDEX_CHECK(schema.DeleteField(2).ok());

  const auto compiled = CompiledRowLayout::Compile(schema);
  KV_INDEX_CHECK(compiled.ok());
  KV_INDEX_CHECK_EQ(compiled->fields().size(), 1U);
  KV_INDEX_CHECK(compiled->FindField(1) != nullptr);
  KV_INDEX_CHECK(compiled->FindField(2) == nullptr);
}

}  // namespace

int main() {
  RuntimeSchemaTracksActiveAndDeletedFields();
  RuntimeSchemaRejectsInvalidPoliciesAndEvolution();
  FieldEncodingConfigUsesUnifiedDictionarySpelling();
  CompiledLayoutIsDeterministicAndAligned();
  DeletedFieldsDoNotAllocateRowStorage();
  return 0;
}
