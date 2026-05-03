#include "src/offline_artifact_builder.h"

#include <arrow/api.h>
#include <parquet/arrow/reader.h>

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <dirent.h>
#include <fstream>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "kv_index/forward_index.h"
#include "kv_index/schema.h"
#include "src/artifact/artifact_writer.h"
#include "src/model/row_storage.h"

namespace kv_index::offline {
namespace {

namespace model = kv_index::model;

using FieldValue = FieldDefaultValue;

struct ParsedField {
  FieldSpec field;
  std::string encoding_name;
  bool has_field_id = false;
  bool has_name = false;
  bool has_type = false;
  bool has_is_list = false;
  bool has_nullable = false;
  bool has_encoding = false;
};

struct ParsedSchemaConfig {
  std::uint64_t schema_version = 0;
  bool has_schema_version = false;
  std::optional<std::uint32_t> shard_count;
  std::optional<std::uint64_t> hash_seed;
  std::optional<std::uint32_t> hash_version;
  std::string primary_key_name;
  FieldType primary_key_type = FieldType::kUInt64;
  bool has_primary_key_name = false;
  bool has_primary_key_type = false;
  std::vector<FieldSpec> fields;
};

std::string Trim(std::string_view text) {
  std::size_t begin = 0;
  while (begin < text.size() &&
         (text[begin] == ' ' || text[begin] == '\t' || text[begin] == '\r')) {
    ++begin;
  }
  std::size_t end = text.size();
  while (end > begin &&
         (text[end - 1] == ' ' || text[end - 1] == '\t' ||
          text[end - 1] == '\r')) {
    --end;
  }
  return std::string(text.substr(begin, end - begin));
}

std::string StripComment(std::string_view line) {
  bool in_single_quote = false;
  bool in_double_quote = false;
  for (std::size_t i = 0; i < line.size(); ++i) {
    const char c = line[i];
    if (c == '\'' && !in_double_quote) {
      in_single_quote = !in_single_quote;
    } else if (c == '"' && !in_single_quote) {
      in_double_quote = !in_double_quote;
    } else if (c == '#' && !in_single_quote && !in_double_quote) {
      return std::string(line.substr(0, i));
    }
  }
  return std::string(line);
}

std::string Unquote(std::string value) {
  if (value.size() >= 2 &&
      ((value.front() == '"' && value.back() == '"') ||
       (value.front() == '\'' && value.back() == '\''))) {
    return value.substr(1, value.size() - 2);
  }
  return value;
}

StatusOr<std::pair<std::string, std::string>> ParseKeyValue(
    std::string_view text) {
  const std::size_t colon = text.find(':');
  if (colon == std::string_view::npos) {
    return Status::InvalidArgument("schema line is not a key-value pair");
  }
  std::string key = Trim(text.substr(0, colon));
  std::string value = Unquote(Trim(text.substr(colon + 1)));
  if (key.empty()) {
    return Status::InvalidArgument("schema key is empty");
  }
  return std::pair<std::string, std::string>{std::move(key), std::move(value)};
}

template <typename T>
StatusOr<T> ParseUnsigned(std::string_view text, std::string_view name) {
  if (text.empty()) {
    return Status::InvalidArgument(std::string(name) + " is empty");
  }
  T value = 0;
  const char* begin = text.data();
  const char* end = text.data() + text.size();
  const auto [ptr, ec] = std::from_chars(begin, end, value);
  if (ec != std::errc() || ptr != end) {
    return Status::InvalidArgument(std::string("invalid unsigned integer for ") +
                                   std::string(name));
  }
  return value;
}

StatusOr<bool> ParseBool(std::string_view text) {
  if (text == "true") {
    return true;
  }
  if (text == "false") {
    return false;
  }
  return Status::InvalidArgument("invalid boolean value");
}

StatusOr<FieldType> ParseType(std::string_view text) {
  if (text == "int8") {
    return FieldType::kInt8;
  }
  if (text == "int32") {
    return FieldType::kInt32;
  }
  if (text == "int64") {
    return FieldType::kInt64;
  }
  if (text == "uint64") {
    return FieldType::kUInt64;
  }
  if (text == "bool") {
    return FieldType::kBool;
  }
  if (text == "string") {
    return FieldType::kString;
  }
  return Status::InvalidArgument("unknown field type");
}

Status ApplyTopLevelKey(ParsedSchemaConfig* config, const std::string& key,
                        const std::string& value) {
  if (key == "schema_version") {
    auto parsed = ParseUnsigned<std::uint64_t>(value, key);
    if (!parsed.ok()) {
      return parsed.status();
    }
    config->schema_version = *parsed;
    config->has_schema_version = true;
    return Status::Ok();
  }
  if (key == "shard_count") {
    auto parsed = ParseUnsigned<std::uint32_t>(value, key);
    if (!parsed.ok()) {
      return parsed.status();
    }
    config->shard_count = *parsed;
    return Status::Ok();
  }
  if (key == "hash_seed") {
    auto parsed = ParseUnsigned<std::uint64_t>(value, key);
    if (!parsed.ok()) {
      return parsed.status();
    }
    config->hash_seed = *parsed;
    return Status::Ok();
  }
  if (key == "hash_version") {
    auto parsed = ParseUnsigned<std::uint32_t>(value, key);
    if (!parsed.ok()) {
      return parsed.status();
    }
    config->hash_version = *parsed;
    return Status::Ok();
  }
  return Status::InvalidArgument("unsupported top-level schema key: " + key);
}

Status ApplyPrimaryKey(ParsedSchemaConfig* config, const std::string& key,
                       const std::string& value) {
  if (key == "name") {
    config->primary_key_name = value;
    config->has_primary_key_name = true;
    return Status::Ok();
  }
  if (key == "type") {
    auto type = ParseType(value);
    if (!type.ok()) {
      return type.status();
    }
    config->primary_key_type = *type;
    config->has_primary_key_type = true;
    return Status::Ok();
  }
  return Status::InvalidArgument("unsupported primary_key schema key: " + key);
}

Status ApplyFieldKey(ParsedField* field, const std::string& key,
                     const std::string& value) {
  if (key == "field_id") {
    auto parsed = ParseUnsigned<FieldId>(value, key);
    if (!parsed.ok()) {
      return parsed.status();
    }
    field->field.field_id = *parsed;
    field->has_field_id = true;
    return Status::Ok();
  }
  if (key == "name") {
    field->field.name = value;
    field->has_name = true;
    return Status::Ok();
  }
  if (key == "type") {
    auto type = ParseType(value);
    if (!type.ok()) {
      return type.status();
    }
    field->field.type = *type;
    field->has_type = true;
    return Status::Ok();
  }
  if (key == "is_list") {
    auto parsed = ParseBool(value);
    if (!parsed.ok()) {
      return parsed.status();
    }
    field->field.is_list = *parsed;
    field->has_is_list = true;
    return Status::Ok();
  }
  if (key == "nullable") {
    auto parsed = ParseBool(value);
    if (!parsed.ok()) {
      return parsed.status();
    }
    field->field.nullable = *parsed;
    field->has_nullable = true;
    return Status::Ok();
  }
  if (key == "encoding") {
    field->encoding_name = value;
    field->has_encoding = true;
    return Status::Ok();
  }
  return Status::InvalidArgument("unsupported field schema key: " + key);
}

Status FlushParsedField(ParsedField* pending, bool* has_pending,
                        ParsedSchemaConfig* config) {
  if (!*has_pending) {
    return Status::Ok();
  }
  if (!pending->has_field_id || !pending->has_name || !pending->has_type ||
      !pending->has_is_list || !pending->has_nullable ||
      !pending->has_encoding) {
    return Status::InvalidArgument("schema field is missing required keys");
  }
  auto encoding = ParseFieldEncoding(pending->encoding_name,
                                     pending->field.is_list,
                                     pending->field.type);
  if (!encoding.ok()) {
    return encoding.status();
  }
  pending->field.encoding = *encoding;
  config->fields.push_back(std::move(pending->field));
  *pending = ParsedField{};
  *has_pending = false;
  return Status::Ok();
}

StatusOr<ParsedSchemaConfig> ParseSchemaFile(const std::string& path) {
  std::ifstream input(path);
  if (!input) {
    return Status::NotFound("schema file not found");
  }

  enum class Section {
    kTopLevel,
    kPrimaryKey,
    kFields,
  };

  ParsedSchemaConfig config;
  Section section = Section::kTopLevel;
  ParsedField pending_field;
  bool has_pending_field = false;

  std::string raw_line;
  std::uint64_t line_number = 0;
  while (std::getline(input, raw_line)) {
    ++line_number;
    const std::string without_comment = StripComment(raw_line);
    const std::string trimmed = Trim(without_comment);
    if (trimmed.empty()) {
      continue;
    }

    std::size_t indent = 0;
    while (indent < without_comment.size() && without_comment[indent] == ' ') {
      ++indent;
    }

    Status status = Status::Ok();
    if (indent == 0) {
      status = FlushParsedField(&pending_field, &has_pending_field, &config);
      if (!status.ok()) {
        return status;
      }
      if (trimmed == "primary_key:") {
        section = Section::kPrimaryKey;
        continue;
      }
      if (trimmed == "fields:") {
        section = Section::kFields;
        continue;
      }
      auto key_value = ParseKeyValue(trimmed);
      if (!key_value.ok()) {
        return Status::InvalidArgument("invalid schema line " +
                                       std::to_string(line_number) + ": " +
                                       key_value.status().message());
      }
      section = Section::kTopLevel;
      status = ApplyTopLevelKey(&config, key_value->first, key_value->second);
    } else if (section == Section::kPrimaryKey) {
      auto key_value = ParseKeyValue(trimmed);
      if (!key_value.ok()) {
        return Status::InvalidArgument("invalid primary_key line " +
                                       std::to_string(line_number) + ": " +
                                       key_value.status().message());
      }
      status = ApplyPrimaryKey(&config, key_value->first, key_value->second);
    } else if (section == Section::kFields) {
      if (trimmed.rfind("- ", 0) == 0) {
        status = FlushParsedField(&pending_field, &has_pending_field, &config);
        if (!status.ok()) {
          return status;
        }
        has_pending_field = true;
        const std::string first_key = Trim(std::string_view(trimmed).substr(2));
        if (!first_key.empty()) {
          auto key_value = ParseKeyValue(first_key);
          if (!key_value.ok()) {
            return Status::InvalidArgument("invalid field line " +
                                           std::to_string(line_number) + ": " +
                                           key_value.status().message());
          }
          status = ApplyFieldKey(&pending_field, key_value->first,
                                 key_value->second);
        }
      } else {
        if (!has_pending_field) {
          return Status::InvalidArgument(
              "field property appeared before a field item");
        }
        auto key_value = ParseKeyValue(trimmed);
        if (!key_value.ok()) {
          return Status::InvalidArgument("invalid field line " +
                                         std::to_string(line_number) + ": " +
                                         key_value.status().message());
        }
        status =
            ApplyFieldKey(&pending_field, key_value->first, key_value->second);
      }
    } else {
      return Status::InvalidArgument("schema line is in an unknown section");
    }

    if (!status.ok()) {
      return Status::InvalidArgument("invalid schema line " +
                                     std::to_string(line_number) + ": " +
                                     status.message());
    }
  }

  if (const Status status =
          FlushParsedField(&pending_field, &has_pending_field, &config);
      !status.ok()) {
    return status;
  }
  if (!config.has_schema_version) {
    return Status::InvalidArgument("schema_version is required");
  }
  if (!config.has_primary_key_name || !config.has_primary_key_type) {
    return Status::InvalidArgument("primary_key name and type are required");
  }
  if (config.primary_key_type != FieldType::kUInt64) {
    return Status::InvalidArgument("primary_key type must be uint64");
  }
  if (config.fields.empty()) {
    return Status::InvalidArgument("schema must contain at least one field");
  }
  return config;
}

StatusOr<RuntimeSchema> BuildRuntimeSchema(const ParsedSchemaConfig& config,
                                           std::uint64_t schema_version) {
  RuntimeSchema schema(schema_version);
  for (FieldSpec field : config.fields) {
    if (const Status status = schema.AddField(std::move(field)); !status.ok()) {
      return status;
    }
  }
  return schema;
}

std::string FieldTypeName(FieldType type) {
  switch (type) {
    case FieldType::kInt8:
      return "int8";
    case FieldType::kInt32:
      return "int32";
    case FieldType::kInt64:
      return "int64";
    case FieldType::kUInt64:
      return "uint64";
    case FieldType::kBool:
      return "bool";
    case FieldType::kString:
      return "string";
  }
  return "unknown";
}

Status WriteSchemaFile(const std::string& path, const ParsedSchemaConfig& config,
                       std::uint64_t schema_version,
                       std::uint32_t shard_count, std::uint64_t hash_seed,
                       std::uint32_t hash_version) {
  std::ofstream out(path, std::ios::trunc);
  if (!out) {
    return Status::Unavailable("failed to open output schema file");
  }
  out << "schema_version: " << schema_version << "\n";
  out << "shard_count: " << shard_count << "\n";
  out << "hash_seed: " << hash_seed << "\n";
  out << "hash_version: " << hash_version << "\n\n";
  out << "primary_key:\n";
  out << "  name: " << config.primary_key_name << "\n";
  out << "  type: uint64\n\n";
  out << "fields:\n";
  for (const FieldSpec& field : config.fields) {
    out << "  - field_id: " << field.field_id << "\n";
    out << "    name: " << field.name << "\n";
    out << "    type: " << FieldTypeName(field.type) << "\n";
    out << "    is_list: " << (field.is_list ? "true" : "false") << "\n";
    out << "    nullable: " << (field.nullable ? "true" : "false") << "\n";
    out << "    encoding: " << FieldEncodingConfigName(field.encoding) << "\n\n";
  }
  if (!out) {
    return Status::Unavailable("failed to write output schema file");
  }
  return Status::Ok();
}

std::string JoinPath(const std::string& lhs, const std::string& rhs) {
  if (lhs.empty() || lhs == "/") {
    return lhs + rhs;
  }
  return lhs + "/" + rhs;
}

bool EndsWith(std::string_view text, std::string_view suffix) {
  return text.size() >= suffix.size() &&
         text.substr(text.size() - suffix.size()) == suffix;
}

Status MakeDirs(const std::string& path) {
  if (path.empty()) {
    return Status::InvalidArgument("directory path is empty");
  }
  std::string current;
  std::size_t index = 0;
  if (path[0] == '/') {
    current = "/";
    index = 1;
  }
  while (index <= path.size()) {
    const std::size_t slash = path.find('/', index);
    const std::string part = path.substr(
        index, slash == std::string::npos ? std::string::npos : slash - index);
    if (!part.empty()) {
      current = current == "/" ? current + part : JoinPath(current, part);
      if (mkdir(current.c_str(), 0755) != 0 && errno != EEXIST) {
        return Status::Unavailable(std::string("failed to create directory: ") +
                                   std::strerror(errno));
      }
      struct stat statbuf {};
      if (stat(current.c_str(), &statbuf) != 0 || !S_ISDIR(statbuf.st_mode)) {
        return Status::InvalidArgument("output path component is not a directory");
      }
    }
    if (slash == std::string::npos) {
      break;
    }
    index = slash + 1;
  }
  return Status::Ok();
}

Status CollectParquetFilesRecursive(const std::string& path,
                                    std::vector<std::string>* files) {
  struct stat statbuf {};
  if (stat(path.c_str(), &statbuf) != 0) {
    return Status::NotFound("input path does not exist");
  }
  if (S_ISREG(statbuf.st_mode)) {
    if (EndsWith(path, ".parquet")) {
      files->push_back(path);
    }
    return Status::Ok();
  }
  if (!S_ISDIR(statbuf.st_mode)) {
    return Status::InvalidArgument("input path is neither file nor directory");
  }

  DIR* dir = opendir(path.c_str());
  if (dir == nullptr) {
    return Status::Unavailable(std::string("failed to open input directory: ") +
                               std::strerror(errno));
  }
  std::vector<std::string> children;
  while (dirent* entry = readdir(dir)) {
    const std::string name = entry->d_name;
    if (name == "." || name == ".." || name.empty() || name[0] == '.') {
      continue;
    }
    children.push_back(JoinPath(path, name));
  }
  closedir(dir);
  std::sort(children.begin(), children.end());
  for (const std::string& child : children) {
    if (const Status status = CollectParquetFilesRecursive(child, files);
        !status.ok()) {
      return status;
    }
  }
  return Status::Ok();
}

StatusOr<std::vector<std::string>> CollectParquetFiles(
    const std::string& input_path) {
  std::vector<std::string> files;
  if (const Status status = CollectParquetFilesRecursive(input_path, &files);
      !status.ok()) {
    return status;
  }
  if (files.empty()) {
    return Status::NotFound("no parquet files found under input path");
  }
  return files;
}

std::string ShardFileName(std::uint32_t shard_id) {
  std::ostringstream out;
  out << "shard_";
  out.width(5);
  out.fill('0');
  out << shard_id << ".kvi";
  return out.str();
}

Status ArrowStatusToKv(const arrow::Status& status,
                       const std::string& context) {
  if (status.ok()) {
    return Status::Ok();
  }
  return Status::InvalidArgument(context + ": " + status.ToString());
}

template <typename ArrowArray>
StatusOr<const ArrowArray*> CheckedArray(const arrow::Array& array,
                                         arrow::Type::type expected,
                                         std::string_view field_name) {
  if (array.type_id() != expected) {
    return Status::InvalidArgument("parquet column has wrong type for field " +
                                   std::string(field_name));
  }
  return static_cast<const ArrowArray*>(&array);
}

StatusOr<std::pair<const arrow::Array*, std::int64_t>> LocateArray(
    const std::shared_ptr<arrow::ChunkedArray>& column, std::int64_t row_index) {
  std::int64_t offset = 0;
  for (const std::shared_ptr<arrow::Array>& chunk : column->chunks()) {
    if (row_index < offset + chunk->length()) {
      return std::pair<const arrow::Array*, std::int64_t>{
          chunk.get(), row_index - offset};
    }
    offset += chunk->length();
  }
  return Status::InvalidArgument("row index is out of parquet column bounds");
}

StatusOr<std::uint64_t> ReadPrimaryKey(
    const std::shared_ptr<arrow::ChunkedArray>& column, std::int64_t row_index,
    const std::string& name) {
  auto located = LocateArray(column, row_index);
  if (!located.ok()) {
    return located.status();
  }
  const arrow::Array* array = located->first;
  const std::int64_t local = located->second;
  if (array->IsNull(local)) {
    return Status::InvalidArgument("primary key column contains null: " + name);
  }
  if (array->type_id() == arrow::Type::UINT64) {
    return static_cast<const arrow::UInt64Array*>(array)->Value(local);
  }
  if (array->type_id() == arrow::Type::INT64) {
    const std::int64_t value =
        static_cast<const arrow::Int64Array*>(array)->Value(local);
    if (value < 0) {
      return Status::InvalidArgument("primary key column contains negative value");
    }
    return static_cast<std::uint64_t>(value);
  }
  return Status::InvalidArgument("primary key column must be uint64 or int64");
}

template <typename Out, typename ArrowArray>
StatusOr<Out> ReadIntegerValue(const arrow::Array& array, std::int64_t index,
                               arrow::Type::type expected,
                               std::string_view field_name) {
  auto typed = CheckedArray<ArrowArray>(array, expected, field_name);
  if (!typed.ok()) {
    return typed.status();
  }
  const auto raw = (*typed)->Value(index);
  if constexpr (std::is_signed_v<decltype(raw)> == std::is_signed_v<Out>) {
    if (raw < static_cast<decltype(raw)>(std::numeric_limits<Out>::min()) ||
        raw > static_cast<decltype(raw)>(std::numeric_limits<Out>::max())) {
      return Status::InvalidArgument("integer value is out of range");
    }
  }
  return static_cast<Out>(raw);
}

StatusOr<std::string> ReadStringValue(const arrow::Array& array,
                                      std::int64_t index,
                                      std::string_view field_name) {
  if (array.type_id() == arrow::Type::STRING) {
    return static_cast<const arrow::StringArray&>(array).GetString(index);
  }
  if (array.type_id() == arrow::Type::LARGE_STRING) {
    return static_cast<const arrow::LargeStringArray&>(array).GetString(index);
  }
  return Status::InvalidArgument("parquet column has wrong type for field " +
                                 std::string(field_name));
}

StatusOr<FieldValue> ReadScalarValue(const FieldLayout& field,
                                     const arrow::Array& array,
                                     std::int64_t index) {
  switch (field.type) {
    case FieldType::kInt8: {
      auto value = ReadIntegerValue<std::int8_t, arrow::Int8Array>(
          array, index, arrow::Type::INT8, field.name);
      if (!value.ok()) {
        return value.status();
      }
      return FieldValue(*value);
    }
    case FieldType::kInt32: {
      auto value = ReadIntegerValue<std::int32_t, arrow::Int32Array>(
          array, index, arrow::Type::INT32, field.name);
      if (!value.ok()) {
        return value.status();
      }
      return FieldValue(*value);
    }
    case FieldType::kInt64: {
      auto value = ReadIntegerValue<std::int64_t, arrow::Int64Array>(
          array, index, arrow::Type::INT64, field.name);
      if (!value.ok()) {
        return value.status();
      }
      return FieldValue(*value);
    }
    case FieldType::kUInt64: {
      auto value = ReadIntegerValue<std::uint64_t, arrow::UInt64Array>(
          array, index, arrow::Type::UINT64, field.name);
      if (!value.ok()) {
        return value.status();
      }
      return FieldValue(*value);
    }
    case FieldType::kBool: {
      auto typed = CheckedArray<arrow::BooleanArray>(
          array, arrow::Type::BOOL, field.name);
      if (!typed.ok()) {
        return typed.status();
      }
      return (*typed)->Value(index);
    }
    case FieldType::kString: {
      auto value = ReadStringValue(array, index, field.name);
      if (!value.ok()) {
        return value.status();
      }
      return FieldValue(std::move(value).value());
    }
  }
  return Status::InvalidArgument("unsupported scalar field type");
}

template <typename Out, typename ArrowArray>
StatusOr<std::vector<Out>> ReadIntegerList(const arrow::Array& values,
                                           std::int64_t start,
                                           std::int64_t length,
                                           arrow::Type::type expected,
                                           std::string_view field_name) {
  auto typed = CheckedArray<ArrowArray>(values, expected, field_name);
  if (!typed.ok()) {
    return typed.status();
  }
  std::vector<Out> result;
  result.reserve(static_cast<std::size_t>(length));
  for (std::int64_t i = 0; i < length; ++i) {
    const std::int64_t index = start + i;
    if (values.IsNull(index)) {
      return Status::InvalidArgument("list field contains null element");
    }
    const auto raw = (*typed)->Value(index);
    result.push_back(static_cast<Out>(raw));
  }
  return result;
}

StatusOr<FieldValue> ReadListValuesByType(const FieldLayout& field,
                                          const arrow::Array& values,
                                          std::int64_t start,
                                          std::int64_t length) {
  switch (field.type) {
    case FieldType::kInt8: {
      auto value = ReadIntegerList<std::int8_t, arrow::Int8Array>(
          values, start, length, arrow::Type::INT8, field.name);
      if (!value.ok()) {
        return value.status();
      }
      return FieldValue(std::move(value).value());
    }
    case FieldType::kInt32: {
      auto value = ReadIntegerList<std::int32_t, arrow::Int32Array>(
          values, start, length, arrow::Type::INT32, field.name);
      if (!value.ok()) {
        return value.status();
      }
      return FieldValue(std::move(value).value());
    }
    case FieldType::kInt64: {
      auto value = ReadIntegerList<std::int64_t, arrow::Int64Array>(
          values, start, length, arrow::Type::INT64, field.name);
      if (!value.ok()) {
        return value.status();
      }
      return FieldValue(std::move(value).value());
    }
    case FieldType::kUInt64: {
      auto value = ReadIntegerList<std::uint64_t, arrow::UInt64Array>(
          values, start, length, arrow::Type::UINT64, field.name);
      if (!value.ok()) {
        return value.status();
      }
      return FieldValue(std::move(value).value());
    }
    case FieldType::kBool: {
      auto typed =
          CheckedArray<arrow::BooleanArray>(values, arrow::Type::BOOL, field.name);
      if (!typed.ok()) {
        return typed.status();
      }
      std::vector<bool> result;
      result.reserve(static_cast<std::size_t>(length));
      for (std::int64_t i = 0; i < length; ++i) {
        const std::int64_t index = start + i;
        if (values.IsNull(index)) {
          return Status::InvalidArgument("list field contains null element");
        }
        result.push_back((*typed)->Value(index));
      }
      return result;
    }
    case FieldType::kString: {
      std::vector<std::string> result;
      result.reserve(static_cast<std::size_t>(length));
      for (std::int64_t i = 0; i < length; ++i) {
        const std::int64_t index = start + i;
        if (values.IsNull(index)) {
          return Status::InvalidArgument("list field contains null element");
        }
        auto value = ReadStringValue(values, index, field.name);
        if (!value.ok()) {
          return value.status();
        }
        result.push_back(std::move(value).value());
      }
      return result;
    }
  }
  return Status::InvalidArgument("unsupported list field type");
}

StatusOr<FieldValue> ReadListValue(const FieldLayout& field,
                                   const arrow::Array& array,
                                   std::int64_t index) {
  if (array.type_id() == arrow::Type::LIST) {
    const auto& list = static_cast<const arrow::ListArray&>(array);
    return ReadListValuesByType(field, *list.values(), list.value_offset(index),
                                list.value_length(index));
  }
  if (array.type_id() == arrow::Type::LARGE_LIST) {
    const auto& list = static_cast<const arrow::LargeListArray&>(array);
    return ReadListValuesByType(field, *list.values(), list.value_offset(index),
                                list.value_length(index));
  }
  return Status::InvalidArgument("parquet column has wrong list type for field " +
                                 field.name);
}

StatusOr<FieldValue> ReadFieldValue(
    const FieldLayout& field,
    const std::shared_ptr<arrow::ChunkedArray>& column, std::int64_t row_index) {
  auto located = LocateArray(column, row_index);
  if (!located.ok()) {
    return located.status();
  }
  const arrow::Array& array = *located->first;
  const std::int64_t local = located->second;
  if (array.IsNull(local)) {
    if (field.nullable) {
      return FieldValue{};
    }
    return Status::InvalidArgument("required field contains null: " + field.name);
  }
  if (field.is_list) {
    return ReadListValue(field, array, local);
  }
  return ReadScalarValue(field, array, local);
}

template <typename T>
void AppendLittleEndian(std::vector<std::byte>* bytes, T value) {
  using U = std::conditional_t<std::is_same_v<T, bool>, std::uint8_t, T>;
  const U encoded =
      std::is_same_v<T, bool> ? static_cast<U>(value ? 1 : 0)
                              : static_cast<U>(value);
  for (std::size_t i = 0; i < sizeof(U); ++i) {
    bytes->push_back(static_cast<std::byte>(
        (static_cast<std::make_unsigned_t<U>>(encoded) >> (i * 8U)) & 0xffU));
  }
}

void WriteLittleEndianAt(std::vector<std::byte>* bytes, std::size_t offset,
                         std::uint64_t value) {
  for (std::size_t i = 0; i < sizeof(value); ++i) {
    (*bytes)[offset + i] =
        static_cast<std::byte>((value >> (i * 8U)) & 0xffU);
  }
}

void WriteLittleEndianAt(std::vector<std::byte>* bytes, std::size_t offset,
                         std::uint32_t value) {
  for (std::size_t i = 0; i < sizeof(value); ++i) {
    (*bytes)[offset + i] =
        static_cast<std::byte>((value >> (i * 8U)) & 0xffU);
  }
}

void WriteValueRefBytes(std::vector<std::byte>* bytes, std::size_t offset,
                        std::uint64_t ref_offset, std::uint32_t byte_length,
                        std::uint32_t element_count_or_flags) {
  WriteLittleEndianAt(bytes, offset, ref_offset);
  WriteLittleEndianAt(bytes, offset + sizeof(std::uint64_t), byte_length);
  WriteLittleEndianAt(bytes,
                      offset + sizeof(std::uint64_t) + sizeof(std::uint32_t),
                      element_count_or_flags);
}

template <typename T>
Status WriteScalarListArena(const FieldLayout& field,
                            const std::vector<T>& values,
                            model::EncodedRow* encoded) {
  const std::uint64_t offset = encoded->arena.size();
  for (const T& value : values) {
    AppendLittleEndian<T>(&encoded->arena, value);
  }
  const std::uint64_t byte_length64 = encoded->arena.size() - offset;
  if (byte_length64 > std::numeric_limits<std::uint32_t>::max() ||
      values.size() > std::numeric_limits<std::uint32_t>::max()) {
    return Status::InvalidArgument("list value is too large");
  }
  return model::WriteValueRefField(
      field,
      base::ValueRef16{
          .offset = offset,
          .byte_length = static_cast<std::uint32_t>(byte_length64),
          .element_count_or_flags = static_cast<std::uint32_t>(values.size()),
      },
      encoded);
}

Status WriteStringListArena(const FieldLayout& field,
                            const std::vector<std::string>& values,
                            model::EncodedRow* encoded) {
  if (values.size() > std::numeric_limits<std::uint32_t>::max()) {
    return Status::InvalidArgument("string list is too large");
  }
  const std::uint64_t ref_offset = encoded->arena.size();
  const std::size_t ref_bytes = values.size() * 16U;
  encoded->arena.resize(encoded->arena.size() + ref_bytes);
  for (std::size_t i = 0; i < values.size(); ++i) {
    const std::string& value = values[i];
    if (value.size() > std::numeric_limits<std::uint32_t>::max()) {
      return Status::InvalidArgument("string list element is too large");
    }
    const std::uint64_t element_offset = encoded->arena.size();
    encoded->arena.insert(
        encoded->arena.end(), reinterpret_cast<const std::byte*>(value.data()),
        reinterpret_cast<const std::byte*>(value.data()) + value.size());
    WriteValueRefBytes(&encoded->arena,
                       static_cast<std::size_t>(ref_offset) + i * 16U,
                       element_offset,
                       static_cast<std::uint32_t>(value.size()), 0);
  }
  if (ref_bytes > std::numeric_limits<std::uint32_t>::max()) {
    return Status::InvalidArgument("string list ref payload is too large");
  }
  return model::WriteValueRefField(
      field,
      base::ValueRef16{
          .offset = ref_offset,
          .byte_length = static_cast<std::uint32_t>(ref_bytes),
          .element_count_or_flags = static_cast<std::uint32_t>(values.size()),
      },
      encoded);
}

Status WriteScalarFieldValue(const FieldLayout& field, const FieldValue& value,
                             model::EncodedRow* encoded) {
  switch (field.type) {
    case FieldType::kInt8:
      return model::WriteScalarField(field, std::get<std::int8_t>(value),
                                     encoded);
    case FieldType::kInt32:
      return model::WriteScalarField(field, std::get<std::int32_t>(value),
                                     encoded);
    case FieldType::kInt64:
      return model::WriteScalarField(field, std::get<std::int64_t>(value),
                                     encoded);
    case FieldType::kUInt64:
      return model::WriteScalarField(field, std::get<std::uint64_t>(value),
                                     encoded);
    case FieldType::kBool:
      return model::WriteScalarField(field, std::get<bool>(value), encoded);
    case FieldType::kString: {
      const std::string& string_value = std::get<std::string>(value);
      if (field.encoding == FieldEncoding::kArena) {
        return model::WriteArenaStringField(field, string_value, encoded);
      }
      if (field.encoding == FieldEncoding::kDictionary) {
        if (encoded->string_dictionary.size() >
            std::numeric_limits<std::uint32_t>::max()) {
          return Status::InvalidArgument("string dictionary is too large");
        }
        const std::uint32_t id =
            static_cast<std::uint32_t>(encoded->string_dictionary.size());
        encoded->string_dictionary.push_back(string_value);
        return model::WriteDictionaryStringField(field, id, encoded);
      }
      break;
    }
  }
  return Status::InvalidArgument("unsupported scalar field encoding");
}

Status WriteListFieldValue(const FieldLayout& field, const FieldValue& value,
                           model::EncodedRow* encoded) {
  if (field.encoding == FieldEncoding::kArena) {
    switch (field.type) {
      case FieldType::kInt8:
        return WriteScalarListArena(field,
                                    std::get<std::vector<std::int8_t>>(value),
                                    encoded);
      case FieldType::kInt32:
        return WriteScalarListArena(field,
                                    std::get<std::vector<std::int32_t>>(value),
                                    encoded);
      case FieldType::kInt64:
        return WriteScalarListArena(field,
                                    std::get<std::vector<std::int64_t>>(value),
                                    encoded);
      case FieldType::kUInt64:
        return WriteScalarListArena(
            field, std::get<std::vector<std::uint64_t>>(value), encoded);
      case FieldType::kBool:
        return WriteScalarListArena(field, std::get<std::vector<bool>>(value),
                                    encoded);
      case FieldType::kString:
        return WriteStringListArena(
            field, std::get<std::vector<std::string>>(value), encoded);
    }
  }

  if (field.encoding == FieldEncoding::kListDictionary) {
    if (field.type == FieldType::kString) {
      const auto& values = std::get<std::vector<std::string>>(value);
      if (encoded->string_list_dictionary.size() >
              std::numeric_limits<std::uint32_t>::max() ||
          values.size() > std::numeric_limits<std::uint32_t>::max()) {
        return Status::InvalidArgument("string-list dictionary is too large");
      }
      const std::uint32_t id =
          static_cast<std::uint32_t>(encoded->string_list_dictionary.size());
      encoded->string_list_dictionary.push_back(values);
      return model::WriteListDictionaryField(
          field, id, static_cast<std::uint32_t>(values.size()), encoded);
    }

    std::vector<std::byte> bytes;
    std::uint32_t element_count = 0;
    std::visit(
        [&](const auto& typed) {
          using T = std::decay_t<decltype(typed)>;
          if constexpr (std::is_same_v<T, std::vector<std::int8_t>> ||
                        std::is_same_v<T, std::vector<std::int32_t>> ||
                        std::is_same_v<T, std::vector<std::int64_t>> ||
                        std::is_same_v<T, std::vector<std::uint64_t>> ||
                        std::is_same_v<T, std::vector<bool>>) {
            element_count = static_cast<std::uint32_t>(typed.size());
            for (const auto& element : typed) {
              AppendLittleEndian<typename T::value_type>(&bytes, element);
            }
          }
        },
        value);
    if (encoded->scalar_list_dictionary.size() >
        std::numeric_limits<std::uint32_t>::max()) {
      return Status::InvalidArgument("scalar-list dictionary is too large");
    }
    const std::uint32_t id =
        static_cast<std::uint32_t>(encoded->scalar_list_dictionary.size());
    encoded->scalar_list_dictionary.push_back(std::move(bytes));
    return model::WriteListDictionaryField(field, id, element_count, encoded);
  }

  if (field.encoding == FieldEncoding::kElementDictionary &&
      field.type == FieldType::kString) {
    const auto& values = std::get<std::vector<std::string>>(value);
    if (values.size() > std::numeric_limits<std::uint32_t>::max()) {
      return Status::InvalidArgument("element dictionary list is too large");
    }
    std::vector<std::uint32_t> ids;
    ids.reserve(values.size());
    for (const std::string& string_value : values) {
      if (encoded->string_element_dictionary.size() >
          std::numeric_limits<std::uint32_t>::max()) {
        return Status::InvalidArgument("element dictionary is too large");
      }
      ids.push_back(
          static_cast<std::uint32_t>(encoded->string_element_dictionary.size()));
      encoded->string_element_dictionary.push_back(string_value);
    }
    return model::WriteElementDictionaryStringListField(field, ids, encoded);
  }

  return Status::InvalidArgument("unsupported list field encoding");
}

StatusOr<model::EncodedRow> EncodeRow(
    const CompiledRowLayout& layout,
    const std::unordered_map<FieldId, FieldValue>& values) {
  model::EncodedRow encoded = model::CreateEncodedRow(layout);
  for (const FieldLayout& field : layout.fields()) {
    const auto it = values.find(field.field_id);
    if (it == values.end() ||
        std::holds_alternative<std::monostate>(it->second)) {
      if (!field.nullable && !field.HasDefault()) {
        return Status::InvalidArgument("required field is absent: " + field.name);
      }
      continue;
    }
    const Status status = field.is_list
                              ? WriteListFieldValue(field, it->second, &encoded)
                              : WriteScalarFieldValue(field, it->second, &encoded);
    if (!status.ok()) {
      return status;
    }
  }
  return encoded;
}

struct TableColumns {
  std::shared_ptr<arrow::ChunkedArray> primary_key;
  std::unordered_map<FieldId, std::shared_ptr<arrow::ChunkedArray>> fields;
};

StatusOr<TableColumns> ResolveTableColumns(
    const arrow::Table& table, const ParsedSchemaConfig& config,
    const CompiledRowLayout& layout) {
  TableColumns columns;
  columns.primary_key = table.GetColumnByName(config.primary_key_name);
  if (columns.primary_key == nullptr) {
    return Status::InvalidArgument("parquet table is missing primary key column");
  }
  for (const FieldLayout& field : layout.fields()) {
    auto column = table.GetColumnByName(field.name);
    if (column == nullptr) {
      if (!field.nullable && !field.HasDefault()) {
        return Status::InvalidArgument("parquet table is missing required field: " +
                                       field.name);
      }
      continue;
    }
    columns.fields.emplace(field.field_id, std::move(column));
  }
  return columns;
}

Status ReadParquetFileIntoSpec(
    const std::string& path, const ParsedSchemaConfig& schema_config,
    const std::shared_ptr<const CompiledRowLayout>& layout,
    artifact::ArtifactBuildSpec* spec, std::unordered_set<std::uint64_t>* seen,
    BuildArtifactResult* result) {
  parquet::arrow::FileReaderBuilder builder;
  if (const arrow::Status status = builder.OpenFile(path); !status.ok()) {
    return ArrowStatusToKv(status, "failed to open parquet file");
  }
  builder.memory_pool(arrow::default_memory_pool());
  std::unique_ptr<parquet::arrow::FileReader> reader;
  if (const arrow::Status status = builder.Build(&reader); !status.ok()) {
    return ArrowStatusToKv(status, "failed to create parquet reader");
  }
  auto table_result = reader->ReadTable();
  if (!table_result.ok()) {
    return ArrowStatusToKv(table_result.status(), "failed to read parquet file");
  }
  std::shared_ptr<arrow::Table> table = std::move(table_result).ValueOrDie();
  if (table == nullptr) {
    return Status::InvalidArgument("parquet reader returned a null table");
  }
  auto columns = ResolveTableColumns(*table, schema_config, *layout);
  if (!columns.ok()) {
    return columns.status();
  }

  for (std::int64_t row_index = 0; row_index < table->num_rows(); ++row_index) {
    auto primary_key =
        ReadPrimaryKey(columns->primary_key, row_index,
                       schema_config.primary_key_name);
    if (!primary_key.ok()) {
      return primary_key.status();
    }
    if (!seen->insert(*primary_key).second) {
      return Status::FailedPrecondition("duplicate primary key in full artifact");
    }

    std::unordered_map<FieldId, FieldValue> values;
    for (const FieldLayout& field : layout->fields()) {
      const auto column_it = columns->fields.find(field.field_id);
      if (column_it == columns->fields.end()) {
        continue;
      }
      auto value = ReadFieldValue(field, column_it->second, row_index);
      if (!value.ok()) {
        return value.status();
      }
      values.emplace(field.field_id, std::move(value).value());
    }
    auto encoded = EncodeRow(*layout, values);
    if (!encoded.ok()) {
      return encoded.status();
    }
    const std::uint32_t shard_id = static_cast<std::uint32_t>(
        StableHash64(*primary_key, spec->hash_seed, spec->hash_version) &
        (static_cast<std::uint64_t>(spec->shard_count) - 1ULL));
    spec->shards[shard_id].rows.push_back(artifact::ArtifactRow{
        .primary_key = *primary_key,
        .encoded = std::move(encoded).value(),
    });
    ++result->row_count;
    ++result->rows_per_shard[shard_id];
  }
  return Status::Ok();
}

}  // namespace

StatusOr<BuildArtifactResult> BuildArtifactDirectory(
    const BuildArtifactOptions& options) {
  if (options.schema_path.empty()) {
    return Status::InvalidArgument("schema path is required");
  }
  if (options.input_path.empty()) {
    return Status::InvalidArgument("input path is required");
  }
  if (options.output_path.empty()) {
    return Status::InvalidArgument("output path is required");
  }
  if (options.artifact_id.empty()) {
    return Status::InvalidArgument("artifact id is required");
  }

  auto schema_config = ParseSchemaFile(options.schema_path);
  if (!schema_config.ok()) {
    return schema_config.status();
  }

  const std::uint64_t output_schema_version =
      options.schema_version.value_or(schema_config->schema_version + 1);
  const std::uint32_t shard_count =
      options.shard_count.value_or(schema_config->shard_count.value_or(128));
  const std::uint64_t hash_seed =
      options.hash_seed.value_or(schema_config->hash_seed.value_or(0));
  const std::uint32_t hash_version =
      options.hash_version.value_or(schema_config->hash_version.value_or(1));
  if (!IsPowerOfTwo(shard_count)) {
    return Status::InvalidArgument("shard_count must be a non-zero power of two");
  }
  if (hash_version == 0) {
    return Status::InvalidArgument("hash_version must be non-zero");
  }
  if (output_schema_version == 0) {
    return Status::InvalidArgument("schema_version must be non-zero");
  }

  auto runtime_schema = BuildRuntimeSchema(*schema_config, output_schema_version);
  if (!runtime_schema.ok()) {
    return runtime_schema.status();
  }
  auto compiled_layout = CompiledRowLayout::Compile(*runtime_schema);
  if (!compiled_layout.ok()) {
    return compiled_layout.status();
  }
  auto layout = std::make_shared<const CompiledRowLayout>(
      std::move(compiled_layout).value());

  auto parquet_files = CollectParquetFiles(options.input_path);
  if (!parquet_files.ok()) {
    return parquet_files.status();
  }

  artifact::ArtifactBuildSpec spec;
  spec.artifact_id = options.artifact_id;
  spec.shard_count = shard_count;
  spec.hash_seed = hash_seed;
  spec.hash_version = hash_version;
  spec.layout = layout;
  spec.include_source_progress_section = !options.omit_source_progress;
  spec.shards.resize(shard_count);
  for (std::uint32_t shard_id = 0; shard_id < shard_count; ++shard_id) {
    spec.shards[shard_id].shard_id = shard_id;
  }

  BuildArtifactResult result;
  result.shard_count = shard_count;
  result.hash_seed = hash_seed;
  result.hash_version = hash_version;
  result.schema_version = output_schema_version;
  result.rows_per_shard.assign(shard_count, 0);
  result.parquet_files = *parquet_files;

  std::unordered_set<std::uint64_t> seen_primary_keys;
  for (const std::string& parquet_file : *parquet_files) {
    if (const Status status = ReadParquetFileIntoSpec(
            parquet_file, *schema_config, layout, &spec, &seen_primary_keys,
            &result);
        !status.ok()) {
      return status;
    }
  }

  if (const Status status = MakeDirs(options.output_path); !status.ok()) {
    return status;
  }
  if (const Status status =
          WriteSchemaFile(JoinPath(options.output_path, "schema.yaml"),
                          *schema_config, output_schema_version, shard_count,
                          hash_seed, hash_version);
      !status.ok()) {
    return status;
  }
  for (std::uint32_t shard_id = 0; shard_id < shard_count; ++shard_id) {
    if (const Status status = artifact::WriteArtifactShard(
            JoinPath(options.output_path, ShardFileName(shard_id)), spec,
            shard_id);
        !status.ok()) {
      return status;
    }
  }

  return result;
}

}  // namespace kv_index::offline
