#include "src/ingest/update_applier.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <set>
#include <span>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "src/base/byte_io.h"
#include "src/base/hash.h"
#include "src/model/row_storage.h"

namespace kv_index::ingest {
namespace {

// Kafka upsert payload v1 is a complete-row binary frame:
//   magic "KVIU", uint16 version=1, uint16 flags=0, uint32 field_count,
//   repeated fields of uint32 field_id, uint8 kind, uint32 byte_length, bytes.
// Field bytes are little-endian for scalars. List bytes are uint32 element_count
// followed by scalar elements, or repeated uint32 length + bytes for strings.
// Kafka metadata carries the primary key and consumed message offset; commit
// checkpoints are built later as consumed offset + 1.
constexpr std::byte kMagic0{'K'};
constexpr std::byte kMagic1{'V'};
constexpr std::byte kMagic2{'I'};
constexpr std::byte kMagic3{'U'};
constexpr std::uint16_t kWireVersion = 1;

enum class WireKind : std::uint8_t {
  kNull = 0,
  kInt8 = 1,
  kInt32 = 2,
  kInt64 = 3,
  kUInt64 = 4,
  kBool = 5,
  kString = 6,
  kInt8List = 17,
  kInt32List = 18,
  kInt64List = 19,
  kUInt64List = 20,
  kBoolList = 21,
  kStringList = 22,
};

using DecodedValue =
    std::variant<std::monostate, std::int8_t, std::int32_t, std::int64_t,
                 std::uint64_t, bool, std::string, std::vector<std::int8_t>,
                 std::vector<std::int32_t>, std::vector<std::int64_t>,
                 std::vector<std::uint64_t>, std::vector<bool>,
                 std::vector<std::string>>;

struct DecodedField {
  FieldId field_id = 0;
  WireKind kind = WireKind::kNull;
  DecodedValue value;
};

struct DecodedPayload {
  std::vector<DecodedField> fields;

  const DecodedField* Find(FieldId field_id) const noexcept {
    const auto it = std::find_if(
        fields.begin(), fields.end(),
        [field_id](const DecodedField& field) {
          return field.field_id == field_id;
        });
    if (it == fields.end()) {
      return nullptr;
    }
    return &*it;
  }
};

class ByteReader {
 public:
  explicit ByteReader(std::span<const std::byte> bytes) : bytes_(bytes) {}

  template <typename T>
  StatusOr<T> ReadInteger() {
    if (remaining() < sizeof(T)) {
      return Status::InvalidArgument("payload is truncated");
    }
    auto value = base::ReadLittleEndian<T>(bytes_, offset_);
    if (!value.ok()) {
      return value.status();
    }
    offset_ += sizeof(T);
    return value.value();
  }

  StatusOr<std::uint8_t> ReadUInt8() {
    if (remaining() < 1) {
      return Status::InvalidArgument("payload is truncated");
    }
    const std::uint8_t value =
        static_cast<std::uint8_t>(std::to_integer<unsigned char>(bytes_[offset_]));
    ++offset_;
    return value;
  }

  StatusOr<std::span<const std::byte>> ReadBytes(std::size_t size) {
    if (remaining() < size) {
      return Status::InvalidArgument("payload field is truncated");
    }
    const auto out = bytes_.subspan(offset_, size);
    offset_ += size;
    return out;
  }

  std::size_t remaining() const noexcept { return bytes_.size() - offset_; }

 private:
  std::span<const std::byte> bytes_;
  std::size_t offset_ = 0;
};

bool IsPowerOfTwo(std::size_t value) noexcept {
  return value != 0 && (value & (value - 1)) == 0;
}

Status ValidateOptions(const UpdateApplierOptions& options) {
  if (options.logical_topic.empty()) {
    return Status::InvalidArgument("update applier logical topic is empty");
  }
  if (options.targets.empty()) {
    return Status::FailedPrecondition("update applier has no target routes");
  }
  for (const UpdateTargetRoute& route : options.targets) {
    if (route.layout == nullptr) {
      return Status::FailedPrecondition("update target route has no layout");
    }
    if (route.shard_count == 0 || !IsPowerOfTwo(route.shard_count)) {
      return Status::InvalidArgument(
          "update target shard_count must be a non-zero power of two");
    }
    if (route.realtime_shards.size() != route.shard_count) {
      return Status::InvalidArgument(
          "update target realtime shard count does not match shard_count");
    }
    for (const auto& shard : route.realtime_shards) {
      if (shard == nullptr) {
        return Status::FailedPrecondition(
            "update target route contains a null realtime shard");
      }
    }
  }
  return Status::Ok();
}

Status ValidateMessageMetadata(const KafkaUpsertMessage& message,
                               const std::string& logical_topic) {
  if (message.metadata.partition.topic.empty()) {
    return Status::InvalidArgument("kafka message topic is empty");
  }
  if (message.metadata.partition.topic != logical_topic) {
    return Status::FailedPrecondition(
        "kafka message topic does not match coordinator logical topic");
  }
  if (message.metadata.partition.partition < 0) {
    return Status::InvalidArgument("kafka message partition is invalid");
  }
  if (message.metadata.offset < 0) {
    return Status::InvalidArgument("kafka message offset is invalid");
  }
  return Status::Ok();
}

template <typename T>
StatusOr<T> ReadExactInteger(std::span<const std::byte> bytes) {
  if (bytes.size() != sizeof(T)) {
    return Status::InvalidArgument("payload scalar byte length is invalid");
  }
  return base::ReadLittleEndian<T>(bytes, 0);
}

StatusOr<bool> ReadBool(std::span<const std::byte> bytes) {
  auto value = ReadExactInteger<std::uint8_t>(bytes);
  if (!value.ok()) {
    return value.status();
  }
  if (value.value() > 1) {
    return Status::InvalidArgument("payload bool value is invalid");
  }
  return value.value() != 0;
}

template <typename T>
StatusOr<std::vector<T>> ReadScalarList(std::span<const std::byte> bytes) {
  ByteReader reader(bytes);
  auto count = reader.ReadInteger<std::uint32_t>();
  if (!count.ok()) {
    return count.status();
  }
  const std::size_t expected =
      static_cast<std::size_t>(count.value()) * sizeof(T);
  if (reader.remaining() != expected) {
    return Status::InvalidArgument("payload list byte length is invalid");
  }
  std::vector<T> values;
  values.reserve(count.value());
  for (std::uint32_t i = 0; i < count.value(); ++i) {
    auto value = reader.ReadInteger<T>();
    if (!value.ok()) {
      return value.status();
    }
    values.push_back(value.value());
  }
  return values;
}

StatusOr<std::vector<bool>> ReadBoolList(std::span<const std::byte> bytes) {
  ByteReader reader(bytes);
  auto count = reader.ReadInteger<std::uint32_t>();
  if (!count.ok()) {
    return count.status();
  }
  if (reader.remaining() != count.value()) {
    return Status::InvalidArgument("payload bool-list byte length is invalid");
  }
  std::vector<bool> values;
  values.reserve(count.value());
  for (std::uint32_t i = 0; i < count.value(); ++i) {
    auto raw = reader.ReadUInt8();
    if (!raw.ok()) {
      return raw.status();
    }
    if (raw.value() > 1) {
      return Status::InvalidArgument("payload bool-list value is invalid");
    }
    values.push_back(raw.value() != 0);
  }
  return values;
}

StatusOr<std::vector<std::string>> ReadStringList(
    std::span<const std::byte> bytes) {
  ByteReader reader(bytes);
  auto count = reader.ReadInteger<std::uint32_t>();
  if (!count.ok()) {
    return count.status();
  }
  std::vector<std::string> values;
  values.reserve(count.value());
  for (std::uint32_t i = 0; i < count.value(); ++i) {
    auto length = reader.ReadInteger<std::uint32_t>();
    if (!length.ok()) {
      return length.status();
    }
    auto raw = reader.ReadBytes(length.value());
    if (!raw.ok()) {
      return raw.status();
    }
    values.emplace_back(reinterpret_cast<const char*>(raw->data()),
                        raw->size());
  }
  if (reader.remaining() != 0) {
    return Status::InvalidArgument("payload string-list has trailing bytes");
  }
  return values;
}

template <typename T>
StatusOr<DecodedValue> ToDecodedValue(StatusOr<T> value) {
  if (!value.ok()) {
    return value.status();
  }
  return DecodedValue(std::move(value).value());
}

StatusOr<DecodedValue> DecodeValue(WireKind kind,
                                   std::span<const std::byte> bytes) {
  switch (kind) {
    case WireKind::kNull:
      if (!bytes.empty()) {
        return Status::InvalidArgument("payload null field must be empty");
      }
      return DecodedValue(std::monostate{});
    case WireKind::kInt8:
      return ToDecodedValue(ReadExactInteger<std::int8_t>(bytes));
    case WireKind::kInt32:
      return ToDecodedValue(ReadExactInteger<std::int32_t>(bytes));
    case WireKind::kInt64:
      return ToDecodedValue(ReadExactInteger<std::int64_t>(bytes));
    case WireKind::kUInt64:
      return ToDecodedValue(ReadExactInteger<std::uint64_t>(bytes));
    case WireKind::kBool:
      return ToDecodedValue(ReadBool(bytes));
    case WireKind::kString:
      return DecodedValue(std::string(
          reinterpret_cast<const char*>(bytes.data()), bytes.size()));
    case WireKind::kInt8List:
      return ToDecodedValue(ReadScalarList<std::int8_t>(bytes));
    case WireKind::kInt32List:
      return ToDecodedValue(ReadScalarList<std::int32_t>(bytes));
    case WireKind::kInt64List:
      return ToDecodedValue(ReadScalarList<std::int64_t>(bytes));
    case WireKind::kUInt64List:
      return ToDecodedValue(ReadScalarList<std::uint64_t>(bytes));
    case WireKind::kBoolList:
      return ToDecodedValue(ReadBoolList(bytes));
    case WireKind::kStringList:
      return ToDecodedValue(ReadStringList(bytes));
  }
  return Status::InvalidArgument("payload field kind is unsupported");
}

StatusOr<DecodedPayload> DecodePayload(std::span<const std::byte> bytes) {
  if (bytes.size() < 12 || bytes[0] != kMagic0 || bytes[1] != kMagic1 ||
      bytes[2] != kMagic2 || bytes[3] != kMagic3) {
    return Status::InvalidArgument("payload magic is invalid");
  }

  ByteReader reader(bytes.subspan(4));
  auto version = reader.ReadInteger<std::uint16_t>();
  if (!version.ok()) {
    return version.status();
  }
  if (version.value() != kWireVersion) {
    return Status::InvalidArgument("payload version is unsupported");
  }
  auto flags = reader.ReadInteger<std::uint16_t>();
  if (!flags.ok()) {
    return flags.status();
  }
  if (flags.value() != 0) {
    return Status::InvalidArgument("payload flags are unsupported");
  }
  auto field_count = reader.ReadInteger<std::uint32_t>();
  if (!field_count.ok()) {
    return field_count.status();
  }

  DecodedPayload payload;
  payload.fields.reserve(field_count.value());
  std::set<FieldId> seen_fields;
  for (std::uint32_t i = 0; i < field_count.value(); ++i) {
    auto field_id = reader.ReadInteger<std::uint32_t>();
    if (!field_id.ok()) {
      return field_id.status();
    }
    if (!seen_fields.insert(field_id.value()).second) {
      return Status::InvalidArgument("payload contains duplicate field id");
    }
    auto kind_byte = reader.ReadUInt8();
    if (!kind_byte.ok()) {
      return kind_byte.status();
    }
    auto byte_length = reader.ReadInteger<std::uint32_t>();
    if (!byte_length.ok()) {
      return byte_length.status();
    }
    auto field_bytes = reader.ReadBytes(byte_length.value());
    if (!field_bytes.ok()) {
      return field_bytes.status();
    }

    const auto kind = static_cast<WireKind>(kind_byte.value());
    auto value = DecodeValue(kind, field_bytes.value());
    if (!value.ok()) {
      return value.status();
    }
    payload.fields.push_back(DecodedField{
        .field_id = field_id.value(),
        .kind = kind,
        .value = std::move(value).value(),
    });
  }
  if (reader.remaining() != 0) {
    return Status::InvalidArgument("payload has trailing bytes");
  }
  return payload;
}

template <typename T>
const T* GetValue(const DecodedField& field) {
  return std::get_if<T>(&field.value);
}

Status WriteArenaStringListField(const FieldLayout& field,
                                 const std::vector<std::string>& values,
                                 model::EncodedRow* encoded) {
  if (encoded == nullptr) {
    return Status::InvalidArgument("encoded row must not be null");
  }
  if (!field.is_list || field.type != FieldType::kString ||
      field.encoding != FieldEncoding::kArena) {
    return Status::InvalidArgument("arena string-list writer does not match field");
  }
  if (values.size() > std::numeric_limits<std::uint32_t>::max()) {
    return Status::InvalidArgument("string-list value count is too large");
  }

  const std::uint64_t refs_offset = encoded->arena.size();
  const std::size_t refs_bytes = values.size() * 16U;
  if (refs_bytes > std::numeric_limits<std::uint32_t>::max()) {
    return Status::InvalidArgument("string-list ref bytes are too large");
  }
  encoded->arena.resize(encoded->arena.size() + refs_bytes);

  for (std::size_t i = 0; i < values.size(); ++i) {
    const std::string& value = values[i];
    if (value.size() > std::numeric_limits<std::uint32_t>::max()) {
      return Status::InvalidArgument("string-list element is too large");
    }
    const std::uint64_t value_offset = encoded->arena.size();
    encoded->arena.insert(
        encoded->arena.end(), reinterpret_cast<const std::byte*>(value.data()),
        reinterpret_cast<const std::byte*>(value.data()) + value.size());
    const Status ref_status = base::WriteValueRef16(
        base::ValueRef16{
            .offset = value_offset,
            .byte_length = static_cast<std::uint32_t>(value.size()),
            .element_count_or_flags = 0,
        },
        std::span<std::byte>(encoded->arena),
        static_cast<std::size_t>(refs_offset) + i * 16U);
    if (!ref_status.ok()) {
      return ref_status;
    }
  }

  return model::WriteValueRefField(
      field,
      base::ValueRef16{
          .offset = refs_offset,
          .byte_length = static_cast<std::uint32_t>(refs_bytes),
          .element_count_or_flags = static_cast<std::uint32_t>(values.size()),
      },
      encoded);
}

StatusOr<std::vector<std::byte>> EncodeBoolListBytes(
    const std::vector<bool>& values) {
  if (values.size() > std::numeric_limits<std::uint32_t>::max()) {
    return Status::InvalidArgument("bool-list value count is too large");
  }
  std::vector<std::byte> bytes(values.size());
  for (std::size_t i = 0; i < values.size(); ++i) {
    bytes[i] = static_cast<std::byte>(values[i] ? 1 : 0);
  }
  return bytes;
}

Status WriteArenaBoolListField(const FieldLayout& field,
                               const std::vector<bool>& values,
                               model::EncodedRow* encoded) {
  if (encoded == nullptr) {
    return Status::InvalidArgument("encoded row must not be null");
  }
  if (!field.is_list || field.type != FieldType::kBool ||
      field.encoding != FieldEncoding::kArena) {
    return Status::InvalidArgument("arena bool-list writer does not match field");
  }
  auto payload = EncodeBoolListBytes(values);
  if (!payload.ok()) {
    return payload.status();
  }
  const std::uint64_t offset = encoded->arena.size();
  encoded->arena.insert(encoded->arena.end(), payload->begin(), payload->end());
  return model::WriteValueRefField(
      field,
      base::ValueRef16{
          .offset = offset,
          .byte_length = static_cast<std::uint32_t>(payload->size()),
          .element_count_or_flags = static_cast<std::uint32_t>(values.size()),
      },
      encoded);
}

Status WriteScalarFieldValue(const FieldLayout& field,
                             const DecodedField& decoded,
                             model::EncodedRow* encoded) {
  if (field.is_list) {
    return Status::InvalidArgument("payload field shape does not match layout");
  }

  switch (field.type) {
    case FieldType::kInt8: {
      const auto* value = GetValue<std::int8_t>(decoded);
      if (value == nullptr) {
        return Status::InvalidArgument("payload int8 field type mismatch");
      }
      return model::WriteScalarField(field, *value, encoded);
    }
    case FieldType::kInt32: {
      const auto* value = GetValue<std::int32_t>(decoded);
      if (value == nullptr) {
        return Status::InvalidArgument("payload int32 field type mismatch");
      }
      return model::WriteScalarField(field, *value, encoded);
    }
    case FieldType::kInt64: {
      const auto* value = GetValue<std::int64_t>(decoded);
      if (value == nullptr) {
        return Status::InvalidArgument("payload int64 field type mismatch");
      }
      return model::WriteScalarField(field, *value, encoded);
    }
    case FieldType::kUInt64: {
      const auto* value = GetValue<std::uint64_t>(decoded);
      if (value == nullptr) {
        return Status::InvalidArgument("payload uint64 field type mismatch");
      }
      return model::WriteScalarField(field, *value, encoded);
    }
    case FieldType::kBool: {
      const auto* value = GetValue<bool>(decoded);
      if (value == nullptr) {
        return Status::InvalidArgument("payload bool field type mismatch");
      }
      return model::WriteScalarField(field, *value, encoded);
    }
    case FieldType::kString: {
      const auto* value = GetValue<std::string>(decoded);
      if (value == nullptr) {
        return Status::InvalidArgument("payload string field type mismatch");
      }
      if (field.encoding == FieldEncoding::kArena) {
        return model::WriteArenaStringField(field, *value, encoded);
      }
      if (field.encoding == FieldEncoding::kDictionary) {
        if (encoded == nullptr) {
          return Status::InvalidArgument("encoded row must not be null");
        }
        if (encoded->string_dictionary.size() >
            std::numeric_limits<std::uint32_t>::max()) {
          return Status::InvalidArgument("string dictionary is too large");
        }
        const auto id =
            static_cast<std::uint32_t>(encoded->string_dictionary.size());
        encoded->string_dictionary.push_back(*value);
        return model::WriteDictionaryStringField(field, id, encoded);
      }
      return Status::InvalidArgument("payload string field encoding unsupported");
    }
  }
  return Status::InvalidArgument("payload scalar field type unsupported");
}

template <typename T>
Status WriteArenaScalarList(const FieldLayout& field,
                            const std::vector<T>& values,
                            model::EncodedRow* encoded) {
  return model::WriteArenaListField<T>(
      field, std::span<const T>(values.data(), values.size()), encoded);
}

template <typename T>
Status WriteListDictionaryScalarList(const FieldLayout& field,
                                     const std::vector<T>& values,
                                     model::EncodedRow* encoded) {
  if (encoded == nullptr) {
    return Status::InvalidArgument("encoded row must not be null");
  }
  if (values.size() > std::numeric_limits<std::uint32_t>::max() ||
      encoded->scalar_list_dictionary.size() >
          std::numeric_limits<std::uint32_t>::max()) {
    return Status::InvalidArgument("scalar-list dictionary entry is too large");
  }
  auto payload = model::EncodeScalarList(values);
  if (!payload.ok()) {
    return payload.status();
  }
  const auto dictionary_id =
      static_cast<std::uint32_t>(encoded->scalar_list_dictionary.size());
  encoded->scalar_list_dictionary.push_back(std::move(payload).value());
  return model::WriteListDictionaryField(
      field, dictionary_id, static_cast<std::uint32_t>(values.size()), encoded);
}

Status WriteListFieldValue(const FieldLayout& field,
                           const DecodedField& decoded,
                           model::EncodedRow* encoded) {
  if (!field.is_list) {
    return Status::InvalidArgument("payload field shape does not match layout");
  }

  switch (field.type) {
    case FieldType::kInt8: {
      const auto* values = GetValue<std::vector<std::int8_t>>(decoded);
      if (values == nullptr) {
        return Status::InvalidArgument("payload int8-list field type mismatch");
      }
      if (field.encoding == FieldEncoding::kArena) {
        return WriteArenaScalarList(field, *values, encoded);
      }
      if (field.encoding == FieldEncoding::kListDictionary) {
        return WriteListDictionaryScalarList(field, *values, encoded);
      }
      break;
    }
    case FieldType::kInt32: {
      const auto* values = GetValue<std::vector<std::int32_t>>(decoded);
      if (values == nullptr) {
        return Status::InvalidArgument("payload int32-list field type mismatch");
      }
      if (field.encoding == FieldEncoding::kArena) {
        return WriteArenaScalarList(field, *values, encoded);
      }
      if (field.encoding == FieldEncoding::kListDictionary) {
        return WriteListDictionaryScalarList(field, *values, encoded);
      }
      break;
    }
    case FieldType::kInt64: {
      const auto* values = GetValue<std::vector<std::int64_t>>(decoded);
      if (values == nullptr) {
        return Status::InvalidArgument("payload int64-list field type mismatch");
      }
      if (field.encoding == FieldEncoding::kArena) {
        return WriteArenaScalarList(field, *values, encoded);
      }
      if (field.encoding == FieldEncoding::kListDictionary) {
        return WriteListDictionaryScalarList(field, *values, encoded);
      }
      break;
    }
    case FieldType::kUInt64: {
      const auto* values = GetValue<std::vector<std::uint64_t>>(decoded);
      if (values == nullptr) {
        return Status::InvalidArgument(
            "payload uint64-list field type mismatch");
      }
      if (field.encoding == FieldEncoding::kArena) {
        return WriteArenaScalarList(field, *values, encoded);
      }
      if (field.encoding == FieldEncoding::kListDictionary) {
        return WriteListDictionaryScalarList(field, *values, encoded);
      }
      break;
    }
    case FieldType::kBool: {
      const auto* values = GetValue<std::vector<bool>>(decoded);
      if (values == nullptr) {
        return Status::InvalidArgument("payload bool-list field type mismatch");
      }
      if (field.encoding == FieldEncoding::kArena) {
        return WriteArenaBoolListField(field, *values, encoded);
      }
      if (field.encoding == FieldEncoding::kListDictionary) {
        if (encoded == nullptr) {
          return Status::InvalidArgument("encoded row must not be null");
        }
        auto payload = EncodeBoolListBytes(*values);
        if (!payload.ok()) {
          return payload.status();
        }
        if (encoded->scalar_list_dictionary.size() >
            std::numeric_limits<std::uint32_t>::max()) {
          return Status::InvalidArgument("scalar-list dictionary is too large");
        }
        const auto dictionary_id =
            static_cast<std::uint32_t>(encoded->scalar_list_dictionary.size());
        encoded->scalar_list_dictionary.push_back(std::move(payload).value());
        return model::WriteListDictionaryField(
            field, dictionary_id, static_cast<std::uint32_t>(values->size()),
            encoded);
      }
      break;
    }
    case FieldType::kString: {
      const auto* values = GetValue<std::vector<std::string>>(decoded);
      if (values == nullptr) {
        return Status::InvalidArgument(
            "payload string-list field type mismatch");
      }
      if (field.encoding == FieldEncoding::kArena) {
        return WriteArenaStringListField(field, *values, encoded);
      }
      if (field.encoding == FieldEncoding::kListDictionary) {
        if (encoded == nullptr) {
          return Status::InvalidArgument("encoded row must not be null");
        }
        if (values->size() > std::numeric_limits<std::uint32_t>::max() ||
            encoded->string_list_dictionary.size() >
                std::numeric_limits<std::uint32_t>::max()) {
          return Status::InvalidArgument(
              "string-list dictionary entry is too large");
        }
        const auto dictionary_id =
            static_cast<std::uint32_t>(encoded->string_list_dictionary.size());
        encoded->string_list_dictionary.push_back(*values);
        return model::WriteListDictionaryField(
            field, dictionary_id, static_cast<std::uint32_t>(values->size()),
            encoded);
      }
      if (field.encoding == FieldEncoding::kElementDictionary) {
        if (encoded == nullptr) {
          return Status::InvalidArgument("encoded row must not be null");
        }
        std::vector<std::uint32_t> dictionary_ids;
        dictionary_ids.reserve(values->size());
        for (const std::string& value : *values) {
          if (encoded->string_element_dictionary.size() >
              std::numeric_limits<std::uint32_t>::max()) {
            return Status::InvalidArgument("string element dictionary is too large");
          }
          dictionary_ids.push_back(static_cast<std::uint32_t>(
              encoded->string_element_dictionary.size()));
          encoded->string_element_dictionary.push_back(value);
        }
        return model::WriteElementDictionaryStringListField(
            field, std::span<const std::uint32_t>(dictionary_ids), encoded);
      }
      break;
    }
  }
  return Status::InvalidArgument("payload list field encoding unsupported");
}

StatusOr<model::EncodedRow> EncodeForLayout(
    const DecodedPayload& payload,
    const std::shared_ptr<const CompiledRowLayout>& layout) {
  if (layout == nullptr) {
    return Status::FailedPrecondition("target layout is null");
  }

  auto encoded = model::CreateEncodedRow(*layout);
  for (const DecodedField& decoded : payload.fields) {
    const FieldLayout* field = layout->FindField(decoded.field_id);
    if (field == nullptr) {
      continue;
    }
    if (std::holds_alternative<std::monostate>(decoded.value)) {
      return Status::InvalidArgument("payload explicit null field is unsupported");
    }

    const Status write_status =
        field->is_list ? WriteListFieldValue(*field, decoded, &encoded)
                       : WriteScalarFieldValue(*field, decoded, &encoded);
    if (!write_status.ok()) {
      return write_status;
    }
  }

  auto validation = model::MaterializeRow(layout, encoded);
  if (!validation.ok()) {
    return validation.status();
  }
  return encoded;
}

std::size_t ShardFor(std::uint64_t primary_key,
                     const UpdateTargetRoute& route) noexcept {
  const std::uint64_t hash =
      base::StableHash64(primary_key, route.hash_seed, route.hash_version);
  return static_cast<std::size_t>(hash & (route.shard_count - 1U));
}

struct PreparedPublish {
  std::shared_ptr<store::RealtimeDeltaAtomicTable> table;
  std::uint64_t primary_key = 0;
  SourcePosition position;
  model::EncodedRow encoded;
};

}  // namespace

UpdateApplier::UpdateApplier(UpdateApplierOptions options)
    : options_(std::move(options)) {}

Status UpdateApplier::Apply(const KafkaUpsertMessage& message) {
  if (const Status status = ValidateOptions(options_); !status.ok()) {
    return status;
  }
  if (const Status status =
          ValidateMessageMetadata(message, options_.logical_topic);
      !status.ok()) {
    return status;
  }

  auto decoded = DecodePayload(std::span<const std::byte>(message.payload));
  if (!decoded.ok()) {
    return decoded.status();
  }

  const SourcePosition source_position{
      .partition = message.metadata.partition.partition,
      .offset = message.metadata.offset,
  };

  std::vector<PreparedPublish> prepared;
  prepared.reserve(options_.targets.size());
  for (const UpdateTargetRoute& route : options_.targets) {
    const std::size_t shard = ShardFor(message.primary_key, route);
    auto encoded = EncodeForLayout(decoded.value(), route.layout);
    if (!encoded.ok()) {
      return encoded.status();
    }
    prepared.push_back(PreparedPublish{
        .table = route.realtime_shards[shard],
        .primary_key = message.primary_key,
        .position = source_position,
        .encoded = std::move(encoded).value(),
    });
  }

  for (PreparedPublish& publish : prepared) {
    const Status status =
        publish.table->Publish(publish.primary_key, publish.position,
                               std::move(publish.encoded));
    if (!status.ok()) {
      return status;
    }
  }
  return Status::Ok();
}

Status UpdateApplier::ApplyBatch(std::span<const KafkaUpsertMessage> messages) {
  for (const KafkaUpsertMessage& message : messages) {
    if (const Status status = Apply(message); !status.ok()) {
      return status;
    }
  }
  return Status::Ok();
}

}  // namespace kv_index::ingest
