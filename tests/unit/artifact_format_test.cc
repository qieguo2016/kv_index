#include "src/artifact/artifact_format.h"

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "kv_index/schema.h"
#include "kv_index/status.h"
#include "src/base/byte_io.h"
#include "src/model/row_storage.h"
#include "tests/test_support/artifact_writer.h"
#include "tests/test_support/test_macros.h"

namespace {

namespace storage = kv_index::model;

using kv_index::CompiledRowLayout;
using kv_index::FieldEncoding;
using kv_index::FieldSpec;
using kv_index::FieldType;
using kv_index::RuntimeSchema;
using kv_index::Status;
using kv_index::StatusCode;
using kv_index::artifact::ArtifactSourceProgressPolicy;
using kv_index::artifact::ArtifactSectionType;
using kv_index::artifact::ArtifactValidationOptions;
using kv_index::artifact::Fnv1a64;
using kv_index::artifact::kArtifactGlobalShardId;
using kv_index::artifact::kArtifactHeaderSize;
using kv_index::artifact::kArtifactSectionEntrySize;
using kv_index::artifact::ParseArtifact;
using kv_index::base::ReadLittleEndian;
using kv_index::base::WriteLittleEndian;
using kv_index::test_support::ArtifactShardSpec;
using kv_index::test_support::TestArtifactSpec;
using kv_index::test_support::TestSourceProgress;
using kv_index::test_support::WriteTestArtifact;

constexpr std::size_t kEntryTypeOffset = 0;
constexpr std::size_t kEntryShardIdOffset = 4;
constexpr std::size_t kEntryFileOffsetOffset = 8;
constexpr std::size_t kEntryLengthOffset = 16;
constexpr std::size_t kEntryChecksumOffset = 24;

std::string TempPath(const std::string& name) {
  const char* tmpdir = std::getenv("TEST_TMPDIR");
  if (tmpdir == nullptr) {
    tmpdir = "/tmp";
  }
  return std::string(tmpdir) + "/kv_index_" + name;
}

FieldSpec Scalar(kv_index::FieldId field_id, std::string name,
                 FieldType type, bool nullable) {
  return FieldSpec{
      .field_id = field_id,
      .name = std::move(name),
      .type = type,
      .is_list = false,
      .nullable = nullable,
      .encoding = type == FieldType::kString ? FieldEncoding::kArena
                                             : FieldEncoding::kFixed,
  };
}

std::shared_ptr<const CompiledRowLayout> MakeLayout(
    std::uint64_t schema_version = 100) {
  RuntimeSchema schema(schema_version);
  KV_INDEX_CHECK(schema.AddField(Scalar(1, "score", FieldType::kInt32, false))
                     .ok());
  auto layout = CompiledRowLayout::Compile(schema);
  KV_INDEX_CHECK(layout.ok());
  return std::make_shared<const CompiledRowLayout>(std::move(layout).value());
}

storage::EncodedRow MakeRow(const CompiledRowLayout& layout,
                            std::int32_t score) {
  auto encoded = storage::CreateEncodedRow(layout);
  const auto* field = layout.FindField(1);
  KV_INDEX_CHECK(field != nullptr);
  KV_INDEX_CHECK(storage::WriteScalarField(*field, score, &encoded).ok());
  return encoded;
}

TestArtifactSpec ValidSpec() {
  auto layout = MakeLayout();
  TestArtifactSpec spec;
  spec.artifact_id = "artifact-format-valid";
  spec.shard_count = 2;
  spec.hash_seed = 17;
  spec.hash_version = 1;
  spec.layout = layout;
  spec.source_progress.push_back(TestSourceProgress{
      .topic = "topic-a",
      .partition = 3,
      .checkpoint_next_offset = 41,
      .high_watermark = 44,
  });
  spec.shards.resize(2);
  spec.shards[0].shard_id = 0;
  spec.shards[0].rows.push_back({.primary_key = 10,
                                 .encoded = MakeRow(*layout, 123)});
  spec.shards[1].shard_id = 1;
  return spec;
}

std::vector<std::byte> ReadFile(const std::string& path) {
  std::ifstream input(path, std::ios::binary);
  const std::vector<char> chars((std::istreambuf_iterator<char>(input)),
                                std::istreambuf_iterator<char>());
  std::vector<std::byte> bytes(chars.size());
  for (std::size_t i = 0; i < chars.size(); ++i) {
    bytes[i] = static_cast<std::byte>(chars[i]);
  }
  return bytes;
}

Status ValidateSecondSectionIsSourceProgress(std::span<const std::byte> bytes) {
  const std::size_t source_entry_offset =
      kArtifactHeaderSize + kArtifactSectionEntrySize;
  auto source_type =
      ReadLittleEndian<std::uint32_t>(bytes,
                                      source_entry_offset + kEntryTypeOffset);
  auto source_shard_id = ReadLittleEndian<std::uint32_t>(
      bytes, source_entry_offset + kEntryShardIdOffset);
  if (!source_type.ok() || !source_shard_id.ok()) {
    return Status::InvalidArgument("source progress directory entry truncated");
  }
  if (*source_type !=
          static_cast<std::uint32_t>(ArtifactSectionType::kSourceProgress) ||
      *source_shard_id != kArtifactGlobalShardId) {
    return Status::InvalidArgument(
        "second artifact section is not source progress");
  }
  return Status::Ok();
}

Status MakeSourceProgressTopicLengthInvalid(std::vector<std::byte>* bytes) {
  if (const Status status = ValidateSecondSectionIsSourceProgress(*bytes);
      !status.ok()) {
    return status;
  }
  const std::size_t source_entry_offset =
      kArtifactHeaderSize + kArtifactSectionEntrySize;
  auto source_offset = ReadLittleEndian<std::uint64_t>(
      *bytes, source_entry_offset + kEntryFileOffsetOffset);
  auto source_length = ReadLittleEndian<std::uint64_t>(
      *bytes, source_entry_offset + kEntryLengthOffset);
  if (!source_offset.ok() || !source_length.ok()) {
    return Status::InvalidArgument("source progress section entry truncated");
  }
  if (*source_offset > bytes->size() ||
      bytes->size() - static_cast<std::size_t>(*source_offset) <
          *source_length ||
      *source_length < 2 * sizeof(std::uint32_t)) {
    return Status::InvalidArgument("source progress section out of bounds");
  }
  if (const Status status = WriteLittleEndian<std::uint32_t>(
          std::numeric_limits<std::uint32_t>::max(),
          std::span<std::byte>(*bytes),
          *source_offset + sizeof(std::uint32_t));
      !status.ok()) {
    return status;
  }
  const std::uint64_t checksum = Fnv1a64(std::span<const std::byte>(
      bytes->data() + *source_offset, *source_length));
  return WriteLittleEndian<std::uint64_t>(
      checksum, std::span<std::byte>(*bytes),
      source_entry_offset + kEntryChecksumOffset);
}

void ValidTinyArtifactParsesMetadataSectionsAndProgress() {
  const std::string path = TempPath("artifact_format_valid.kvi");
  const auto spec = ValidSpec();
  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());

  auto parsed = ParseArtifact(
      ReadFile(path),
      ArtifactValidationOptions{.expected_shard_count = spec.shard_count,
                                .expected_hash_seed = spec.hash_seed,
                                .expected_hash_version = spec.hash_version});

  KV_INDEX_CHECK(parsed.ok());
  KV_INDEX_CHECK_EQ(parsed->artifact_id, spec.artifact_id);
  KV_INDEX_CHECK_EQ(parsed->shard_count, 2u);
  KV_INDEX_CHECK_EQ(parsed->schema_version, spec.layout->schema_version());
  KV_INDEX_CHECK_EQ(parsed->layout_fingerprint,
                    spec.layout->layout_fingerprint());
  KV_INDEX_CHECK_EQ(parsed->source_progress.size(), 1u);
  KV_INDEX_CHECK_EQ(parsed->source_progress[0].topic, std::string("topic-a"));
  KV_INDEX_CHECK_EQ(parsed->source_progress[0].partition, 3);
  KV_INDEX_CHECK_EQ(parsed->source_progress[0].checkpoint_next_offset, 41);
  KV_INDEX_CHECK_EQ(parsed->source_progress[0].high_watermark, 44);
  KV_INDEX_CHECK(parsed->FindSection(ArtifactSectionType::kFrozenPrimaryKeyIndex,
                                     0)
                     .has_value());
}

void MissingSourceProgressRejectedByDefault() {
  const std::string path =
      TempPath("artifact_format_missing_progress_default.kvi");
  auto spec = ValidSpec();
  spec.include_source_progress_section = false;
  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());

  auto parsed = ParseArtifact(
      ReadFile(path),
      ArtifactValidationOptions{.expected_shard_count = spec.shard_count,
                                .expected_hash_seed = spec.hash_seed,
                                .expected_hash_version = spec.hash_version});

  KV_INDEX_CHECK(!parsed.ok());
  KV_INDEX_CHECK_EQ(parsed.status().code(), StatusCode::kInvalidArgument);
}

void MissingSourceProgressAllowedWhenOptional() {
  const std::string path =
      TempPath("artifact_format_missing_progress_optional.kvi");
  auto spec = ValidSpec();
  spec.include_source_progress_section = false;
  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());

  auto parsed = ParseArtifact(
      ReadFile(path),
      ArtifactValidationOptions{
          .expected_shard_count = spec.shard_count,
          .expected_hash_seed = spec.hash_seed,
          .expected_hash_version = spec.hash_version,
          .source_progress_policy = ArtifactSourceProgressPolicy::kOptional});

  KV_INDEX_CHECK(parsed.ok());
  KV_INDEX_CHECK_EQ(parsed->artifact_id, spec.artifact_id);
  KV_INDEX_CHECK_EQ(parsed->shard_count, spec.shard_count);
  KV_INDEX_CHECK_EQ(parsed->hash_seed, spec.hash_seed);
  KV_INDEX_CHECK_EQ(parsed->hash_version, spec.hash_version);
  KV_INDEX_CHECK(parsed->layout != nullptr);
  KV_INDEX_CHECK(parsed->source_progress.empty());
  KV_INDEX_CHECK(!parsed->FindSection(ArtifactSectionType::kSourceProgress,
                                      kArtifactGlobalShardId)
                      .has_value());
  KV_INDEX_CHECK(parsed->FindSection(ArtifactSectionType::kFrozenPrimaryKeyIndex,
                                     0)
                     .has_value());
}

void MalformedSourceProgressRejectedEvenWhenOptional() {
  const std::string path =
      TempPath("artifact_format_malformed_progress_optional.kvi");
  const auto spec = ValidSpec();
  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());
  auto bytes = ReadFile(path);
  KV_INDEX_CHECK(MakeSourceProgressTopicLengthInvalid(&bytes).ok());

  auto parsed = ParseArtifact(
      bytes, ArtifactValidationOptions{
                 .expected_shard_count = spec.shard_count,
                 .expected_hash_seed = spec.hash_seed,
                 .expected_hash_version = spec.hash_version,
                 .source_progress_policy =
                     ArtifactSourceProgressPolicy::kOptional});

  KV_INDEX_CHECK(!parsed.ok());
  KV_INDEX_CHECK_EQ(parsed.status().code(), StatusCode::kInvalidArgument);
}

void BadMagicAndVersionAreRejected() {
  const std::string path = TempPath("artifact_format_bad_header.kvi");
  const auto spec = ValidSpec();
  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());

  auto bytes = ReadFile(path);
  bytes[0] = std::byte{0};
  auto bad_magic = ParseArtifact(bytes, {});
  KV_INDEX_CHECK(!bad_magic.ok());
  KV_INDEX_CHECK_EQ(bad_magic.status().code(), StatusCode::kInvalidArgument);

  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());
  bytes = ReadFile(path);
  bytes[8] = std::byte{99};
  auto bad_version = ParseArtifact(bytes, {});
  KV_INDEX_CHECK(!bad_version.ok());
  KV_INDEX_CHECK_EQ(bad_version.status().code(), StatusCode::kInvalidArgument);
}

void OverlappingAndOutOfBoundsSectionsAreRejected() {
  const std::string path = TempPath("artifact_format_ranges.kvi");
  const auto spec = ValidSpec();
  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());

  auto overlapping = ReadFile(path);
  auto second_offset =
      kv_index::test_support::SecondSectionOffset(overlapping);
  KV_INDEX_CHECK(second_offset.ok());
  KV_INDEX_CHECK(kv_index::test_support::RewriteFirstSectionOffset(
                     &overlapping, *second_offset)
                     .ok());
  auto overlap = ParseArtifact(overlapping, {});
  KV_INDEX_CHECK(!overlap.ok());

  auto oob = ReadFile(path);
  KV_INDEX_CHECK(
      kv_index::test_support::RewriteFirstSectionLength(&oob, oob.size()).ok());
  auto out_of_bounds = ParseArtifact(oob, {});
  KV_INDEX_CHECK(!out_of_bounds.ok());
}

void SectionChecksumFailuresAreRejected() {
  const std::string path = TempPath("artifact_format_checksum.kvi");
  const auto spec = ValidSpec();
  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());

  auto bytes = ReadFile(path);
  KV_INDEX_CHECK(kv_index::test_support::CorruptFirstSectionByte(&bytes).ok());
  auto parsed = ParseArtifact(bytes, {});
  KV_INDEX_CHECK(!parsed.ok());
  KV_INDEX_CHECK_EQ(parsed.status().code(), StatusCode::kFailedPrecondition);
}

void ShardAndHashMismatchesAreRejected() {
  const std::string path = TempPath("artifact_format_mismatch.kvi");
  const auto spec = ValidSpec();
  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());
  const auto bytes = ReadFile(path);

  auto bad_shards = ParseArtifact(
      bytes, ArtifactValidationOptions{.expected_shard_count = 4,
                                       .expected_hash_seed = spec.hash_seed,
                                       .expected_hash_version = 1});
  KV_INDEX_CHECK(!bad_shards.ok());
  KV_INDEX_CHECK_EQ(bad_shards.status().code(), StatusCode::kFailedPrecondition);

  auto bad_hash = ParseArtifact(
      bytes, ArtifactValidationOptions{.expected_shard_count = spec.shard_count,
                                       .expected_hash_seed = 99,
                                       .expected_hash_version = 1});
  KV_INDEX_CHECK(!bad_hash.ok());
  KV_INDEX_CHECK_EQ(bad_hash.status().code(), StatusCode::kFailedPrecondition);
}

int Main() {
  ValidTinyArtifactParsesMetadataSectionsAndProgress();
  MissingSourceProgressRejectedByDefault();
  MissingSourceProgressAllowedWhenOptional();
  MalformedSourceProgressRejectedEvenWhenOptional();
  BadMagicAndVersionAreRejected();
  OverlappingAndOutOfBoundsSectionsAreRejected();
  SectionChecksumFailuresAreRejected();
  ShardAndHashMismatchesAreRejected();
  return 0;
}

}  // namespace

int main() { return Main(); }
