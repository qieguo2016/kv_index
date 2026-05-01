#include "src/artifact/mmap_snapshot_backing.h"

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>

#include "kv_index/row.h"
#include "kv_index/schema.h"
#include "kv_index/status.h"
#include "src/model/row_storage.h"
#include "src/store/snapshot.h"
#include "tests/test_support/artifact_writer.h"
#include "tests/test_support/test_macros.h"

namespace {

namespace storage = kv_index::internal;

using kv_index::CompiledRowLayout;
using kv_index::FieldEncoding;
using kv_index::FieldSpec;
using kv_index::FieldType;
using kv_index::Row;
using kv_index::RuntimeSchema;
using kv_index::StatusCode;
using kv_index::core::FullSnapshotView;
using kv_index::core::MmapSnapshotBacking;
using kv_index::core::MmapSnapshotLoadOptions;
using kv_index::core::SnapshotBacking;
using kv_index::test_support::ArtifactShardSpec;
using kv_index::test_support::TestArtifactSpec;
using kv_index::test_support::TestSourceProgress;
using kv_index::test_support::WriteTestArtifact;

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
    std::uint64_t schema_version = 700) {
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

TestArtifactSpec SpecWithOneRow() {
  auto layout = MakeLayout();
  TestArtifactSpec spec;
  spec.artifact_id = "mmap-valid";
  spec.shard_count = 1;
  spec.hash_seed = 5;
  spec.hash_version = 1;
  spec.layout = layout;
  spec.source_progress.push_back(TestSourceProgress{
      .topic = "updates",
      .partition = 0,
      .checkpoint_next_offset = 10,
      .high_watermark = 10,
  });
  spec.shards.push_back(ArtifactShardSpec{
      .shard_id = 0,
      .rows = {{.primary_key = 42, .encoded = MakeRow(*layout, 88)}},
  });
  return spec;
}

MmapSnapshotLoadOptions OptionsFor(const TestArtifactSpec& spec) {
  return MmapSnapshotLoadOptions{
      .expected_shard_count = spec.shard_count,
      .expected_hash_seed = spec.hash_seed,
      .expected_hash_version = spec.hash_version,
  };
}

void LocalFileMappingServesRowsThroughFullSnapshotView() {
  const std::string path = TempPath("mmap_snapshot_valid.kvi");
  const auto spec = SpecWithOneRow();
  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());

  auto backing = MmapSnapshotBacking::LoadShard(path, 0, OptionsFor(spec));
  KV_INDEX_CHECK(backing.ok());
  KV_INDEX_CHECK(!(*backing)->prewarmed());
  FullSnapshotView view(*backing);
  auto row = view.Get(42);
  KV_INDEX_CHECK(row.ok());
  KV_INDEX_CHECK(row->has_value());
  KV_INDEX_CHECK_EQ(row->value().Get<std::int32_t>(1).value(), 88);
}

void FileUriMappingAndViewOwnMappedLifetime() {
  const std::string path = TempPath("mmap_snapshot_file_uri.kvi");
  const auto spec = SpecWithOneRow();
  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());

  FullSnapshotView view(std::shared_ptr<const SnapshotBacking>{});
  {
    auto backing =
        MmapSnapshotBacking::LoadShard("file://" + path, 0, OptionsFor(spec));
    KV_INDEX_CHECK(backing.ok());
    view = FullSnapshotView(*backing);
  }

  auto row = view.Get(42);
  KV_INDEX_CHECK(row.ok());
  KV_INDEX_CHECK(row->has_value());
  KV_INDEX_CHECK_EQ(row->value().Get<std::int32_t>(1).value(), 88);
}

void InvalidPathAndRemoteSchemeFailClosed() {
  auto missing = MmapSnapshotBacking::LoadShard(
      "/tmp/kv_index_missing_artifact_for_mmap", 0, {});
  KV_INDEX_CHECK(!missing.ok());
  KV_INDEX_CHECK_EQ(missing.status().code(), StatusCode::kNotFound);

  auto remote = MmapSnapshotBacking::LoadShard(
      "https://example.com/artifact.kvi", 0, {});
  KV_INDEX_CHECK(!remote.ok());
  KV_INDEX_CHECK_EQ(remote.status().code(), StatusCode::kInvalidArgument);
}

void ExplicitPrewarmTouchesShardSectionsAndMarksReady() {
  const std::string path = TempPath("mmap_snapshot_prewarm.kvi");
  const auto spec = SpecWithOneRow();
  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());
  auto backing = MmapSnapshotBacking::LoadShard(path, 0, OptionsFor(spec));
  KV_INDEX_CHECK(backing.ok());

  KV_INDEX_CHECK(!(*backing)->prewarmed());
  KV_INDEX_CHECK((*backing)->Prewarm().ok());
  KV_INDEX_CHECK((*backing)->prewarmed());
  KV_INDEX_CHECK_EQ((*backing)->prewarm_touched_pages_for_testing(), 7u);
}

int Main() {
  LocalFileMappingServesRowsThroughFullSnapshotView();
  FileUriMappingAndViewOwnMappedLifetime();
  InvalidPathAndRemoteSchemeFailClosed();
  ExplicitPrewarmTouchesShardSectionsAndMarksReady();
  return 0;
}

}  // namespace

int main() { return Main(); }
