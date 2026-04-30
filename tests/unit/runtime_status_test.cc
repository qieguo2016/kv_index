#include "kv_index/forward_index.h"
#include "kv_index/schema.h"
#include "src/core/async_load.h"
#include "src/core/artifact_format.h"
#include "src/core/byte_io.h"
#include "src/core/realtime_delta.h"
#include "src/core/row_storage.h"
#include "src/core/shard_state.h"
#include "src/core/snapshot.h"
#include "src/core/snapshot_builder.h"
#include "src/core/test_peer.h"
#include "tests/test_support/artifact_writer.h"
#include "tests/test_support/test_macros.h"

#include <cstdint>
#include <cstdlib>
#include <chrono>
#include <cstddef>
#include <fstream>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

using kv_index::CompiledRowLayout;
using kv_index::FieldEncoding;
using kv_index::FieldLayout;
using kv_index::FieldSpec;
using kv_index::FieldType;
using kv_index::ForwardIndex;
using kv_index::ForwardIndexOptions;
using kv_index::LoadRequest;
using kv_index::LoadStateCode;
using kv_index::RuntimeSchema;
using kv_index::SourcePosition;
using kv_index::Status;
using kv_index::StatusOr;
using kv_index::core::AsyncCatchUpRequest;
using kv_index::core::AsyncCatchUpRunner;
using kv_index::core::AsyncLoadCallbacks;
using kv_index::core::ArtifactSectionType;
using kv_index::core::CompactDeltaSnapshot;
using kv_index::core::ForwardIndexTestPeer;
using kv_index::core::FullSnapshotView;
using kv_index::core::Fnv1a64;
using kv_index::core::OwnedSnapshotBacking;
using kv_index::core::RealtimeDeltaAtomicTable;
using kv_index::core::RunExternalArtifactLoad;
using kv_index::core::SetAsyncCatchUpRunnerFactoryForTesting;
using kv_index::core::ShardState;
using kv_index::core::SnapshotBuilder;
using kv_index::test_support::ArtifactShardSpec;
using kv_index::test_support::TestArtifactSpec;
using kv_index::test_support::TestSourceProgress;
using kv_index::test_support::WriteTestArtifact;
namespace storage = kv_index::internal;

constexpr std::size_t kArtifactSectionCountOffset = 20;
constexpr std::size_t kArtifactEntryTypeOffset = 0;
constexpr std::size_t kArtifactEntryShardIdOffset = 4;
constexpr std::size_t kArtifactEntryFileOffsetOffset = 8;
constexpr std::size_t kArtifactEntryLengthOffset = 16;
constexpr std::size_t kArtifactEntryChecksumOffset = 24;

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

std::shared_ptr<const CompiledRowLayout> MakeLayout(
    std::uint64_t schema_version = 880) {
  RuntimeSchema schema(schema_version);
  KV_INDEX_CHECK(schema.AddField(Scalar(1, "score", FieldType::kInt32)).ok());
  KV_INDEX_CHECK(schema.AddField(Scalar(2, "title", FieldType::kString)).ok());
  auto layout = CompiledRowLayout::Compile(schema);
  KV_INDEX_CHECK(layout.ok());
  return std::make_shared<const CompiledRowLayout>(std::move(layout).value());
}

const FieldLayout& Field(const CompiledRowLayout& layout,
                         kv_index::FieldId field_id) {
  const FieldLayout* field = layout.FindField(field_id);
  KV_INDEX_CHECK(field != nullptr);
  return *field;
}

storage::EncodedRow MakeRow(const CompiledRowLayout& layout,
                            std::int32_t score,
                            std::optional<std::string> title) {
  auto encoded = storage::CreateEncodedRow(layout);
  KV_INDEX_CHECK(
      storage::WriteScalarField(Field(layout, 1), score, &encoded).ok());
  if (title.has_value()) {
    KV_INDEX_CHECK(
        storage::WriteArenaStringField(Field(layout, 2), *title, &encoded)
            .ok());
  }
  return encoded;
}

std::shared_ptr<const OwnedSnapshotBacking> BuildSnapshot(
    std::shared_ptr<const CompiledRowLayout> layout,
    const std::vector<std::pair<std::uint64_t, storage::EncodedRow>>& rows) {
  SnapshotBuilder builder(layout);
  for (const auto& [primary_key, encoded] : rows) {
    KV_INDEX_CHECK(builder.AddRow(primary_key, encoded).ok());
  }
  auto backing = builder.Seal();
  KV_INDEX_CHECK(backing.ok());
  return backing.value();
}

std::shared_ptr<RealtimeDeltaAtomicTable> BuildRealtime(
    std::shared_ptr<const CompiledRowLayout> layout) {
  auto realtime = std::make_shared<RealtimeDeltaAtomicTable>(
      RealtimeDeltaAtomicTable::Options{.layout = layout, .capacity = 16});
  KV_INDEX_CHECK(realtime
                     ->Publish(100, SourcePosition{.partition = 0, .offset = 1},
                               MakeRow(*layout, 30, std::string("realtime")))
                     .ok());
  return realtime;
}

std::string TempPath(const std::string& name) {
  const char* tmpdir = std::getenv("TEST_TMPDIR");
  if (tmpdir == nullptr) {
    tmpdir = "/tmp";
  }
  return std::string(tmpdir) + "/kv_index_w08_" + name;
}

std::uint64_t FindKeyForShard(const ForwardIndex& index,
                              std::uint32_t shard_id) {
  for (std::uint64_t key = 1; key < 100000; ++key) {
    if (index.ShardFor(key) == shard_id) {
      return key;
    }
  }
  std::abort();
}

TestArtifactSpec ArtifactSpec(const ForwardIndexOptions& options,
                              std::shared_ptr<const CompiledRowLayout> layout,
                              std::uint64_t key, std::int32_t score) {
  TestArtifactSpec spec;
  spec.artifact_id = "w08-artifact";
  spec.shard_count = options.shard_count;
  spec.hash_seed = options.hash_seed;
  spec.hash_version = options.hash_version;
  spec.layout = std::move(layout);
  spec.source_progress.push_back(TestSourceProgress{
      .topic = "updates",
      .partition = 0,
      .checkpoint_next_offset = 7,
      .high_watermark = 7,
  });
  for (std::uint32_t shard = 0; shard < options.shard_count; ++shard) {
    ArtifactShardSpec shard_spec{.shard_id = shard};
    if (shard == options.shard_count - 1) {
      shard_spec.rows.push_back(
          {.primary_key = key, .encoded = MakeRow(*spec.layout, score,
                                                  std::string("artifact"))});
    }
    spec.shards.push_back(std::move(shard_spec));
  }
  return spec;
}

class NoopCatchUpRunner final : public AsyncCatchUpRunner {
 public:
  Status Start(const AsyncCatchUpRequest& request) override {
    progress_ = request.safe_progress;
    return Status::Ok();
  }

  StatusOr<kv_index::KafkaProgress> CaptureSafeProgress() override {
    return progress_;
  }

  Status PollApplyCommitOnce() override { return Status::Ok(); }

 private:
  kv_index::KafkaProgress progress_;
};

class ScopedNoopCatchUpFactory {
 public:
  ScopedNoopCatchUpFactory()
      : previous_(SetAsyncCatchUpRunnerFactoryForTesting(
            [](const ForwardIndexOptions&)
                -> StatusOr<std::unique_ptr<AsyncCatchUpRunner>> {
              return std::make_unique<NoopCatchUpRunner>();
            })) {}

  ~ScopedNoopCatchUpFactory() {
    (void)SetAsyncCatchUpRunnerFactoryForTesting(std::move(previous_));
  }

 private:
  kv_index::core::AsyncCatchUpRunnerFactory previous_;
};

void WaitForTerminalState(const ForwardIndex& index, kv_index::LoadId load_id) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (std::chrono::steady_clock::now() < deadline) {
    if (index.GetLoadState(load_id).terminal) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  KV_INDEX_CHECK(index.GetLoadState(load_id).terminal);
}

std::shared_ptr<const ShardState> FullState(
    std::uint32_t shard_id, std::uint64_t generation,
    std::shared_ptr<const CompiledRowLayout> layout, std::uint64_t key,
    std::int32_t score) {
  return std::make_shared<const ShardState>(
      shard_id, generation,
      ShardState::Layers{
          .full_snapshot = FullSnapshotView(BuildSnapshot(
              layout, {{key, MakeRow(*layout, score, std::string("old"))}})),
      });
}

void CorruptByteAtEnd(const std::string& path) {
  std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
  KV_INDEX_CHECK(file.good());
  file.seekg(-1, std::ios::end);
  char byte = 0;
  file.read(&byte, 1);
  file.seekp(-1, std::ios::end);
  byte = static_cast<char>(byte ^ 0x01);
  file.write(&byte, 1);
}

StatusOr<std::vector<std::byte>> ReadArtifactFile(const std::string& path) {
  std::ifstream input(path, std::ios::binary | std::ios::ate);
  if (!input) {
    return Status::Unavailable("failed to open artifact for reading");
  }
  const std::streamsize size = input.tellg();
  if (size < 0) {
    return Status::Unavailable("failed to determine artifact size");
  }
  std::vector<std::byte> bytes(static_cast<std::size_t>(size));
  input.seekg(0, std::ios::beg);
  input.read(reinterpret_cast<char*>(bytes.data()), size);
  if (!input && size > 0) {
    return Status::Unavailable("failed to read artifact bytes");
  }
  return bytes;
}

Status WriteArtifactFile(const std::string& path,
                         const std::vector<std::byte>& bytes) {
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  if (!output) {
    return Status::Unavailable("failed to open artifact for writing");
  }
  output.write(reinterpret_cast<const char*>(bytes.data()),
               static_cast<std::streamsize>(bytes.size()));
  if (!output) {
    return Status::Unavailable("failed to write artifact bytes");
  }
  return Status::Ok();
}

Status MakeShardRowSlotsMisaligned(const std::string& path,
                                   std::uint32_t shard_id) {
  auto bytes_or = ReadArtifactFile(path);
  if (!bytes_or.ok()) {
    return bytes_or.status();
  }
  std::vector<std::byte> bytes = std::move(bytes_or).value();
  auto section_count = storage::ReadLittleEndian<std::uint32_t>(
      bytes, kArtifactSectionCountOffset);
  if (!section_count.ok()) {
    return section_count.status();
  }

  std::optional<std::size_t> target_entry;
  std::uint64_t section_offset = 0;
  std::uint64_t section_length = 0;
  for (std::uint32_t i = 0; i < *section_count; ++i) {
    const std::size_t entry_offset =
        kv_index::core::kArtifactHeaderSize +
        static_cast<std::size_t>(i) * kv_index::core::kArtifactSectionEntrySize;
    auto type = storage::ReadLittleEndian<std::uint32_t>(
        bytes, entry_offset + kArtifactEntryTypeOffset);
    auto entry_shard = storage::ReadLittleEndian<std::uint32_t>(
        bytes, entry_offset + kArtifactEntryShardIdOffset);
    if (!type.ok() || !entry_shard.ok()) {
      return Status::InvalidArgument("artifact section entry is truncated");
    }
    if (*type == static_cast<std::uint32_t>(ArtifactSectionType::kRowSlots) &&
        *entry_shard == shard_id) {
      auto offset = storage::ReadLittleEndian<std::uint64_t>(
          bytes, entry_offset + kArtifactEntryFileOffsetOffset);
      auto length = storage::ReadLittleEndian<std::uint64_t>(
          bytes, entry_offset + kArtifactEntryLengthOffset);
      if (!offset.ok() || !length.ok()) {
        return Status::InvalidArgument("artifact row-slot entry is truncated");
      }
      target_entry = entry_offset;
      section_offset = *offset;
      section_length = *length;
      break;
    }
  }
  if (!target_entry.has_value()) {
    return Status::InvalidArgument("artifact row-slot section not found");
  }
  if (section_offset > bytes.size() ||
      bytes.size() - section_offset < section_length) {
    return Status::InvalidArgument("artifact row-slot section is out of bounds");
  }

  const std::uint64_t insert_pos = section_offset + section_length;
  bytes.insert(bytes.begin() + static_cast<std::ptrdiff_t>(insert_pos),
               std::byte{0x7f});

  const std::uint64_t new_length = section_length + 1;
  if (const Status status = storage::WriteLittleEndian<std::uint64_t>(
          new_length, std::span<std::byte>(bytes),
          *target_entry + kArtifactEntryLengthOffset);
      !status.ok()) {
    return status;
  }
  const std::uint64_t new_checksum = Fnv1a64(std::span<const std::byte>(
      bytes.data() + section_offset, static_cast<std::size_t>(new_length)));
  if (const Status status = storage::WriteLittleEndian<std::uint64_t>(
          new_checksum, std::span<std::byte>(bytes),
          *target_entry + kArtifactEntryChecksumOffset);
      !status.ok()) {
    return status;
  }

  for (std::uint32_t i = 0; i < *section_count; ++i) {
    const std::size_t entry_offset =
        kv_index::core::kArtifactHeaderSize +
        static_cast<std::size_t>(i) * kv_index::core::kArtifactSectionEntrySize;
    if (entry_offset == *target_entry) {
      continue;
    }
    auto offset = storage::ReadLittleEndian<std::uint64_t>(
        bytes, entry_offset + kArtifactEntryFileOffsetOffset);
    if (!offset.ok()) {
      return offset.status();
    }
    if (*offset >= insert_pos) {
      if (const Status status = storage::WriteLittleEndian<std::uint64_t>(
              *offset + 1, std::span<std::byte>(bytes),
              entry_offset + kArtifactEntryFileOffsetOffset);
          !status.ok()) {
        return status;
      }
    }
  }

  return WriteArtifactFile(path, bytes);
}

void EmptyIndexStatusReportsEveryShardAsEmpty() {
  ForwardIndexOptions options;
  options.shard_count = 4;
  ForwardIndex index(options);

  const auto status = index.GetRuntimeStatus();

  KV_INDEX_CHECK_EQ(status.shard_count, 4U);
  KV_INDEX_CHECK_EQ(status.shards.size(), 4U);
  KV_INDEX_CHECK(status.loads.empty());
  for (std::uint32_t shard_id = 0; shard_id < options.shard_count;
       ++shard_id) {
    const auto& shard = status.shards[shard_id];
    KV_INDEX_CHECK_EQ(shard.shard_id, shard_id);
    KV_INDEX_CHECK_EQ(shard.generation, 0U);
    KV_INDEX_CHECK_EQ(shard.schema_version, 0U);
    KV_INDEX_CHECK_EQ(shard.layout_fingerprint, 0U);
    KV_INDEX_CHECK(!shard.has_realtime_delta);
    KV_INDEX_CHECK(!shard.has_compact_delta);
    KV_INDEX_CHECK(!shard.has_full_snapshot);
    KV_INDEX_CHECK_EQ(shard.realtime_delta.published_row_count, 0U);
    KV_INDEX_CHECK_EQ(shard.accessor_mismatch_count, 0U);
    KV_INDEX_CHECK(shard.last_error.empty());
  }
}

void PopulatedShardStatusReportsLayersAndRealtimeStats() {
  ForwardIndexOptions options;
  options.shard_count = 4;
  ForwardIndex index(options);
  auto layout = MakeLayout(881);
  auto realtime = BuildRealtime(layout);
  auto full = BuildSnapshot(
      layout, {{100, MakeRow(*layout, 10, std::string("full"))}});
  auto compact = BuildSnapshot(
      layout, {{100, MakeRow(*layout, 20, std::string("compact"))}});
  auto state = std::make_shared<const ShardState>(
      2, 42,
      ShardState::Layers{
          .realtime_delta = realtime,
          .compact_delta = CompactDeltaSnapshot(compact),
          .full_snapshot = FullSnapshotView(full),
      });
  KV_INDEX_CHECK(ForwardIndexTestPeer::PublishShard(index, state).ok());

  const auto status = index.GetRuntimeStatus();
  const auto& shard = status.shards[2];

  KV_INDEX_CHECK_EQ(shard.generation, 42U);
  KV_INDEX_CHECK_EQ(shard.schema_version, 881U);
  KV_INDEX_CHECK_EQ(shard.layout_fingerprint, layout->layout_fingerprint());
  KV_INDEX_CHECK(shard.has_realtime_delta);
  KV_INDEX_CHECK(shard.has_compact_delta);
  KV_INDEX_CHECK(shard.has_full_snapshot);
  KV_INDEX_CHECK_EQ(shard.realtime_delta.hash_capacity, 16U);
  KV_INDEX_CHECK_EQ(shard.realtime_delta.unique_visible_keys, 1U);
  KV_INDEX_CHECK_EQ(shard.realtime_delta.published_row_count, 1U);
  KV_INDEX_CHECK_EQ(shard.compact_row_count, 1U);
  KV_INDEX_CHECK_EQ(shard.full_row_count, 1U);
}

void RuntimeStatusIncludesLoadFailureState() {
  ForwardIndex index(ForwardIndexOptions{});
  const kv_index::LoadId load_id = index.LoadAsync(LoadRequest{
      .artifact_uri = "file:///tmp/kv_index/definitely-missing-w08-artifact",
      .artifact_id = "missing-artifact",
  });

  const auto status = index.GetRuntimeStatus();

  KV_INDEX_CHECK_EQ(status.loads.size(), 1U);
  KV_INDEX_CHECK_EQ(status.loads[0].id, load_id);
  KV_INDEX_CHECK_EQ(status.loads[0].code, LoadStateCode::kFailed);
  KV_INDEX_CHECK(status.loads[0].terminal);
  KV_INDEX_CHECK_EQ(status.loads[0].artifact_id, "missing-artifact");
  KV_INDEX_CHECK(!status.loads[0].last_error.empty());
  KV_INDEX_CHECK_EQ(status.last_error, status.loads[0].last_error);
}

void ChecksumFailureLeavesServingShardUnchangedAndStatusFailed() {
  ForwardIndexOptions options;
  options.shard_count = 2;
  options.hash_seed = 17;
  ForwardIndex index(options);
  auto layout = MakeLayout(882);
  const std::uint64_t key = FindKeyForShard(index, 1);
  KV_INDEX_CHECK(ForwardIndexTestPeer::PublishShard(
                     index, FullState(1, 1, layout, key, 11))
                     .ok());
  const std::string path = TempPath("checksum_failure.kvi");
  auto spec = ArtifactSpec(options, layout, key, 99);
  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());
  CorruptByteAtEnd(path);

  const kv_index::LoadId load_id = index.LoadAsync(LoadRequest{
      .artifact_uri = path,
      .artifact_id = spec.artifact_id,
  });
  WaitForTerminalState(index, load_id);
  const auto state = index.GetLoadState(load_id);

  KV_INDEX_CHECK_EQ(state.code, LoadStateCode::kFailed);
  KV_INDEX_CHECK_EQ(state.cutover_shard_count, 0U);
  auto row = index.Get(key);
  KV_INDEX_CHECK(row.has_value());
  KV_INDEX_CHECK_EQ(row->Get<std::int32_t>(1).value(), 11);
  const auto status = index.GetRuntimeStatus();
  KV_INDEX_CHECK_EQ(status.loads.back().last_error, state.last_error);
}

void LayoutValidationFailureLeavesServingShardUnchanged() {
  ForwardIndexOptions options;
  options.shard_count = 2;
  options.hash_seed = 17;
  ForwardIndex index(options);
  auto layout = MakeLayout(883);
  const std::uint64_t key = FindKeyForShard(index, 1);
  KV_INDEX_CHECK(ForwardIndexTestPeer::PublishShard(
                     index, FullState(1, 1, layout, key, 12))
                     .ok());
  const auto before = ForwardIndexTestPeer::LoadShard(index, 1);
  KV_INDEX_CHECK(before.ok());
  KV_INDEX_CHECK((*before) != nullptr);
  KV_INDEX_CHECK_EQ((*before)->Generation(), 1U);
  const std::string path = TempPath("layout_failure.kvi");
  auto spec = ArtifactSpec(options, layout, key, 99);
  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());
  KV_INDEX_CHECK(MakeShardRowSlotsMisaligned(path, 1).ok());
  ScopedNoopCatchUpFactory factory;

  const kv_index::LoadId load_id = index.LoadAsync(LoadRequest{
      .artifact_uri = path,
      .artifact_id = spec.artifact_id,
  });
  WaitForTerminalState(index, load_id);
  const auto state = index.GetLoadState(load_id);

  KV_INDEX_CHECK_EQ(state.code, LoadStateCode::kFailed);
  KV_INDEX_CHECK_EQ(state.cutover_shard_count, 0U);
  KV_INDEX_CHECK(state.last_error.find("row slot") != std::string::npos ||
                 state.last_error.find("layout") != std::string::npos ||
                 state.last_error.find("schema") != std::string::npos);
  auto row = index.Get(key);
  KV_INDEX_CHECK(row.has_value());
  KV_INDEX_CHECK_EQ(row->Get<std::int32_t>(1).value(), 12);
  KV_INDEX_CHECK_EQ(row->Get<std::string>(2).value(), std::string("old"));
  const auto status = index.GetRuntimeStatus();
  KV_INDEX_CHECK_EQ(status.shards[1].generation, 1U);
  const auto after = ForwardIndexTestPeer::LoadShard(index, 1);
  KV_INDEX_CHECK(after.ok());
  KV_INDEX_CHECK((*after) != nullptr);
  KV_INDEX_CHECK_EQ((*after)->Generation(), 1U);
}

void CutoverPublishFailureReportsExplicitPartialCutover() {
  ForwardIndexOptions options;
  options.shard_count = 2;
  options.hash_seed = 17;
  auto layout = MakeLayout(884);
  const std::string path = TempPath("cutover_publish_failure.kvi");
  ForwardIndex index(options);
  const std::uint64_t first_key = FindKeyForShard(index, 0);
  const std::uint64_t key = FindKeyForShard(index, 1);
  auto spec = ArtifactSpec(options, layout, key, 99);
  spec.shards[0].rows.push_back({.primary_key = first_key,
                                 .encoded = MakeRow(*layout, 88,
                                                    std::string("artifact"))});
  KV_INDEX_CHECK(WriteTestArtifact(path, spec).ok());
  ScopedNoopCatchUpFactory factory;

  std::vector<std::shared_ptr<const ShardState>> serving = {
      FullState(0, 1, layout, first_key, 10),
      FullState(1, 1, layout, key, 20),
  };
  kv_index::LoadState state;
  Status status = RunExternalArtifactLoad(
      LoadRequest{.artifact_uri = path, .artifact_id = spec.artifact_id}, 99,
      options,
      AsyncLoadCallbacks{
          .publish_shard =
              [&serving](std::uint32_t shard_id,
                         std::shared_ptr<const ShardState> shard) {
                if (shard_id == 1) {
                  return Status::Unavailable("injected publish failure");
                }
                serving[shard_id] = std::move(shard);
                return Status::Ok();
              },
          .is_cancelled = [] { return false; },
      },
      &state);

  KV_INDEX_CHECK(!status.ok());
  KV_INDEX_CHECK_EQ(state.code, LoadStateCode::kFailed);
  KV_INDEX_CHECK_EQ(state.cutover_shard_count, 1U);
  KV_INDEX_CHECK_EQ(serving[0]->Generation(), 99U);
  KV_INDEX_CHECK_EQ(serving[1]->Generation(), 1U);
  KV_INDEX_CHECK_EQ(state.last_error, "injected publish failure");
  auto cutover_row = serving[0]->Get(first_key);
  KV_INDEX_CHECK(cutover_row.ok());
  KV_INDEX_CHECK(cutover_row->has_value());
  KV_INDEX_CHECK_EQ(cutover_row->value().Get<std::int32_t>(1).value(), 88);
  KV_INDEX_CHECK_EQ(cutover_row->value().Get<std::string>(2).value(),
                    std::string("artifact"));
  auto failed_row = serving[1]->Get(key);
  KV_INDEX_CHECK(failed_row.ok());
  KV_INDEX_CHECK(failed_row->has_value());
  KV_INDEX_CHECK_EQ(failed_row->value().Get<std::int32_t>(1).value(), 20);
  KV_INDEX_CHECK_EQ(failed_row->value().Get<std::string>(2).value(),
                    std::string("old"));
}

}  // namespace

int main() {
  EmptyIndexStatusReportsEveryShardAsEmpty();
  PopulatedShardStatusReportsLayersAndRealtimeStats();
  RuntimeStatusIncludesLoadFailureState();
  ChecksumFailureLeavesServingShardUnchangedAndStatusFailed();
  LayoutValidationFailureLeavesServingShardUnchanged();
  CutoverPublishFailureReportsExplicitPartialCutover();
  return 0;
}
