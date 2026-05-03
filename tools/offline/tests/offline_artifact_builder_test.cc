#include "src/offline_artifact_builder.h"

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>

#include <chrono>
#include <cerrno>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "kv_index/forward_index.h"

namespace {

#define CHECK(condition)                                                     \
  do {                                                                       \
    if (!(condition)) {                                                      \
      std::cerr << __FILE__ << ":" << __LINE__ << ": check failed: "        \
                << #condition << std::endl;                                  \
      std::abort();                                                          \
    }                                                                        \
  } while (false)

#define CHECK_EQ(actual, expected)                                           \
  do {                                                                       \
    const auto actual_value = (actual);                                      \
    const auto expected_value = (expected);                                  \
    if (!(actual_value == expected_value)) {                                 \
      std::cerr << __FILE__ << ":" << __LINE__ << ": check failed: "        \
                << #actual << " == " << #expected << std::endl;             \
      std::abort();                                                          \
    }                                                                        \
  } while (false)

std::string JoinPath(const std::string& lhs, const std::string& rhs) {
  if (lhs.empty() || lhs == "/") {
    return lhs + rhs;
  }
  return lhs + "/" + rhs;
}

std::string TempPath(const std::string& name) {
  const char* tmpdir = std::getenv("TEST_TMPDIR");
  if (tmpdir == nullptr) {
    tmpdir = "/tmp";
  }
  return JoinPath(tmpdir, "kv_index_offline_" + name + "_" +
                              std::to_string(static_cast<long long>(getpid())));
}

bool Exists(const std::string& path) {
  struct stat statbuf {};
  return stat(path.c_str(), &statbuf) == 0;
}

void MakeDir(const std::string& path) {
  if (mkdir(path.c_str(), 0755) != 0 && errno != EEXIST) {
    std::cerr << "failed to create directory: " << path << std::endl;
    std::abort();
  }
}

void WriteTextFile(const std::string& path, const std::string& text) {
  std::ofstream out(path);
  CHECK(out);
  out << text;
  CHECK(out.good());
}

std::string ReadTextFile(const std::string& path) {
  std::ifstream in(path);
  CHECK(in);
  return std::string(std::istreambuf_iterator<char>(in),
                     std::istreambuf_iterator<char>());
}

void CheckArrowStatus(const arrow::Status& status) {
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    std::abort();
  }
}

template <typename T>
std::shared_ptr<T> ValueOrAbort(arrow::Result<std::shared_ptr<T>> result) {
  if (!result.ok()) {
    std::cerr << result.status().ToString() << std::endl;
    std::abort();
  }
  return std::move(result).ValueOrDie();
}

void WriteParquetFile(const std::string& path,
                      const std::vector<std::uint64_t>& record_ids,
                      const std::vector<std::int32_t>& scores,
                      const std::vector<std::string>& titles,
                      const std::vector<std::vector<std::string>>& tags) {
  CHECK_EQ(record_ids.size(), scores.size());
  CHECK_EQ(record_ids.size(), titles.size());
  CHECK_EQ(record_ids.size(), tags.size());

  arrow::UInt64Builder id_builder;
  arrow::Int32Builder score_builder;
  arrow::StringBuilder title_builder;
  arrow::StringBuilder tag_value_builder;
  arrow::ListBuilder tags_builder(arrow::default_memory_pool(),
                                  std::make_shared<arrow::StringBuilder>());
  auto* tags_value_builder =
      static_cast<arrow::StringBuilder*>(tags_builder.value_builder());

  for (std::size_t i = 0; i < record_ids.size(); ++i) {
    CheckArrowStatus(id_builder.Append(record_ids[i]));
    CheckArrowStatus(score_builder.Append(scores[i]));
    CheckArrowStatus(title_builder.Append(titles[i]));
    CheckArrowStatus(tags_builder.Append());
    for (const std::string& tag : tags[i]) {
      CheckArrowStatus(tags_value_builder->Append(tag));
    }
  }

  std::shared_ptr<arrow::Array> ids;
  std::shared_ptr<arrow::Array> score_values;
  std::shared_ptr<arrow::Array> title_values;
  std::shared_ptr<arrow::Array> tag_values;
  CheckArrowStatus(id_builder.Finish(&ids));
  CheckArrowStatus(score_builder.Finish(&score_values));
  CheckArrowStatus(title_builder.Finish(&title_values));
  CheckArrowStatus(tags_builder.Finish(&tag_values));

  auto schema = arrow::schema({
      arrow::field("record_id", arrow::uint64(), false),
      arrow::field("score", arrow::int32(), false),
      arrow::field("title", arrow::utf8(), false),
      arrow::field("tags", arrow::list(arrow::utf8()), false),
  });
  auto table = arrow::Table::Make(schema, {ids, score_values, title_values,
                                           tag_values});
  auto output = ValueOrAbort(arrow::io::FileOutputStream::Open(path));
  CheckArrowStatus(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(),
                                              output, 2));
}

void WaitForTerminalState(const kv_index::ForwardIndex& index,
                          kv_index::LoadId load_id) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (std::chrono::steady_clock::now() < deadline) {
    if (index.GetLoadState(load_id).terminal) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  CHECK(index.GetLoadState(load_id).terminal);
}

void BuildsShardDirectoryFromParquetDirectoryAndUpdatesSchemaVersion() {
  const std::string root = TempPath("end_to_end");
  const std::string input_dir = JoinPath(root, "input");
  const std::string nested_input_dir = JoinPath(input_dir, "dt=2026-05-03");
  const std::string output_dir = JoinPath(root, "output");
  MakeDir(root);
  MakeDir(input_dir);
  MakeDir(nested_input_dir);

  const std::string schema_path = JoinPath(root, "schema.yaml");
  WriteTextFile(schema_path, R"yaml(
schema_version: 11
shard_count: 4
hash_seed: 17
hash_version: 1

primary_key:
  name: record_id
  type: uint64

fields:
  - field_id: 1
    name: score
    type: int32
    is_list: false
    nullable: false
    encoding: fixed

  - field_id: 2
    name: title
    type: string
    is_list: false
    nullable: false
    encoding: arena

  - field_id: 3
    name: tags
    type: string
    is_list: true
    nullable: false
    encoding: element_dictionary
)yaml");

  WriteParquetFile(JoinPath(input_dir, "part-00000.parquet"), {1001, 2002}, {10, 20},
                   {"alpha", "beta"}, {{"blue", "hot"}, {"green"}});
  WriteParquetFile(JoinPath(nested_input_dir, "part-00001.parquet"),
                   {3003}, {30}, {"gamma"}, {{"red", "cold"}});

  kv_index::offline::BuildArtifactOptions build_options;
  build_options.schema_path = schema_path;
  build_options.input_path = input_dir;
  build_options.output_path = output_dir;
  build_options.artifact_id = "offline-build-test";
  build_options.omit_source_progress = true;

  auto result = kv_index::offline::BuildArtifactDirectory(build_options);
  CHECK(result.ok());
  CHECK_EQ(result->row_count, 3U);
  CHECK_EQ(result->schema_version, 12U);
  CHECK_EQ(result->shard_count, 4U);
  CHECK(Exists(JoinPath(output_dir, "schema.yaml")));
  CHECK(Exists(JoinPath(output_dir, "shard_00000.kvi")));
  CHECK(Exists(JoinPath(output_dir, "shard_00001.kvi")));
  CHECK(Exists(JoinPath(output_dir, "shard_00002.kvi")));
  CHECK(Exists(JoinPath(output_dir, "shard_00003.kvi")));

  const std::string output_schema = ReadTextFile(JoinPath(output_dir, "schema.yaml"));
  CHECK(output_schema.find("schema_version: 12") != std::string::npos);
  CHECK(output_schema.find("shard_count: 4") != std::string::npos);
  CHECK(output_schema.find("hash_seed: 17") != std::string::npos);

  kv_index::ForwardIndexOptions index_options;
  index_options.mode = kv_index::ForwardIndexMode::kFullSnapshotOnly;
  index_options.shard_count = 4;
  index_options.hash_seed = 17;
  index_options.hash_version = 1;
  kv_index::ForwardIndex index(index_options);
  const kv_index::LoadId load_id = index.LoadAsync(kv_index::LoadRequest{
      .artifact_uri = output_dir,
      .artifact_id = "offline-build-test",
  });
  WaitForTerminalState(index, load_id);
  const kv_index::LoadState state = index.GetLoadState(load_id);
  CHECK_EQ(state.code, kv_index::LoadStateCode::kSucceeded);
  CHECK_EQ(state.cutover_shard_count, 4U);

  auto row = index.Get(1001);
  CHECK(row.has_value());
  CHECK_EQ(row->Get<std::int32_t>(1).value(), 10);
  CHECK_EQ(row->Get<std::string>(2).value(), std::string("alpha"));
  const auto tags = row->GetList<std::string>(3);
  CHECK_EQ(tags.size(), 2U);
  CHECK_EQ(tags[0], std::string("blue"));
  CHECK_EQ(tags[1], std::string("hot"));
}

}  // namespace

int main() {
  BuildsShardDirectoryFromParquetDirectoryAndUpdatesSchemaVersion();
  return 0;
}
