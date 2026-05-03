#include <cstdint>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>

#include "src/artifact/artifact_writer.h"

namespace {

struct Options {
  std::string schema_path;
  std::string input_path;
  std::string output_path;
  std::string artifact_id;
  std::uint32_t shard_count = 128;
  std::uint64_t hash_seed = 0;
  std::uint32_t hash_version = 1;
  bool omit_source_progress = false;
  bool help = false;
};

void PrintUsage(std::ostream& out) {
  out << "usage: kv_index_build_artifact \\\n"
      << "  --schema schema.yaml \\\n"
      << "  --input data.parquet \\\n"
      << "  --output full.kvi \\\n"
      << "  --artifact_id build-id \\\n"
      << "  [--shard_count 128] [--hash_seed 0] [--hash_version 1] \\\n"
      << "  [--omit_source_progress]\n";
}

std::optional<std::string_view> ValueFor(int* index, int argc, char** argv) {
  if (*index + 1 >= argc) {
    return std::nullopt;
  }
  ++(*index);
  return std::string_view(argv[*index]);
}

bool ParseUInt32(std::string_view text, std::uint32_t* out) {
  std::uint64_t value = 0;
  if (text.empty()) {
    return false;
  }
  for (const char c : text) {
    if (c < '0' || c > '9') {
      return false;
    }
    value = value * 10 + static_cast<std::uint64_t>(c - '0');
    if (value > UINT32_MAX) {
      return false;
    }
  }
  *out = static_cast<std::uint32_t>(value);
  return true;
}

bool ParseUInt64(std::string_view text, std::uint64_t* out) {
  std::uint64_t value = 0;
  if (text.empty()) {
    return false;
  }
  for (const char c : text) {
    if (c < '0' || c > '9') {
      return false;
    }
    const std::uint64_t digit = static_cast<std::uint64_t>(c - '0');
    if (value > (UINT64_MAX - digit) / 10) {
      return false;
    }
    value = value * 10 + digit;
  }
  *out = value;
  return true;
}

bool ParseArgs(int argc, char** argv, Options* options, std::ostream& err) {
  for (int i = 1; i < argc; ++i) {
    const std::string_view arg(argv[i]);
    if (arg == "--help" || arg == "-h") {
      options->help = true;
      return true;
    }
    if (arg == "--schema") {
      auto value = ValueFor(&i, argc, argv);
      if (!value.has_value()) {
        err << "missing value for --schema\n";
        return false;
      }
      options->schema_path = std::string(*value);
    } else if (arg == "--input") {
      auto value = ValueFor(&i, argc, argv);
      if (!value.has_value()) {
        err << "missing value for --input\n";
        return false;
      }
      options->input_path = std::string(*value);
    } else if (arg == "--output") {
      auto value = ValueFor(&i, argc, argv);
      if (!value.has_value()) {
        err << "missing value for --output\n";
        return false;
      }
      options->output_path = std::string(*value);
    } else if (arg == "--artifact_id") {
      auto value = ValueFor(&i, argc, argv);
      if (!value.has_value()) {
        err << "missing value for --artifact_id\n";
        return false;
      }
      options->artifact_id = std::string(*value);
    } else if (arg == "--shard_count") {
      auto value = ValueFor(&i, argc, argv);
      if (!value.has_value() || !ParseUInt32(*value, &options->shard_count) ||
          options->shard_count == 0) {
        err << "invalid value for --shard_count\n";
        return false;
      }
    } else if (arg == "--hash_seed") {
      auto value = ValueFor(&i, argc, argv);
      if (!value.has_value() || !ParseUInt64(*value, &options->hash_seed)) {
        err << "invalid value for --hash_seed\n";
        return false;
      }
    } else if (arg == "--hash_version") {
      auto value = ValueFor(&i, argc, argv);
      if (!value.has_value() || !ParseUInt32(*value, &options->hash_version) ||
          options->hash_version == 0) {
        err << "invalid value for --hash_version\n";
        return false;
      }
    } else if (arg == "--omit_source_progress") {
      options->omit_source_progress = true;
    } else {
      err << "unknown argument: " << arg << "\n";
      return false;
    }
  }
  return true;
}

bool ValidateRequired(const Options& options, std::ostream& err) {
  if (options.schema_path.empty()) {
    err << "missing required --schema\n";
    return false;
  }
  if (options.input_path.empty()) {
    err << "missing required --input\n";
    return false;
  }
  if (options.output_path.empty()) {
    err << "missing required --output\n";
    return false;
  }
  if (options.artifact_id.empty()) {
    err << "missing required --artifact_id\n";
    return false;
  }
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  Options options;
  if (!ParseArgs(argc, argv, &options, std::cerr)) {
    PrintUsage(std::cerr);
    return 2;
  }
  if (options.help) {
    PrintUsage(std::cout);
    return 0;
  }
  if (!ValidateRequired(options, std::cerr)) {
    PrintUsage(std::cerr);
    return 2;
  }

  kv_index::artifact::ArtifactBuildSpec build_spec;
  build_spec.artifact_id = options.artifact_id;
  build_spec.shard_count = options.shard_count;
  build_spec.hash_seed = options.hash_seed;
  build_spec.hash_version = options.hash_version;
  build_spec.include_source_progress_section = !options.omit_source_progress;

  std::cerr << "schema and parquet adapters are not implemented yet; "
            << "artifact_id=" << build_spec.artifact_id
            << ", shard_count=" << build_spec.shard_count
            << ", hash_seed=" << build_spec.hash_seed
            << ", hash_version=" << build_spec.hash_version
            << ", include_source_progress="
            << (build_spec.include_source_progress_section ? "true" : "false")
            << ", "
            << "validated CLI options for schema=" << options.schema_path
            << ", input=" << options.input_path
            << ", output=" << options.output_path << "\n";
  return 3;
}
